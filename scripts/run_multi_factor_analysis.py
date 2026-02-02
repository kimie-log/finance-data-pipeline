"""
Alphalens 多因子分析腳本

參考 references/多因子/ 內加權排名與 PCA 兩種方式，從 BigQuery 或本地 Parquet／FinLab API
讀取價量與多個因子資料，進行多因子 Alphalens 分析。

模式：
  - weighted_rank：多因子加權排名後，對每個 N 因子組合做 Alphalens 分析（參考 main_alphalens_analysis_for_multiple_factors_with_weighted_rank.py）
  - pca：多因子合併後做主成分分析，對指定主成分（如 PC2、PC4）做 Alphalens 分析（參考 main_alphalens_analysis_for_multiple_factors_with_pca.py）

流程：
    1. 讀取價量資料 → 轉換為 Alphalens 所需價格格式
    2. 讀取多個因子資料（BigQuery／本地／FinLab API）
    3. weighted_rank：各因子排名 → 組合加權排名 → 每個組合跑 Alphalens
    4. pca：合併因子 → 標準化 → PCA → 對選定主成分跑 Alphalens
    5. 報告輸出至 data/multi_factor_analysis_reports/

執行範例：
    # 加權排名（五因子等權，因子列表從 config 或 --factors 指定）
    python -m scripts.run_multi_factor_analysis --mode weighted_rank --factors "營運現金流,歸屬母公司淨利,ROE稅後,營業利益成長率,稅後淨利成長率" --start 2017-05-16 --end 2021-05-15

    # PCA（對 PC2、PC4 做分析）
    python -m scripts.run_multi_factor_analysis --mode pca --factors "營業利益,營運現金流,ROE稅後,營業利益成長率,稅後淨利成長率" --pcs 2,4 --start 2017-05-16 --end 2021-05-15

依賴：
    .env（GCP_PROJECT_ID，若用 BigQuery）
    alphalens-reloaded
    scikit-learn（僅 PCA 模式）
"""

from __future__ import annotations

import os
import sys
import argparse
from pathlib import Path
from typing import Optional
from datetime import datetime
from itertools import combinations
from contextlib import redirect_stdout

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

try:
    from alphalens.tears import create_full_tear_sheet
    from alphalens.utils import get_clean_factor_and_forward_returns
except ImportError:
    print("錯誤：請先安裝 alphalens-reloaded")
    print("執行：pip install alphalens-reloaded")
    sys.exit(1)

from utils.data_loader import load_price_data, load_factor_data
from utils.google_cloud_storage import upload_file
from utils.logger import logger
from utils.cli import load_config, resolve_multi_factor_params
from factors.factor_ranking import FactorRanking
from factors.finlab_factor_fetcher import FinLabFactorFetcher
from ingestion.finlab_fetcher import FinLabFetcher


def prepare_prices_for_alphalens(df_price: pd.DataFrame) -> pd.DataFrame:
    """將價量資料轉為 Alphalens 所需格式：index=date, columns=stock_id, values=close。"""
    prices = df_price.pivot(index="date", columns="stock_id", values="close")
    prices.index = pd.to_datetime(prices.index)
    prices = prices.sort_index()
    logger.info(f"價格資料準備完成: {prices.shape[0]} 個交易日，{prices.shape[1]} 檔股票")
    return prices


def find_local_parquet_files(
    dataset_id: str,
    start_date: str,
    end_date: str,
    data_type: str = "price",
) -> Optional[Path]:
    """在 data/processed 下尋找符合條件的 parquet 檔案。"""
    processed_dir = ROOT_DIR / "data" / "processed"
    if not processed_dir.exists():
        return None
    pattern = "fact_price*.parquet" if data_type == "price" else "fact_factor*.parquet"
    parquet_files = list(processed_dir.rglob(pattern))
    if not parquet_files:
        return None
    latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"找到本地 {data_type} 檔案: {latest}")
    return latest


def _ensure_factor_datetime_asset_value(df: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """
    將因子 DataFrame 統一為 datetime, asset, value 欄位（FactorRanking 與後續加權排名用）。
    若來源為 date/stock_id，則轉成 datetime/asset/value。
    """
    if "datetime" in df.columns and "asset" in df.columns:
        if "value" not in df.columns and factor_name in df.columns:
            out = df[["datetime", "asset"]].copy()
            out["value"] = df[factor_name].values
            return out
        return df[["datetime", "asset", "value"]].copy()
    # date, stock_id 格式
    out = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
    out = out.rename(columns={"date": "datetime", "stock_id": "asset"})
    value_col = [c for c in out.columns if c not in ("datetime", "asset")]
    if value_col:
        out["value"] = out[value_col[0]]
    else:
        raise ValueError("因子資料缺少數值欄位")
    return out[["datetime", "asset", "value"]]


def _factor_series_for_alphalens(
    combined_or_pc: pd.DataFrame,
    index_names: tuple[str, str] = ("date", "stock_id"),
) -> pd.Series:
    """將組合排名或主成分 DataFrame 轉成 Alphalens 所需的 MultiIndex Series"""
    s = combined_or_pc.squeeze()
    if not isinstance(s, pd.Series):
        s = combined_or_pc.iloc[:, 0]
    s = s.dropna()
    s.index = s.index.rename(list(index_names))
    return s


def run_weighted_rank(
    df_price: pd.DataFrame,
    factors_data_dict: dict[str, pd.DataFrame],
    params: dict,
    prices_alphalens: pd.DataFrame,
    periods: list[int],
) -> list[Path]:
    """
    多因子加權排名：對每個 N 因子組合計算加權排名，再跑 Alphalens，回傳報告路徑列表
    """
    from itertools import combinations as comb

    factors = list(factors_data_dict.keys())
    combo_size = min(params["combo_size"], len(factors))
    if combo_size <= 0:
        logger.error("combo_size 須至少 1，且因子數量須足夠")
        return []

    # 轉成 datetime, asset, value 並做單因子排名
    rank_dfs = {}
    for name, df in factors_data_dict.items():
        long_df = _ensure_factor_datetime_asset_value(df, name)
        ranked = FactorRanking.rank_stocks_by_factor(
            long_df,
            positive_corr=params["positive_corr"],
            rank_column="value",
            rank_result_column="rank",
        )
        rank_dfs[name] = ranked

    # 等權重或指定權重
    n_in_combo = combo_size
    weights = params.get("weights")
    if weights is None or len(weights) != n_in_combo:
        weights = [1.0 / n_in_combo] * n_in_combo

    combos = list(comb(factors, n_in_combo))
    logger.info(f"加權排名模式：共 {len(combos)} 個 {n_in_combo} 因子組合")

    report_paths = []
    for pair in combos:
        combined = FactorRanking.calculate_weighted_rank(
            ranked_dfs=[rank_dfs[f] for f in pair],
            weights=weights,
            positive_corr=params["positive_corr"],
            rank_column="rank",
        )
        combined = combined.set_index(["datetime", "asset"])
        combined = combined.rename(columns={"weighted_rank": "factor_value"})
        # 對齊 Alphalens：index 命名為 date, stock_id
        combined.index = combined.index.rename(["date", "stock_id"])
        factor_series = _factor_series_for_alphalens(combined[["factor_value"]])

        alphalens_data = get_clean_factor_and_forward_returns(
            factor=factor_series,
            prices=prices_alphalens,
            quantiles=params["quantiles"],
            periods=periods,
        )
        label = "_".join(pair)
        report_dir = _save_tear_sheet(
            alphalens_data,
            label=label,
            params=params,
        )
        if report_dir:
            report_paths.append(report_dir)
    return report_paths


def run_pca(
    df_price: pd.DataFrame,
    factors_data_dict: dict[str, pd.DataFrame],
    params: dict,
    prices_alphalens: pd.DataFrame,
    periods: list[int],
) -> list[Path]:
    """
    多因子 PCA：合併因子 → 標準化 → PCA → 對指定主成分跑 Alphalens，回傳報告路徑列表
    """
    try:
        from sklearn.decomposition import PCA
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        logger.error("PCA 模式需要 scikit-learn，請執行：pip install scikit-learn")
        return []

    # 合併為 (date, stock_id) x factor_name
    concat_list = []
    for name, df in factors_data_dict.items():
        d = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
        d = d.rename(columns={"datetime": "date", "asset": "stock_id"})
        date_col = "date" if "date" in d.columns else None
        sid_col = "stock_id" if "stock_id" in d.columns else None
        if not date_col or not sid_col:
            continue
        val_cols = [c for c in d.columns if c not in (date_col, sid_col)]
        if not val_cols:
            continue
        out = d[[date_col, sid_col]].copy()
        out.columns = ["date", "stock_id"]
        out["value"] = d[val_cols[0]].values
        out["factor_name"] = name
        concat_list.append(out)

    concat_factors = pd.concat(concat_list, ignore_index=True)
    pivot_factors = concat_factors.pivot_table(
        index=["date", "stock_id"], columns="factor_name", values="value"
    )
    pivot_factors.replace([np.inf, -np.inf], np.nan, inplace=True)
    pivot_factors = pivot_factors.dropna()

    n_components = params.get("n_components")
    if n_components is None:
        n_components = len(factors_data_dict) - 1
    n_components = max(1, min(n_components, pivot_factors.shape[1]))

    scaler = StandardScaler()
    scaled = scaler.fit_transform(pivot_factors.values)
    pca = PCA(n_components=n_components)
    principal_components = pca.fit_transform(scaled)

    principal_df = pd.DataFrame(
        data=principal_components,
        index=pivot_factors.index,
        columns=[f"PC{i}" for i in range(1, n_components + 1)],
    )
    principal_df.index = principal_df.index.rename(["date", "stock_id"])

    # 與 weighted_rank 相同：路徑含因子名稱，label 為 {因子1}_{因子2}_..._{PCn}
    factor_names_joined = "_".join(factors_data_dict.keys())
    pcs_to_run = params.get("pcs") or [2, 4]
    report_paths = []
    for pc_num in pcs_to_run:
        if pc_num < 1 or pc_num > n_components:
            logger.warning(f"跳過 PC{pc_num}（有效範圍 1..{n_components}）")
            continue
        col = f"PC{pc_num}"
        factor_series = _factor_series_for_alphalens(principal_df[[col]])
        alphalens_data = get_clean_factor_and_forward_returns(
            factor=factor_series,
            prices=prices_alphalens,
            quantiles=params["quantiles"],
            periods=periods,
        )
        label = f"{factor_names_joined}_{col}"
        report_dir = _save_tear_sheet(alphalens_data, label=label, params=params)
        if report_dir:
            report_paths.append(report_dir)
    return report_paths


def _save_tear_sheet(
    alphalens_data: pd.DataFrame,
    label: str,
    params: dict,
) -> Optional[Path]:
    """
    產生 Alphalens tear sheet 並存到：
        data/multi_factor_analysis_reports/
          s{開始}_e{結束}_mv{市值日}/
            multi_{mode}_{timestamp}/{label}/

    並輸出 PDF、PNG 及文字 summary（stdout 內容）。
    """
    # 本次執行共用的時間戳（由 main 設定），若未提供則當場產生
    run_ts = params.get("run_timestamp")
    if not run_ts:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        params["run_timestamp"] = run_ts

    safe_label = label.replace("/", "_").replace("\\", "_").replace(",", "_")
    start_s = (params["start"] or "").replace("-", "")
    end_s = (params["end"] or "").replace("-", "")
    mv = params.get("market_value_date") or params.get("start") or ""
    mv_s = mv.replace("-", "") if isinstance(mv, str) else ""
    range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"
    # 外層依模式與執行時間戳建立資料夾，內層再依因子組合或主成分名稱區分
    outer_folder = f"multi_{params['mode']}_{run_ts}"
    outer_dir = ROOT_DIR / "data" / "multi_factor_analysis_reports" / range_dir / outer_folder
    report_dir = outer_dir / safe_label
    report_dir.mkdir(parents=True, exist_ok=True)
    report_base = f"multi_{params['mode']}_{run_ts}"
    report_path = report_dir / report_base

    # 在 plt.show 被 Alphalens 呼叫時「當場」儲存 figure，避免 show/close 後變成空白
    from matplotlib.backends.backend_pdf import PdfPages

    pdf_path = report_path.with_suffix(".pdf")
    pdf_file = PdfPages(pdf_path)
    saved_count = [0]

    def _save_on_show(*args, **kwargs):
        fig = plt.gcf()
        if fig.axes:
            try:
                fig.canvas.draw()
                pdf_file.savefig(fig, bbox_inches="tight", facecolor="white")
                saved_count[0] += 1
                png_path = report_path.parent / f"{report_base}_page_{saved_count[0]:02d}.png"
                fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor="white")
                logger.info(f"  已保存圖表 {saved_count[0]}: {png_path.name}")
            except Exception as e:
                logger.warning(f"保存圖表時失敗: {e}")

    _original_show = plt.show
    plt.show = _save_on_show

    # 將 Alphalens 在終端機輸出的各項統計（Quantiles Statistics、Returns Analysis、IC 等）
    # 一併寫入 summary 檔案
    summary_path = report_dir / f"{report_base}_summary.txt"
    try:
        with open(summary_path, "w", encoding="utf-8") as summary_file, redirect_stdout(
            summary_file
        ):
            create_full_tear_sheet(alphalens_data)
    finally:
        plt.show = _original_show
        pdf_file.close()

    if saved_count[0] > 0:
        logger.info(f"報表已保存: {report_dir}")
    else:
        logger.warning("未偵測到 Alphalens 產生的圖表")
        report_dir = None
    return report_dir


def main() -> int:
    config = load_config(ROOT_DIR)
    cfg = config.get("multi_factor_analysis", {})

    parser = argparse.ArgumentParser(
        description="Alphalens 多因子分析（加權排名 或 PCA）。預設使用 config/settings.yaml。"
    )
    parser.add_argument("--dataset", default=cfg.get("dataset"), help="BigQuery Dataset ID")
    parser.add_argument("--start", default=cfg.get("start"), help="分析區間起始 (YYYY-MM-DD)")
    parser.add_argument("--end", default=cfg.get("end"), help="分析區間結束 (YYYY-MM-DD)")
    parser.add_argument("--market-value-date", default=cfg.get("market_value_date"), help="市值基準日 (YYYY-MM-DD)，用於報告路徑 mv 資料夾名")
    parser.add_argument("--local-price", default=cfg.get("local_price"), help="本地價量 parquet 路徑")
    parser.add_argument("--quantiles", type=int, default=cfg.get("quantiles", 5), help="分位數數量")
    parser.add_argument("--periods", type=str, default=cfg.get("periods", "1,5,10"), help="前瞻期間，逗號分隔")
    parser.add_argument("--factor-table", default=cfg.get("factor_table", "fact_factor"), help="因子表名稱")
    parser.add_argument("--auto-find-local", action="store_true", help="自動在 data/processed 尋找本地檔案")
    parser.add_argument("--from-finlab-api", action="store_true", help="從 FinLab API 抓取因子")
    parser.add_argument(
        "--mode",
        choices=["weighted_rank", "pca"],
        default=cfg.get("mode", "weighted_rank"),
        help="分析模式：weighted_rank 或 pca",
    )
    parser.add_argument("--factors", type=str, default=None, help="因子名稱，逗號分隔（覆寫 config）")
    parser.add_argument("--combo-size", type=int, default=cfg.get("combo_size", 5), help="weighted_rank：每個組合的因子數")
    parser.add_argument("--weights", type=str, default=None, help="weighted_rank：權重逗號分隔，須與 combo_size 一致")
    parser.add_argument("--positive-corr", action="store_true", dest="positive_corr", help="因子與收益正相關")
    parser.add_argument("--no-positive-corr", action="store_false", dest="positive_corr", help="因子與收益負相關")
    parser.set_defaults(positive_corr=cfg.get("positive_corr", True))
    parser.add_argument("--pcs", type=str, default=cfg.get("pcs", "2,4"), help="pca：要分析的主成分編號，逗號分隔")
    parser.add_argument("--n-components", type=int, default=None, help="pca：主成分數量，預設為因子數-1")
    parser.add_argument("--skip-gcs", action="store_true", help="略過上傳報表至 GCS；未加則上傳")

    args = parser.parse_args()
    params = resolve_multi_factor_params(config, args)

    missing = [k for k in ("dataset", "start", "end") if not params.get(k)]
    if missing:
        logger.error("請提供以下參數（可從 config 或 CLI 指定）：%s。例：--dataset <dataset_id> --start 2017-05-16 --end 2021-05-15", ", ".join(missing))
        return 1
    if not params["factors"]:
        logger.error("請提供至少一個因子（config.multi_factor_analysis.factors 或 --factors）")
        return 1

    periods = [int(p.strip()) for p in str(params["periods"]).split(",")]

    logger.info("=== Alphalens 多因子分析開始 ===")
    logger.info(f"Dataset: {params['dataset']}")
    logger.info(f"模式: {params['mode']}")
    logger.info(f"因子: {params['factors']}")
    logger.info(f"日期: {params['start']} ~ {params['end']}")

    # 為本次執行產生統一的時間戳，供所有報表共用（資料夾與檔名一致）
    if not params.get("run_timestamp"):
        params["run_timestamp"] = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        local_price_path = params["local_price"]
        local_factor_path = None
        if params["auto_find_local"]:
            if not local_price_path:
                found = find_local_parquet_files(params["dataset"], params["start"], params["end"], "price")
                if found:
                    local_price_path = str(found)
            found_factor = find_local_parquet_files(params["dataset"], params["start"], params["end"], "factor")
            if found_factor:
                local_factor_path = str(found_factor)

        df_price = load_price_data(
            dataset_id=params["dataset"],
            start_date=params["start"],
            end_date=params["end"],
            local_parquet_path=local_price_path,
            use_local_first=True,
        )
        if df_price.empty:
            logger.error("價量資料為空")
            return 1

        prices_alphalens = prepare_prices_for_alphalens(df_price)
        stock_ids = sorted(df_price["stock_id"].unique().tolist())
        trading_days = pd.DatetimeIndex(pd.to_datetime(df_price["date"].unique())).sort_values()

        factors_data_dict: dict[str, pd.DataFrame] = {}
        for factor_name in params["factors"]:
            df_factor = None
            try:
                df_factor = load_factor_data(
                    dataset_id=params["dataset"],
                    factor_name=factor_name,
                    start_date=params["start"],
                    end_date=params["end"],
                    local_parquet_path=local_factor_path if params["auto_find_local"] else None,
                    use_local_first=True,
                    factor_table=params["factor_table"],
                )
            except Exception as e:
                logger.warning(f"無法從 BigQuery/本地讀取因子 {factor_name}: {e}")

            if (df_factor is None or df_factor.empty) and params["from_finlab_api"]:
                try:
                    FinLabFetcher.finlab_login()
                    raw = FinLabFactorFetcher.get_factor_data(
                        stock_symbols=stock_ids,
                        factor_name=factor_name,
                        trading_days=trading_days,
                    )
                    raw = raw.rename(columns={"datetime": "date", "asset": "stock_id"})
                    raw = raw[(raw["date"] >= params["start"]) & (raw["date"] <= params["end"])]
                    df_factor = raw.set_index(["date", "stock_id"])[["value"]]
                    df_factor.columns = [factor_name]
                except Exception as e:
                    logger.warning(f"FinLab API 取得因子 {factor_name} 失敗: {e}")

            if df_factor is not None and not df_factor.empty:
                factors_data_dict[factor_name] = df_factor
            else:
                logger.warning(f"跳過因子 {factor_name}（無資料）")

        if not factors_data_dict:
            logger.error("沒有任何因子資料可分析")
            return 1

        logger.info(f"已載入 {len(factors_data_dict)} 個因子: {list(factors_data_dict.keys())}")

        if params["mode"] == "weighted_rank":
            report_paths = run_weighted_rank(
                df_price, factors_data_dict, params, prices_alphalens, periods
            )
        else:
            report_paths = run_pca(
                df_price, factors_data_dict, params, prices_alphalens, periods
            )

        logger.info(f"共產生 {len(report_paths)} 份報告")

        # 所有組合／主成分分析完成後，一次性上傳所有報告至 GCS
        if report_paths and not params.get("skip_gcs"):
            bucket_name = os.getenv("GCS_BUCKET")
            if bucket_name:
                start_s = (params["start"] or "").replace("-", "")
                end_s = (params["end"] or "").replace("-", "")
                mv = params.get("market_value_date") or params.get("start") or ""
                mv_s = mv.replace("-", "") if isinstance(mv, str) else ""
                range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"

                logger.info("開始上傳 %d 個多因子報告至 GCS...", len(report_paths))
                for report_dir in report_paths:
                    if not report_dir or not report_dir.exists():
                        continue
                    outer_folder = report_dir.parent.name
                    label_folder = report_dir.name
                    gcs_prefix = (
                        f"data/multi_factor_analysis_reports/{range_dir}/{outer_folder}/{label_folder}"
                    )
                    for f in report_dir.iterdir():
                        if f.is_file():
                            try:
                                upload_file(
                                    bucket_name,
                                    f,
                                    f"{gcs_prefix}/{f.name}",
                                )
                                logger.info(
                                    "已上傳多因子報表至 GCS: %s/%s",
                                    gcs_prefix,
                                    f.name,
                                )
                            except Exception as e:
                                logger.warning("上傳 %s 至 GCS 失敗: %s", f.name, e)
                logger.info("所有多因子報告已上傳至 GCS 完成")
            else:
                logger.warning("GCS_BUCKET 未設定，略過上傳多因子報表至 GCS")

        logger.info("=== Alphalens 多因子分析完成 ===")
        return 0

    except Exception as e:
        import traceback
        logger.error(f"分析失敗: {e}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
