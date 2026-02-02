"""
多因子回測腳本（Backtrader + PyFolio）

參考 references/回測/main_for_multiple_factors_backtrader_2.py，
從 BigQuery 或本地 Parquet 讀取價量與多個因子，依季度做加權排名後做多／做空回測。

流程：
    1. 讀取價量資料
    2. 列出分析區間內季度，每季取得各因子資料並排名，再以權重計算加權排名（FactorRanking.calculate_weighted_rank）
    3. 合併價量與加權排名
    4. 使用 strategies.PandasDataWithRank、FactorRankStrategy 執行回測
    5. PyFolio 產出 tear sheet

執行範例：
    python -m scripts.run_multi_factor_backtest \\
        --dataset tw_top_50_stock_data \\
        --factors "營運現金流,歸屬母公司淨利,營業利益成長率,稅前淨利成長率,稅後淨利成長率" \\
        --start 2017-05-16 --end 2021-05-15 \\
        [--weights "0.2,0.2,0.2,0.2,0.2"] \\
        [--buy-n 20] [--sell-n 20] [--initial-cash 20000000]

依賴：backtrader, pyfolio-reloaded, .env（GCP_PROJECT_ID，若用 BigQuery）
"""

from __future__ import annotations

import argparse
import sys
import warnings
from datetime import datetime
from pathlib import Path

# 在 import pyfolio 前先過濾 zipline 警告（pyfolio 載入時會觸發）
warnings.filterwarnings("ignore", message=".*zipline.*", category=UserWarning)
warnings.filterwarnings("ignore")

# 在 import pyfolio 前設定 Agg，讓 PyFolio 產生的 figure 可由 pyplot.get_fignums() 取得並存檔
import matplotlib
matplotlib.use("Agg")

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

try:
    import backtrader as bt
    import pyfolio as pf
except ImportError:
    print("錯誤：請先安裝 backtrader 與 pyfolio-reloaded")
    print("執行：pip install backtrader pyfolio-reloaded")
    sys.exit(1)

from dotenv import load_dotenv

load_dotenv()

from factors.factor_ranking import FactorRanking
from factors.finlab_factor_fetcher import FinLabFactorFetcher
from utils.cli import load_config, resolve_multi_factor_backtest_params
from utils.data_loader import load_factor_data, load_price_data
from utils.logger import logger


def _quarters_in_range(start_date: str, end_date: str):
    """列出與 [start_date, end_date] 有交集的季度 (YYYY-Qn)。"""
    from datetime import datetime

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    quarters = []
    for year in range(start.year, end.year + 2):
        for q in range(1, 5):
            qstr = f"{year}-Q{q}"
            try:
                q_start, q_end = FinLabFactorFetcher.convert_quarter_to_dates(qstr)
                qs = datetime.strptime(q_start, "%Y-%m-%d").date()
                qe = datetime.strptime(q_end, "%Y-%m-%d").date()
                if qe >= start and qs <= end:
                    quarters.append(qstr)
            except ValueError:
                pass
    return quarters


def _factor_df_to_datetime_asset_value(df_factor: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """將 load_factor_data 回傳的 MultiIndex/欄位轉成 datetime, asset, value。"""
    out = df_factor.reset_index()
    out = out.rename(columns={"date": "datetime", "stock_id": "asset"})
    if factor_name in out.columns:
        out["value"] = out[factor_name]
    else:
        val_col = [c for c in out.columns if c not in ("datetime", "asset")][0]
        out["value"] = out[val_col]
    return out[["datetime", "asset", "value"]]


def find_local_parquet_files(
    dataset_id: str,
    start_date: str,
    end_date: str,
    data_type: str = "price",
):
    """在 data/processed 下尋找符合條件的 parquet 檔案。"""
    processed_dir = ROOT_DIR / "data" / "processed"
    if not processed_dir.exists():
        return None
    pattern = "fact_price*.parquet" if data_type == "price" else "fact_factor*.parquet"
    parquet_files = list(processed_dir.rglob(pattern))
    if not parquet_files:
        return None
    return max(parquet_files, key=lambda p: p.stat().st_mtime)


def run_multi_factor_backtest(
    dataset_id: str,
    factors: list[str],
    start_date: str,
    end_date: str,
    weights: list[float] | None = None,
    local_price_path: str | None = None,
    local_factor_path: str | None = None,
    factor_table: str = "fact_factor",
    positive_corr: bool = True,
    buy_n: int = 20,
    sell_n: int = 20,
    initial_cash: float = 20_000_000,
    commission: float = 0.001,
    auto_find_local: bool = False,
) -> None:
    if not factors:
        raise ValueError("至少需指定一個因子")
    n = len(factors)
    if weights is None:
        weights = [1.0 / n] * n
    if len(weights) != n:
        raise ValueError("weights 長度須與 factors 相同")

    if auto_find_local:
        if not local_price_path:
            found = find_local_parquet_files(dataset_id, start_date, end_date, "price")
            if found:
                local_price_path = str(found)
        if not local_factor_path:
            found = find_local_parquet_files(dataset_id, start_date, end_date, "factor")
            if found:
                local_factor_path = str(found)

    # 價量
    df_price = load_price_data(
        dataset_id=dataset_id,
        start_date=start_date,
        end_date=end_date,
        local_parquet_path=local_price_path,
        use_local_first=bool(local_price_path),
    )
    df_price["datetime"] = pd.to_datetime(df_price["date"]).astype(str)
    df_price["asset"] = df_price["stock_id"].astype(str)
    df_price = df_price.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )

    # 先取得全區間各因子資料（供每季切片）
    trading_days = pd.date_range(start=start_date, end=end_date)
    rank_factors_data_dict = {}
    for factor_name in factors:
        df_f = load_factor_data(
            dataset_id=dataset_id,
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
            local_parquet_path=local_factor_path,
            use_local_first=bool(local_factor_path),
            factor_table=factor_table,
        )
        df_f = _factor_df_to_datetime_asset_value(df_f, factor_name)
        df_f["datetime"] = pd.to_datetime(df_f["datetime"])
        df_f = (
            df_f.sort_values(["asset", "datetime"])
            .groupby("asset", group_keys=False)
            .apply(lambda g: g.ffill())
            .dropna()
        )
        df_f = FactorRanking.rank_stocks_by_factor(
            df_f,
            positive_corr=positive_corr,
            rank_column="value",
            rank_result_column="rank",
        ).drop(columns=["value"], errors="ignore")
        rank_factors_data_dict[factor_name] = df_f

    # 每季加權排名
    quarters = _quarters_in_range(start_date, end_date)
    all_factor_data = []
    for quarter in quarters:
        q_start, q_end = FinLabFactorFetcher.convert_quarter_to_dates(quarter)
        ranked_dfs = []
        for factor_name in factors:
            df_r = rank_factors_data_dict[factor_name]
            mask = (df_r["datetime"] >= pd.Timestamp(q_start)) & (
                df_r["datetime"] <= pd.Timestamp(q_end)
            )
            ranked_dfs.append(df_r.loc[mask, ["datetime", "asset", "rank"]])
        if not all(not df.empty for df in ranked_dfs):
            continue
        quarter_weighted = FactorRanking.calculate_weighted_rank(
            ranked_dfs=ranked_dfs,
            weights=weights,
            positive_corr=positive_corr,
            rank_column="rank",
        )
        quarter_weighted = quarter_weighted.rename(columns={"weighted_rank": "rank"})
        all_factor_data.append(quarter_weighted)

    if not all_factor_data:
        raise ValueError("區間內無任何季度加權排名資料")
    all_factor_data = pd.concat(all_factor_data, ignore_index=True)
    all_factor_data["datetime"] = all_factor_data["datetime"].astype(str)
    all_factor_data["asset"] = all_factor_data["asset"].astype(str)
    all_factor_data["rank"] = all_factor_data["rank"].astype(float)

    # 合併價量與排名
    merge_cols = ["datetime", "asset"]
    merged = pd.merge(
        df_price[merge_cols + ["Open", "High", "Low", "Close", "Volume"]],
        all_factor_data[merge_cols + ["rank"]],
        on=merge_cols,
        how="outer",
    )
    merged = (
        merged.sort_values(["asset", "datetime"])
        .groupby("asset", group_keys=False)
        .apply(lambda g: g.ffill())
        .reset_index(drop=True)
    )
    merged = merged.dropna(subset=["Open", "Close"]).copy()
    merged["rank"] = merged["rank"].fillna(999999)

    # Cerebro
    from strategies import FactorRankStrategy, PandasDataWithRank

    cerebro = bt.Cerebro()
    each_cash = initial_cash * 0.9 / (buy_n + sell_n) if (buy_n + sell_n) else 0
    cerebro.addstrategy(
        FactorRankStrategy,
        buy_n=buy_n,
        sell_n=sell_n,
        each_cash=each_cash,
    )

    stock_list = merged["asset"].unique().tolist()
    base_stock = stock_list[0] if stock_list else None
    if not base_stock:
        raise ValueError("無股票資料")
    base_dates = merged[merged["asset"] == base_stock]["datetime"]
    base_dates = pd.to_datetime(base_dates).sort_values().reset_index(drop=True)

    for stock in stock_list:
        sub = merged[merged["asset"] == stock].copy()
        sub["datetime"] = pd.to_datetime(sub["datetime"])
        sub = sub.drop(columns=["asset"])
        sub = sub.set_index("datetime").reindex(base_dates)
        sub = sub.ffill().bfill().reset_index()
        sub = sub.rename(columns={"index": "datetime"})
        sub = sub.dropna(subset=["Open", "Close"]).sort_values("datetime").reset_index(drop=True)
        sub["rank"] = sub["rank"].fillna(999999)
        feed = PandasDataWithRank(dataname=sub)
        cerebro.adddata(feed, name=stock)

    cerebro.broker.set_cash(initial_cash)
    cerebro.broker.setcommission(commission=commission)
    cerebro.addanalyzer(bt.analyzers.PyFolio)

    logger.info("開始執行多因子回測")
    results = cerebro.run()
    strat = results[0]
    pyfoliozer = strat.analyzers.getbyname("pyfolio")
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()

    # 報告路徑與多因子分析一致：data/multi_factor_backtest_reports/s{開始}_e{結束}_mv{市值日}/multi_backtest_{因子名}_{時間戳}/
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_s = (start_date or "").replace("-", "")
    end_s = (end_date or "").replace("-", "")
    mv_s = start_s
    range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"
    factors_label = "_".join(factors).replace("/", "_").replace("\\", "_").replace(",", "_")
    folder_name = f"multi_backtest_{factors_label}_{timestamp}"
    report_dir = ROOT_DIR / "data" / "multi_factor_backtest_reports" / range_dir / folder_name
    report_dir.mkdir(parents=True, exist_ok=True)
    report_base = f"multi_backtest_{timestamp}"
    report_path = report_dir / report_base

    import matplotlib.pyplot as plt
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
    _original_close = plt.close
    plt.show = _save_on_show
    # 暫時不關閉 figure，讓 get_fignums() 能在呼叫後取得所有圖表
    def _no_close(*args, **kwargs):
        pass
    plt.close = _no_close
    try:
        pf.create_full_tear_sheet(returns)
    finally:
        plt.show = _original_show
        plt.close = _original_close

    # PyFolio 在終端機可能不呼叫 plt.show()，改為在呼叫後儲存所有已建立的 figure（僅當 hook 未存到任何圖時）
    if saved_count[0] == 0:
        for fignum in plt.get_fignums():
            fig = plt.figure(fignum)
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
            plt.close(fig)
    pdf_file.close()

    if saved_count[0] > 0:
        logger.info(f"回測報告已保存: {report_dir}")
    logger.info("多因子回測完成（PyFolio tear sheet 已輸出）")


def main():
    config = load_config(ROOT_DIR)
    cfg = config.get("multi_factor_backtest", {})

    parser = argparse.ArgumentParser(
        description="多因子回測（Backtrader + PyFolio）。預設取自 config/settings.yaml 的 multi_factor_backtest，CLI 參數可覆寫。"
    )
    parser.add_argument("--dataset", default=cfg.get("dataset"), help="BigQuery dataset ID")
    parser.add_argument(
        "--factors",
        default=",".join(cfg.get("factors") or []) if isinstance(cfg.get("factors"), list) else (cfg.get("factors") or ""),
        help="因子名稱，逗號分隔（例：營運現金流,歸屬母公司淨利,稅後淨利成長率）",
    )
    parser.add_argument("--start", default=cfg.get("start"), help="分析區間起始日 YYYY-MM-DD")
    parser.add_argument("--end", default=cfg.get("end"), help="分析區間結束日 YYYY-MM-DD")
    parser.add_argument(
        "--weights",
        default=None,
        help="各因子權重，逗號分隔，長度須與 --factors 一致；未指定則等權",
    )
    parser.add_argument("--local-price", default=None, help="價量 parquet 路徑（可選）")
    parser.add_argument("--local-factor", default=None, help="因子 parquet 路徑（可選）")
    parser.add_argument("--auto-find-local", action="store_true", help="自動在 data/processed 尋找價量與因子 parquet")
    parser.add_argument("--factor-table", default=None, help="因子表名稱")
    parser.add_argument("--positive-corr", action="store_true", help="因子與收益正相關")
    parser.add_argument("--negative-corr", action="store_true", help="因子與收益負相關")
    parser.add_argument("--buy-n", type=int, default=None, help="做多檔數")
    parser.add_argument("--sell-n", type=int, default=None, help="做空檔數")
    parser.add_argument("--initial-cash", type=float, default=None, help="初始資金")
    parser.add_argument("--commission", type=float, default=None, help="手續費率")
    args = parser.parse_args()

    params = resolve_multi_factor_backtest_params(config, args)
    for key in ("dataset", "start", "end"):
        if not params.get(key):
            parser.error(f"缺少必要參數：{key}（請在 config/settings.yaml 的 multi_factor_backtest 設定，或透過 --{key.replace('_', '-')} 傳入）")
    if not params.get("factors"):
        parser.error("缺少必要參數：factors（請在 config 的 multi_factor_backtest.factors 設定，或透過 --factors 傳入）")

    run_multi_factor_backtest(
        dataset_id=params["dataset"],
        factors=params["factors"],
        start_date=params["start"],
        end_date=params["end"],
        weights=params["weights"],
        local_price_path=params["local_price"],
        local_factor_path=params["local_factor"],
        factor_table=params["factor_table"],
        positive_corr=params["positive_corr"],
        buy_n=params["buy_n"],
        sell_n=params["sell_n"],
        initial_cash=params["initial_cash"],
        commission=params["commission"],
        auto_find_local=params.get("auto_find_local", False),
    )


if __name__ == "__main__":
    main()
