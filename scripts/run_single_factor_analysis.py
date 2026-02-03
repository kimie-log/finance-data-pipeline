"""
Alphalens 單因子分析腳本。

用途：
    - 對「一個或多個單因子」逐一跑 Alphalens，檢視分位數報酬、IC、tear sheet。
    - 與多因子分析共用 ETL 輸出的價量與因子（BigQuery 或本地 Parquet）。

資料來源：
    - 價量：BigQuery fact_price 或 data/processed/fact_price*.parquet
    - 因子：BigQuery fact_factor 或 data/processed/fact_factor*.parquet

報表輸出：
    - 本地目錄：data/single_factor_analysis_reports/s{開始}_e{結束}_mv{市值日}/single_{因子名}_{時間戳}/
    - 若 skip_gcs = false 且環境變數 GCS_BUCKET 有設定，會將 PDF / PNG 一併上傳至 GCS。

執行範例：
    python -m scripts.run_single_factor_analysis \\
        --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \\
        --start 2017-05-16 --end 2021-05-15 \\
        --factors "營運現金流,歸屬母公司淨利"

依賴：
    .env（GCP_PROJECT_ID，若用 BigQuery；GCS_BUCKET，若要上傳報表）
    alphalens-reloaded
"""

from __future__ import annotations

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Optional
from datetime import datetime
from contextlib import redirect_stdout

import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

try:
    from alphalens.tears import create_full_tear_sheet
    from alphalens.utils import get_clean_factor_and_forward_returns, MaxLossExceededError
except ImportError:
    print("錯誤：請先安裝 alphalens-reloaded")
    print("執行：pip install alphalens-reloaded")
    sys.exit(1)

from utils.data_loader import load_price_data, load_factor_data  # noqa: E402
from utils.google_cloud_storage import upload_file  # noqa: E402
from utils.logger import logger  # noqa: E402
from utils.cli import load_config  # noqa: E402
from utils.alphalens_utils import (  # noqa: E402
    prepare_prices_for_alphalens,
    find_local_parquet_files,
    ensure_factor_datetime_asset_value,
    factor_series_for_alphalens,
    save_single_factor_tear_sheet,
)


def _save_tear_sheet(
    alphalens_data: pd.DataFrame,
    label: str,
    params: dict,
) -> Optional[Path]:
    """
    向後相容的 wrapper，實作已抽到 utils.alphalens_utils.save_single_factor_tear_sheet。
    """
    return save_single_factor_tear_sheet(
        alphalens_data=alphalens_data,
        label=label,
        params=params,
        root_dir=ROOT_DIR,
        create_full_tear_sheet_func=create_full_tear_sheet,
    )


def run_single_factor_analysis(params: dict) -> int:
    """執行單因子分析（可一次跑多個因子，逐一產生報表）。"""
    dataset_id = params["dataset"]
    start = params["start"]
    end = params["end"]
    factors = params["factors"] or []
    if not factors:
        logger.error("請提供至少一個因子（config.multi_factor_analysis.factors 或 --factors）")
        return 1

    # periods / quantiles
    periods_str = params.get("periods") or "1,5,10"
    if isinstance(periods_str, str):
        periods = [int(p.strip()) for p in periods_str.split(",") if p.strip()]
    else:
        periods = list(periods_str)
    quantiles = params.get("quantiles") or 5

    # 自動尋找本地價量／因子 parquet
    local_price_path = params.get("local_price")
    local_factor_path = params.get("local_factor")
    if params.get("auto_find_local"):
        if not local_price_path:
            found = find_local_parquet_files(ROOT_DIR, dataset_id, start, end, "price")
            if found:
                local_price_path = str(found)
        if not local_factor_path:
            found = find_local_parquet_files(
                ROOT_DIR, dataset_id, start, end, "factor"
            )
            if found:
                local_factor_path = str(found)

    # 價量資料
    df_price = load_price_data(
        dataset_id=dataset_id,
        start_date=start,
        end_date=end,
        local_parquet_path=local_price_path,
        use_local_first=bool(local_price_path),
    )
    prices_alphalens = prepare_prices_for_alphalens(df_price)

    factor_table = params.get("factor_table") or "fact_factor"

    # 為本次執行產生統一的時間戳，供所有因子共用（資料夾與檔名一致）
    if not params.get("run_timestamp"):
        params["run_timestamp"] = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 收集所有成功產生的報告目錄，最後一次性上傳 GCS
    report_dirs = []

    for factor_name in factors:
        logger.info("開始單因子分析：%s", factor_name)
        # 讀取因子資料（僅支援 BigQuery / 本地 Parquet；若需 FinLab 即時抓取，建議先寫入 fact_factor）
        df_factor_raw = load_factor_data(
            dataset_id=dataset_id,
            factor_name=factor_name,
            start_date=start,
            end_date=end,
            local_parquet_path=local_factor_path,
            use_local_first=bool(local_factor_path),
            factor_table=factor_table,
        )
        if df_factor_raw is None or df_factor_raw.empty:
            logger.warning("因子 %s 無資料，略過。", factor_name)
            continue

        df_factor_std = ensure_factor_datetime_asset_value(
            df_factor_raw, factor_name
        )
        factor_series = factor_series_for_alphalens(df_factor_std)


        max_loss = params.get("max_loss") or 0.35
        try:
            alphalens_data = get_clean_factor_and_forward_returns(
                factor=factor_series,
                prices=prices_alphalens,
                quantiles=quantiles,
                periods=periods,
                max_loss=max_loss,
            )
        except MaxLossExceededError as e:
            logger.warning(
                "因子 %s 的有效資料損失率過高（%s），略過此因子並繼續後續因子。",
                factor_name,
                e,
            )
            continue

        report_dir = _save_tear_sheet(alphalens_data, label=factor_name, params=params)
        if report_dir:
            report_dirs.append(report_dir)

    # 所有因子分析完成後，一次性上傳所有報告至 GCS
    if report_dirs and not params.get("skip_gcs"):
        bucket_name = os.getenv("GCS_BUCKET")
        if bucket_name:
            start_s = (start or "").replace("-", "")
            end_s = (end or "").replace("-", "")
            mv = params.get("market_value_date") or start or ""
            mv_s = mv.replace("-", "") if isinstance(mv, str) else ""
            range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"

            logger.info("開始上傳 %d 個因子報告至 GCS...", len(report_dirs))
            for report_dir in report_dirs:
                folder_name = report_dir.name
                gcs_prefix = (
                    f"data/single_factor_analysis_reports/{range_dir}/{folder_name}"
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
                                "已上傳單因子報表至 GCS: %s/%s",
                                gcs_prefix,
                                f.name,
                            )
                        except Exception as e:
                            logger.warning("上傳 %s 至 GCS 失敗: %s", f.name, e)
            logger.info("所有單因子報告已上傳至 GCS 完成")
        else:
            logger.warning("GCS_BUCKET 未設定，略過上傳單因子報表至 GCS")

    return 0


def main() -> int:
    """CLI 入口：解析參數 → 執行單因子分析。"""
    config = load_config(ROOT_DIR)
    # 單因子分析預設從 single_factor_analysis 讀取；若未設定則回退到 multi_factor_analysis 以維持相容性
    cfg = config.get("single_factor_analysis", {}) or config.get(
        "multi_factor_analysis", {}
    )

    parser = argparse.ArgumentParser(
        description="Alphalens 單因子分析。預設使用 config/settings.yaml 的 multi_factor_analysis 區塊。"
    )
    parser.add_argument(
        "--dataset", default=cfg.get("dataset"), help="BigQuery Dataset ID"
    )
    parser.add_argument(
        "--start", default=cfg.get("start"), help="分析區間起始 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", default=cfg.get("end"), help="分析區間結束 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--market-value-date",
        default=cfg.get("market_value_date"),
        help="市值基準日 (YYYY-MM-DD)，用於報告路徑 mv 資料夾名",
    )
    parser.add_argument(
        "--local-price", default=cfg.get("local_price"), help="本地價量 parquet 路徑"
    )
    parser.add_argument(
        "--local-factor", default=None, help="本地因子 parquet 路徑（可選）"
    )
    parser.add_argument(
        "--quantiles",
        type=int,
        default=cfg.get("quantiles", 5),
        help="分位數數量（預設 5 組）",
    )
    parser.add_argument(
        "--periods",
        type=str,
        default=cfg.get("periods", "1,5,10"),
        help="前瞻期間，逗號分隔，如 1,5,10",
    )
    parser.add_argument(
        "--factor-table",
        default=cfg.get("factor_table", "fact_factor"),
        help="因子表名稱（預設 fact_factor）",
    )
    parser.add_argument(
        "--auto-find-local",
        action="store_true",
        help="自動在 data/processed 尋找本地價量與因子 parquet",
    )
    parser.add_argument(
        "--from-finlab-api",
        action="store_true",
        help="（目前單因子腳本不直接支援 FinLab 即時抓取，建議先透過 ETL 寫入 fact_factor 再分析）",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help="要分析的因子名稱，逗號分隔（例：營運現金流,歸屬母公司淨利）",
    )
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="略過上傳單因子報表至 GCS；未加則依 config.single_factor_analysis.skip_gcs",
    )

    args = parser.parse_args()

    # 參數合併順序（單因子分析專用）：
    # CLI > config.single_factor_analysis > config.multi_factor_analysis > 預設值
    mcfg = config.get("multi_factor_analysis", {})

    dataset = getattr(args, "dataset", None) or cfg.get("dataset") or mcfg.get(
        "dataset"
    )
    start = getattr(args, "start", None) or cfg.get("start") or mcfg.get("start")
    end = getattr(args, "end", None) or cfg.get("end") or mcfg.get("end")
    mv_date = (
        getattr(args, "market_value_date", None)
        or cfg.get("market_value_date")
        or mcfg.get("market_value_date")
    )
    local_price = (
        getattr(args, "local_price", None)
        or cfg.get("local_price")
        or mcfg.get("local_price")
    )
    local_factor = getattr(args, "local_factor", None) or cfg.get("local_factor")
    quantiles = getattr(args, "quantiles", None) or cfg.get("quantiles") or mcfg.get(
        "quantiles", 5
    )
    periods_str = (
        getattr(args, "periods", None)
        or cfg.get("periods")
        or mcfg.get("periods", "1,5,10")
    )
    factor_table = (
        getattr(args, "factor_table", None)
        or cfg.get("factor_table")
        or mcfg.get("factor_table")
        or "fact_factor"
    )
    auto_find_local = (
        getattr(args, "auto_find_local", False)
        or cfg.get("auto_find_local", False)
        or mcfg.get("auto_find_local", False)
    )
    skip_gcs = (
        getattr(args, "skip_gcs", False)
        or cfg.get("skip_gcs", False)
        or mcfg.get("skip_gcs", False)
    )

    # factors：單因子分析自己的 config 區塊優先
    factors_raw = getattr(args, "factors", None)
    if factors_raw and isinstance(factors_raw, str) and factors_raw.strip():
        factors = [f.strip() for f in factors_raw.split(",") if f.strip()]
    else:
        factors = cfg.get("factors") or mcfg.get("factors") or []

    params = {
        "dataset": dataset,
        "start": start,
        "end": end,
        "market_value_date": mv_date,
        "local_price": local_price,
        "local_factor": local_factor,
        "quantiles": quantiles,
        "periods": periods_str,
        "factor_table": factor_table,
        "auto_find_local": auto_find_local,
        "skip_gcs": skip_gcs,
        "factors": factors,
        "mode": "single",  # 僅供 log / 報表路徑識別
    }

    missing = [k for k in ("dataset", "start", "end") if not params.get(k)]
    if missing:
        logger.error(
            "請提供以下參數（可從 config 或 CLI 指定）：%s。例：--dataset <dataset_id> --start 2017-05-16 --end 2021-05-15",
            ", ".join(missing),
        )
        return 1

    # factors 預設順序：
    #   1) CLI --factors
    #   2) config.single_factor_analysis.factors
    #   3) config.multi_factor_analysis.factors
    #   4) etl.factors.factors_list 指向的 factors_list.json（fundamental_features）
    if not params.get("factors"):
        etl_cfg = config.get("etl", {})
        factors_cfg = etl_cfg.get("factors", {})
        factors_list_path = factors_cfg.get("factors_list")
        if not factors_list_path:
            default_path = ROOT_DIR / "factors" / "factors_list.json"
            if default_path.exists():
                factors_list_path = str(default_path)
        if factors_list_path:
            try:
                with open(factors_list_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                params["factors"] = data.get("fundamental_features") or []
                logger.info(
                    "從 %s 載入 %d 個預設因子供單因子分析使用",
                    factors_list_path,
                    len(params["factors"]),
                )
            except Exception as e:
                logger.warning("載入 factors_list.json 失敗（%s），請改用 --factors 指定因子名稱", e)

    # 最終仍無因子則報錯
    if not params.get("factors"):
        logger.error("請提供至少一個因子（config.single_factor_analysis.factors 或 --factors）")
        return 1

    return run_single_factor_analysis(params)


if __name__ == "__main__":
    sys.exit(main())

