import argparse
from datetime import date
from pathlib import Path
import yaml


def parse_args() -> argparse.Namespace:
    # CLI 入口：統一解析執行參數，避免散落在多個檔案
    parser = argparse.ArgumentParser(
        description="ETL pipeline for Taiwan stock data (FinLab + yfinance)."
    )
    parser.add_argument(
        "--market-value-date",
        help="單一市值基準日期 (YYYY-MM-DD)，與 --market-value-dates 二擇一",
    )
    parser.add_argument(
        "--market-value-dates",
        help="多個市值基準日期，逗號分隔 (例: 2024-01-15,2024-02-15)，供滾動回測一次跑多期 ETL",
    )
    parser.add_argument("--start", required=True, help="價量與因子區間起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="價量與因子區間結束日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, help="市值前 N 大")
    parser.add_argument(
        "--excluded-industry",
        action="append",
        dest="excluded_industry",
        help="排除產業（可重複指定）",
    )
    parser.add_argument("--pre-list-date", help="上市日期須早於此日期 (YYYY-MM-DD)")
    parser.add_argument("--dataset", help="BigQuery dataset ID")
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="略過上傳 GCS（僅保留本地輸出）",
    )
    parser.add_argument(
        "--with-factors",
        action="store_true",
        help="一併抓取財報因子並寫入 BigQuery fact_factor",
    )
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="略過基準指數抓取與寫入",
    )
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="略過交易日曆 dim_calendar 寫入",
    )
    parser.add_argument(
        "--factor-table-suffix",
        help="因子表名後綴，同一組 (mv, start, end, top_n) 可並存多組因子 (例: value, momentum)",
    )
    return parser.parse_args()


def load_config(root_dir: Path) -> dict:
    # 統一讀取設定檔，避免將參數硬寫在程式碼中
    config_path = root_dir / "config/settings.yaml"
    return yaml.safe_load(open(config_path))


def resolve_params(config: dict, args: argparse.Namespace) -> dict:
    # CLI 參數優先，其次才使用設定檔預設
    top_stocks_cfg = config.get("top_stocks", {})
    yfinance_cfg = config.get("yfinance", {})
    bigquery_cfg = config.get("bigquery", {})

    # 多個市值日（滾動回測）或單一市值日
    if getattr(args, "market_value_dates", None) and args.market_value_dates.strip():
        market_value_dates = [d.strip() for d in args.market_value_dates.split(",") if d.strip()]
    elif getattr(args, "market_value_date", None) and args.market_value_date:
        market_value_dates = [args.market_value_date]
    else:
        market_value_dates = []

    # 排除產業可用多次參數累加，未提供則使用設定檔
    excluded_industry = (
        args.excluded_industry
        if args.excluded_industry is not None
        else top_stocks_cfg.get("excluded_industry", [])
    )
    pre_list_date = args.pre_list_date or top_stocks_cfg.get("pre_list_date")
    top_n = args.top_n if args.top_n is not None else top_stocks_cfg.get("top_n", 50)

    # start/end 必填（回測區間）
    start_date = args.start or yfinance_cfg.get("start")
    end_date = args.end or yfinance_cfg.get("end")

    # dataset 支援 {top_n} / {_top_n} 動態代換
    dataset_id = args.dataset or bigquery_cfg.get("dataset")
    if isinstance(dataset_id, str):
        dataset_id = dataset_id.replace("{top_n}", str(top_n)).replace("{_top_n}", str(top_n))

    factors_cfg = config.get("factors", {})
    benchmark_cfg = config.get("benchmark", {})
    backtest_cfg = config.get("backtest_config", {})

    return {
        "market_value_dates": market_value_dates,
        "market_value_date": market_value_dates[0] if market_value_dates else None,
        "excluded_industry": excluded_industry,
        "pre_list_date": pre_list_date,
        "top_n": top_n,
        "start_date": start_date,
        "end_date": end_date,
        "dataset_id": dataset_id,
        "skip_gcs": args.skip_gcs,
        "with_factors": args.with_factors,
        "factor_names": factors_cfg.get("factor_names", []),
        "benchmark_index_ids": benchmark_cfg.get("index_ids", ["^TWII"]),
        "backtest_config": backtest_cfg,
        "skip_benchmark": args.skip_benchmark,
        "skip_calendar": args.skip_calendar,
        "factor_table_suffix": getattr(args, "factor_table_suffix", None) or factors_cfg.get("factor_table_suffix"),
    }
