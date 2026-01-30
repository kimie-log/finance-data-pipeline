import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# 路徑設定：確保可以從任意工作目錄執行
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# 載入環境變數：集中管理 API Token / GCP 設定
load_dotenv()

# 匯入模組：集中在頂部方便依賴關係一眼看清
from ingestion.yfinance_fetcher import YFinanceFetcher
from ingestion.finlab_fetcher import FinLabFetcher
from ingestion.finlab_factor_fetcher import fetch_factors_daily
from processing.transformer import Transformer
from utils.google_cloud_storage import upload_file
from utils.google_cloud_bigquery import load_to_bigquery
from utils.logger import logger
from utils.google_cloud_platform import check_gcp_environment
from utils.cli import parse_args, load_config, resolve_params


def main() -> int:
    # 解析 CLI 與設定檔參數
    args = parse_args()
    config = load_config(ROOT_DIR)
    params = resolve_params(config, args)

    # GCS & BigQuery 設定：從環境與設定檔取得
    bucket_name = os.getenv("GCS_BUCKET")
    bq_dataset = params["dataset_id"]

    # 建立日期資料夾使用：以日期分區方便管理與回溯
    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%d_%H%M")

    # 僅支援 interval 模式（固定市值日 + 區間），供回測可重現
    if not params.get("market_value_dates") or not params["start_date"] or not params["end_date"]:
        logger.error("請提供 --market-value-date 或 --market-value-dates，且 --start、--end 皆為必填。")
        return 1

    raw_dir = ROOT_DIR / "data/raw" / "interval" / date_folder
    processed_dir = ROOT_DIR / "data/processed" / "interval" / date_folder
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # 若有指定區間，檔名與表名加入日期範圍
    start_date = params["start_date"]
    end_date = params["end_date"]
    date_range_tag = f"{start_date}_to_{end_date}" if (start_date and end_date) else None

    # 初始化 GCP 環境：確保金鑰存在且避免被 Git 追蹤
    key_path = check_gcp_environment(ROOT_DIR)
    logger.info(f"=== STEP 0 :GCP 金鑰： {key_path} ===")

    # 滾動回測：對每個市值日各跑一次完整 ETL
    for mv_date in params["market_value_dates"]:
        params["market_value_date"] = mv_date
        logger.info("=== 開始 ETL：market_value_date=%s ===", mv_date)

        # ----------------------------------------------------------
        # 1. Ingestion: 獲取資料 (Raw Data)
        # ----------------------------------------------------------
        logger.info("=== STEP 1 - A: Ingestion Started 取得股票列表 ===")

        FinLabFetcher.finlab_login()
        universe_df = FinLabFetcher.fetch_top_stocks_universe(
            excluded_industry=params["excluded_industry"],
            pre_list_date=params["pre_list_date"],
            top_n=params["top_n"],
            market_value_date=params["market_value_date"],
        )
        top_tickers = universe_df["stock_id"].tolist()
        logger.info(
            "Target Tickers (market_value_date=%s): %s",
            params["market_value_date"],
            top_tickers,
        )

        logger.info("=== STEP 1 - B: Ingestion Started 抓取股價(Raw, OHLCV) ===")

        try:
            df_ohlcv_raw = YFinanceFetcher.fetch_daily_ohlcv_data(
                stock_symbols=top_tickers,
                start_date=start_date,
                end_date=end_date,
                is_tw_stock=True,
            )

            # 本地檔名含 mv 與 top_n，便於與 BigQuery 表名對應
            mv_tag = params["market_value_date"].replace("-", "")
            top_n = params["top_n"]
            if date_range_tag:
                raw_filename = f"mv{mv_tag}_top{top_n}_ohlcv_raw_{date_range_tag}_{timestamp}.parquet"
            else:
                raw_filename = f"mv{mv_tag}_top{top_n}_ohlcv_raw_{timestamp}.parquet"
            raw_local_path = raw_dir / raw_filename
            df_ohlcv_raw.to_parquet(raw_local_path)

            if not params["skip_gcs"]:
                upload_file(
                    bucket_name,
                    raw_local_path,
                    f"data/raw/interval/{date_folder}/{raw_filename}",
                )

        except Exception as e:
            logger.error(f"Ingestion Failed (market_value_date={mv_date}): {e}")
            return 1

        # ----------------------------------------------------------
        # 2. Transformation: 清洗與轉置
        # ----------------------------------------------------------
        logger.info("=== STEP 2: Transformation Started (OHLCV) ===")

        try:
            df_cleaned_price = Transformer.process_ohlcv_data(df_ohlcv_raw)

            if df_cleaned_price.empty:
                raise ValueError("Transformed data is empty! Check yfinance source.")

            df_cleaned_price['stock_id'] = df_cleaned_price['stock_id'].astype(str)

            duplicates = df_cleaned_price.duplicated(subset=['date', 'stock_id']).sum()
            if duplicates > 0:
                logger.warning(f"Detected {duplicates} duplicate rows. Dropping duplicates...")
                df_cleaned_price = df_cleaned_price.drop_duplicates(subset=['date', 'stock_id'])

            processed_dir.mkdir(parents=True, exist_ok=True)

            if date_range_tag:
                processed_path = processed_dir / f"fact_price_ohlcv_mv{mv_tag}_top{top_n}_{date_range_tag}_{timestamp}.parquet"
            else:
                processed_path = processed_dir / f"fact_price_ohlcv_mv{mv_tag}_top{top_n}_{timestamp}.parquet"

            df_cleaned_price.to_parquet(processed_path, index=False, compression='snappy')

            if not params["skip_gcs"]:
                upload_file(
                    bucket_name,
                    processed_path,
                    f"data/processed/interval/{date_folder}/{processed_path.name}",
                )

            logger.info(f"Transformation Success! Saved to: {processed_path.name}")
            logger.info(f"Summary: {len(df_cleaned_price)} rows, {df_cleaned_price['stock_id'].nunique()} tickers.")

        except Exception as e:
            logger.error(f"Transformation Failed (market_value_date={mv_date}): {str(e)}")
            return 1

        # ----------------------------------------------------------
        # 3. Loading: 寫入 BigQuery (Data Warehouse)
        # ----------------------------------------------------------
        logger.info("=== STEP 3: Loading to BigQuery Started ===")

        try:
            mv_date_bq = params["market_value_date"].replace("-", "")
            start_tag = params["start_date"].replace("-", "")
            end_tag = params["end_date"].replace("-", "")
            target_dataset = f"{bq_dataset}_interval"
            target_table = f"fact_price_mv{mv_date_bq}_s{start_tag}_e{end_tag}_top{top_n}"

            load_to_bigquery(
                df=df_cleaned_price,
                dataset_id=target_dataset,
                table_id=target_table,
                if_exists="upsert",
            )

            universe_table = f"dim_universe_mv{mv_date_bq}_top{top_n}"
            load_to_bigquery(
                df=universe_df,
                dataset_id=target_dataset,
                table_id=universe_table,
                if_exists="truncate",
            )

            if not params["skip_calendar"]:
                calendar_df = pd.DataFrame(
                    {"date": pd.to_datetime(df_cleaned_price["date"].unique()), "is_trading_day": 1}
                )
                load_to_bigquery(
                    df=calendar_df,
                    dataset_id=target_dataset,
                    table_id="dim_calendar",
                    if_exists="truncate",
                )

            if not params["skip_benchmark"] and params["benchmark_index_ids"]:
                df_benchmark = YFinanceFetcher.fetch_benchmark_daily(
                    index_ids=params["benchmark_index_ids"],
                    start_date=start_date,
                    end_date=end_date,
                )
                if not df_benchmark.empty:
                    load_to_bigquery(
                        df=df_benchmark,
                        dataset_id=target_dataset,
                        table_id="fact_benchmark_daily",
                        if_exists="truncate",
                    )

            if params.get("backtest_config"):
                cfg = params["backtest_config"]
                backtest_df = pd.DataFrame(
                    [{"config_key": k, "config_value": v} for k, v in cfg.items()]
                )
                load_to_bigquery(
                    df=backtest_df,
                    dataset_id=target_dataset,
                    table_id="dim_backtest_config",
                    if_exists="truncate",
                )

            factor_names = params.get("factor_names") or []
            if params.get("with_factors") and factor_names:
                trading_days = pd.DatetimeIndex(df_cleaned_price["date"].unique()).sort_values()
                df_factor = fetch_factors_daily(
                    stock_ids=top_tickers,
                    factor_names=factor_names,
                    start_date=start_date,
                    end_date=end_date,
                    trading_days=trading_days,
                )
                if not df_factor.empty:
                    suffix = params.get("factor_table_suffix")
                    factor_table = f"fact_factor_mv{mv_date_bq}_s{start_tag}_e{end_tag}_top{top_n}"
                    if suffix:
                        factor_table = f"{factor_table}_{suffix}"
                    load_to_bigquery(
                        df=df_factor,
                        dataset_id=target_dataset,
                        table_id=factor_table,
                        if_exists="truncate",
                    )

            logger.info("Pipeline Completed for market_value_date=%s", mv_date)

        except Exception as e:
            import traceback
            logger.error("Loading Failed (market_value_date=%s): %s", mv_date, e)
            logger.error(traceback.format_exc())
            return 1

    logger.info("Pipeline Completed Successfully! (all market_value_dates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())