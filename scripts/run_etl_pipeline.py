"""
ETL Pipeline 主腳本：台股價量與因子資料抓取、轉換、寫入 BigQuery。

流程：
    1. 參數解析：依 CLI 或 config/settings.yaml 取得 market_value_date、start、end 等
    2. Ingestion：FinLab 取得 Top N 市值股票清單 → yfinance 抓取 OHLCV 原始價量
    3. Transformation：Transformer 清洗 OHLCV、計算日報酬、標記交易可行性（漲跌停、停牌）
    4. Loading：寫入 BigQuery（fact_price、dim_universe、dim_calendar、fact_benchmark_daily、dim_backtest_config、可選 fact_factor）
    5. 本地輸出：data/raw/{date}/、data/processed/{date}/，可選上傳 GCS

依賴：
    .env：FINLAB_API_TOKEN、GCP_PROJECT_ID、GCS_BUCKET
    config/settings.yaml：etl、top_stocks、yfinance、bigquery、factors 等區塊
    gcp_keys/：GCP Service Account JSON 金鑰

執行：
    python -m scripts.run_etl_pipeline --market-value-date 2017-05-16 --start 2017-05-16 --end 2021-05-15 --with-factors
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# 專案根目錄，供後續讀取 config、data 等
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()  # 載入 .env 環境變數

from ingestion.yfinance_fetcher import YFinanceFetcher
from ingestion.finlab_fetcher import FinLabFetcher
from factors.finlab_factor_fetcher import FinLabFactorFetcher
from processing.transformer import Transformer
from utils.google_cloud_storage import upload_file
from utils.google_cloud_bigquery import load_to_bigquery
from utils.logger import logger
from utils.google_cloud_platform import check_gcp_environment
from utils.cli import parse_args, load_config, resolve_params


def main() -> int:
    """
    ETL Pipeline 主流程：解析參數 → 對每個市值日執行 Ingestion → Transformation → Loading

    Returns:
        0 成功；1 參數錯誤或任一步驟失敗（Ingestion / Transformation / Loading 任一拋錯即回傳 1）

    Note:
        - 僅支援 interval 模式（固定市值日 + 區間），供回測可重現
        - 任一個市值日失敗即中斷，不回滾已寫入的 BigQuery / 本地檔案
    """
    # 參數：優先從 CLI，未指定則用 config；factor_names 可從 factors/factors_list.json 載入
    config = load_config(ROOT_DIR)
    args = parse_args(config)
    params = resolve_params(config, args, ROOT_DIR)

    bucket_name = os.getenv("GCS_BUCKET")
    bq_dataset = params["dataset_id"]  # 例：tw_top_50_stock_data

    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")  # 本地輸出子資料夾，例：2026-02-02
    timestamp = now.strftime("%Y%m%d_%H%M")  # 檔名時間戳，例：20260202_1215

    # 必填參數檢查
    if not params.get("market_value_dates") or not params["start_date"] or not params["end_date"]:
        logger.error(
            "請提供 market_value_date(s)、start、end（可從 CLI 或 config.etl / yfinance 設定）。"
        )
        return 1

    # 本地輸出目錄：raw 存 yfinance 原始資料，processed 存清洗後價量與因子
    raw_dir = ROOT_DIR / "data/raw" / date_folder
    processed_dir = ROOT_DIR / "data/processed" / date_folder
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    start_date = params["start_date"]
    end_date = params["end_date"]
    date_range_tag = f"{start_date}_to_{end_date}" if (start_date and end_date) else None  # 用於檔名，例：2017-05-16_to_2021-05-15

    # GCP 金鑰檢查：gcp_keys/ 下須有 Service Account JSON
    key_path = check_gcp_environment(ROOT_DIR)
    logger.info(f"=== STEP 0 :GCP 金鑰： {key_path} ===")

    # 對每個市值基準日分別執行 ETL（可一次跑多個市值日供滾動回測）
    for mv_date in params["market_value_dates"]:
        params["market_value_date"] = mv_date
        logger.info("=== 開始 ETL：market_value_date=%s ===", mv_date)

        # ========== STEP 1: Ingestion ==========
        # 1-A：依指定市值日，從 FinLab 取得 Top N 股票清單（universe）
        logger.info("=== STEP 1 - A: Ingestion Started 取得股票列表 ===")

        FinLabFetcher.finlab_login()
        universe_df = FinLabFetcher.fetch_top_stocks_universe(
            excluded_industry=params["excluded_industry"],  # 排除產業
            pre_list_date=params["pre_list_date"],          # 上市日期須早於此日
            top_n=params["top_n"],                          # 市值前 N 大
            market_value_date=params["market_value_date"],  # 市值基準日
        )
        top_tickers = universe_df["stock_id"].tolist()
        logger.info(
            "Target Tickers (market_value_date=%s): %s",
            params["market_value_date"],
            top_tickers,
        )

        # 1-B：用 yfinance 抓取上述股票的 OHLCV 價量（auto_adjust 處理除權息）
        logger.info("=== STEP 1 - B: Ingestion Started 抓取股價(Raw, OHLCV) ===")

        try:
            # 檔名格式：mv{YYYYMMDD}_top{N}_ohlcv_raw_{date_range}_{timestamp}.parquet
            df_ohlcv_raw = YFinanceFetcher.fetch_daily_ohlcv_data(
                stock_symbols=top_tickers,
                start_date=start_date,
                end_date=end_date,
                is_tw_stock=True,
            )

            mv_tag = params["market_value_date"].replace("-", "")
            top_n = params["top_n"]
            if date_range_tag:
                raw_filename = f"mv{mv_tag}_top{top_n}_ohlcv_raw_{date_range_tag}_{timestamp}.parquet"
            else:
                raw_filename = f"mv{mv_tag}_top{top_n}_ohlcv_raw_{timestamp}.parquet"
            raw_local_path = raw_dir / raw_filename
            df_ohlcv_raw.to_parquet(raw_local_path)

            # 可選：上傳 raw parquet 至 GCS data/raw/{date_folder}/
            if not params["skip_gcs"]:
                upload_file(
                    bucket_name,
                    raw_local_path,
                    f"data/raw/{date_folder}/{raw_filename}",
                )

        except Exception as e:
            logger.error(f"Ingestion Failed (market_value_date={mv_date}): {e}")
            return 1

        # ========== STEP 2: Transformation ==========
        # OHLCV 清洗：補值、daily_return、is_suspended / is_limit_up / is_limit_down
        logger.info("=== STEP 2: Transformation Started (OHLCV) ===")

        try:
            df_cleaned_price = Transformer.process_ohlcv_data(df_ohlcv_raw)

            if df_cleaned_price.empty:
                raise ValueError("Transformed data is empty! Check yfinance source.")

            df_cleaned_price["stock_id"] = df_cleaned_price["stock_id"].astype(str)

            # 同一天同一股票重複列會導致 BigQuery upsert 結果不確定，須先 drop_duplicates
            duplicates = df_cleaned_price.duplicated(subset=['date', 'stock_id']).sum()
            if duplicates > 0:
                logger.warning(f"Detected {duplicates} duplicate rows. Dropping duplicates...")
                df_cleaned_price = df_cleaned_price.drop_duplicates(subset=['date', 'stock_id'])

            processed_dir.mkdir(parents=True, exist_ok=True)

            # 檔名格式：fact_price_ohlcv_mv{YYYYMMDD}_top{N}_{date_range}_{timestamp}.parquet
            if date_range_tag:
                processed_path = processed_dir / f"fact_price_ohlcv_mv{mv_tag}_top{top_n}_{date_range_tag}_{timestamp}.parquet"
            else:
                processed_path = processed_dir / f"fact_price_ohlcv_mv{mv_tag}_top{top_n}_{timestamp}.parquet"

            df_cleaned_price.to_parquet(processed_path, index=False, compression="snappy")

            # 可選：上傳 processed 價量 parquet 至 GCS
            if not params["skip_gcs"]:
                upload_file(
                    bucket_name,
                    processed_path,
                    f"data/processed/{date_folder}/{processed_path.name}",
                )

            logger.info(f"Transformation Success! Saved to: {processed_path.name}")
            logger.info(f"Summary: {len(df_cleaned_price)} rows, {df_cleaned_price['stock_id'].nunique()} tickers.")

        except Exception as e:
            logger.error(f"Transformation Failed (market_value_date={mv_date}): {str(e)}")
            return 1

        # ========== STEP 3: Loading ==========
        # 寫入 BigQuery：fact_price 用 upsert（依 date+stock_id），其餘表用 truncate 覆寫
        logger.info("=== STEP 3: Loading to BigQuery Started ===")

        try:
            mv_date_bq = params["market_value_date"].replace("-", "")
            start_tag = params["start_date"].replace("-", "")
            end_tag = params["end_date"].replace("-", "")
            
            # Dataset 命名：{base}_s{YYYYMMDD}_e{YYYYMMDD}_mv{YYYYMMDD}，例：tw_top_50_stock_data_s20170516_e20210515_mv20170516
            target_dataset = f"{bq_dataset}_s{start_tag}_e{end_tag}_mv{mv_date_bq}"

            # fact_price：價量事實表，upsert 避免重複列
            load_to_bigquery(
                df=df_cleaned_price,
                dataset_id=target_dataset,
                table_id="fact_price",
                if_exists="upsert",
            )

            # dim_universe：該市值日的 Top N 股票清單（含 delist_date 等）
            load_to_bigquery(
                df=universe_df,
                dataset_id=target_dataset,
                table_id="dim_universe",
                if_exists="truncate",
            )

            # dim_calendar：交易日曆，由價量日期產生，供回測對齊交易日
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

            # fact_benchmark_daily：基準指數（預設 ^TWII）日收盤與日報酬
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

            # dim_backtest_config：回測預設參數（手續費 fee_bps、證交稅 tax_bps 等）
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

            # fact_factor（可選）：財報因子日頻資料，需 --with-factors 且 factor_names 非空
            # factor_names 優先順序：--factor-names > config > factors/factors_list.json
            factor_names = params.get("factor_names") or []
            if params.get("with_factors") and factor_names:
                trading_days = pd.DatetimeIndex(df_cleaned_price["date"].unique()).sort_values()
                # 從 FinLab 抓取多個因子，季頻展開為日頻（ffill）
                df_factor = FinLabFactorFetcher.fetch_factors_daily(
                    stock_ids=top_tickers,
                    factor_names=factor_names,
                    start_date=start_date,
                    end_date=end_date,
                    trading_days=trading_days,
                )
                if not df_factor.empty:
                    # 儲存因子至本地 parquet，供多因子分析／回測的 --auto-find-local 使用
                    if date_range_tag:
                        factor_parquet_name = f"fact_factor_mv{mv_tag}_top{top_n}_{date_range_tag}_{timestamp}.parquet"
                    else:
                        factor_parquet_name = f"fact_factor_mv{mv_tag}_top{top_n}_{timestamp}.parquet"
                    factor_parquet_path = processed_dir / factor_parquet_name
                    df_factor.to_parquet(factor_parquet_path, index=False, compression="snappy")
                    logger.info(f"Factor data saved to: {factor_parquet_path.name}")

                    if not params["skip_gcs"]:
                        upload_file(
                            bucket_name,
                            factor_parquet_path,
                            f"data/processed/{date_folder}/{factor_parquet_name}",
                        )

                    # 因子表名可加 suffix（例 fact_factor_value）以並存多組因子
                    suffix = params.get("factor_table_suffix")
                    factor_table = "fact_factor"
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