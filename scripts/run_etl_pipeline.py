# %%
import os
import sys
from pathlib import Path
from datetime import datetime, date
import yaml
from dotenv import load_dotenv

# 路徑設定
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# 載入環境變數
load_dotenv()

# 匯入模組
from ingestion.yfinance_fetcher import YFinanceFetcher
from ingestion.finlab_fetcher import FinLabFetcher
from processing.transformer import Transformer
from utils.google_cloud_storage import upload_file
from utils.google_cloud_bigquery import load_to_bigquery
from utils.logger import logger
from utils.google_cloud_platform import check_gcp_environment

# %%
# ----------------------------------------------------------
# 0. 初始化設定
# ----------------------------------------------------------
# 載入設定檔
config_path = ROOT_DIR / "config/settings.yaml"
config = yaml.safe_load(open(config_path))

# GCS & BigQuery 設定
bucket_name = os.getenv("GCS_BUCKET")
bq_dataset = config["bigquery"]["dataset"]

# 建立日期資料夾使用
now = datetime.now()
date_folder = now.strftime("%Y-%m-%d")
timestamp = now.strftime("%Y%m%d_%H%M")

# 資料夾設定
raw_dir = ROOT_DIR / "data/raw" / date_folder
processed_dir = ROOT_DIR / "data/processed" / date_folder

if not (ROOT_DIR / "data/raw" / date_folder).exists():
    raw_dir.mkdir(parents=True, exist_ok=True)

if not (ROOT_DIR / "data/processed" / date_folder).exists():
    processed_dir.mkdir(parents=True, exist_ok=True)

# 初始化 GCP 環境
key_path = check_gcp_environment(ROOT_DIR)
logger.info(f"=== STEP 0 :GCP 金鑰： {key_path} ===")


# %%
# ----------------------------------------------------------
# 1. Ingestion: 獲取資料 (Raw Data)
# ----------------------------------------------------------
logger.info("=== STEP 1 - A: Ingestion Started 取得 Top 50 股票列表 ===")

# FinLab 登入 ( .env 設定 FINLABTOKEN)
FinLabFetcher.finlab_login()

# A. 取得 Top 50 股票列表 (包含 .TW /.TWO )
'''
excluded_industry: 排除產業清單
pre_list_date: 上市日期須早於此日期
top_n: 取得前 N 大市值股票
return: 股票代碼列表 (只包含上市股票)
'''
excluded_industry = config["top_stocks"].get("excluded_industry", [])
pre_list_date = config["top_stocks"].get("pre_list_date", None)
top_n = config["top_stocks"].get("top_n", 50)

top50_tickers = FinLabFetcher.fetch_top_stocks_by_market_value(
    excluded_industry=excluded_industry,
    pre_list_date=pre_list_date,
    top_n=top_n,
)
logger.info(f"Target Tickers: {top50_tickers}")

# %%
logger.info("=== STEP 1 - B: Ingestion Started 抓取股價(Raw) ===")

# B. 抓取股價 (Raw)
start_date = config["yfinance"]["start"]
end_date = config["yfinance"]["end"] or date.today().strftime("%Y-%m-%d")

try:
    '''
    stock_symbols: 股票代碼列表
    start_date: 起始日期
    end_date: 結束日期
    is_tw_stock: stock_symbols 是否是台灣股票
    return: 每日股票收盤價資料表 (索引是日期(DatetimeIndex格式)，欄位名稱為純股票代碼)
    '''
    df_close_raw = YFinanceFetcher.fetch_daily_close_prices(
        stock_symbols=top50_tickers, 
        start_date=start_date, 
        end_date=end_date, 
        is_tw_stock=True
        )
    
    # 儲存 Raw Data 到 Local
    raw_filename = f"top50_close_raw_{timestamp}.parquet"
    raw_local_path = raw_dir / raw_filename
    df_close_raw.to_parquet(raw_local_path)
    
    # 上傳 Raw Data 到 GCS
    # 如果這裡發生 invalid_grant，請在 Terminal 執行 `gcloud auth application-default login`
    upload_file(bucket_name, raw_local_path, f"raw/yfinance/{date_folder}/{raw_filename}")

except Exception as e:
    logger.error(f"Ingestion Failed: {e}")
    sys.exit(1)

# %%
# ----------------------------------------------------------
# 2. Transformation: 清洗與轉置
# ----------------------------------------------------------
logger.info("=== STEP 2: Transformation Started ===")

try:
    # 執行轉置與清洗
    df_cleaned_price = Transformer.process_market_data(df_close_raw)
    
    # 資料驗證：檢查是否為空
    if df_cleaned_price.empty:
        raise ValueError("Transformed data is empty! Check yfinance source.")

    # 型別一致性：確保 stock_id 永遠是字串 (避免 Parquet schema 錯誤)
    df_cleaned_price['stock_id'] = df_cleaned_price['stock_id'].astype(str)
    
    # 檢查重複值：同天同一支股票不應有兩筆資料
    duplicates = df_cleaned_price.duplicated(subset=['date', 'stock_id']).sum()
    if duplicates > 0:
        logger.warning(f"Detected {duplicates} duplicate rows. Dropping duplicates...")
        df_cleaned_price = df_cleaned_price.drop_duplicates(subset=['date', 'stock_id'])

    # 儲存 Processed Data
    processed_dir = ROOT_DIR / "data/processed" / date_folder
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    processed_path = processed_dir / f"fact_price_{timestamp}.parquet"
    
    # compression='snappy' 兼顧速度與體積
    df_cleaned_price.to_parquet(processed_path, index=False, compression='snappy')
    
    logger.info(f"Transformation Success! Saved to: {processed_path.name}")
    logger.info(f"Summary: {len(df_cleaned_price)} rows, {df_cleaned_price['stock_id'].nunique()} tickers.")
    
except Exception as e:
    logger.error(f"Transformation Failed: {str(e)}")
    sys.exit(1)

# %%
# ----------------------------------------------------------
# 3. Loading: 寫入 BigQuery (Data Warehouse)
# ----------------------------------------------------------
logger.info("=== STEP 3: Loading to BigQuery Started ===")

try:
    # 寫入 Fact Table
    load_to_bigquery(
        df=df_cleaned_price, 
        dataset_id=bq_dataset, 
        table_id="fact_daily_price", 
        if_exists="append"
    )
    logger.info("Pipeline Completed Successfully!")

except Exception as e:
    logger.error(f"Loading Failed: {e}")
    sys.exit(1)
# %%