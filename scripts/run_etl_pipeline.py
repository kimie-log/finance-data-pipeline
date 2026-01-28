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
from ingestion.get_close_price import get_daily_close_prices_data
from ingestion.stock_selector import get_top_stocks_by_market_value
from ingestion.finlab_fetcher import FinLabFetcher
from processing.transformer import DataTransformer
from utils.gcs import upload_file
from utils.bigquery_loader import load_to_bigquery
from utils.logger import logger

# %%
# -----------------------------
# 0. 初始化設定
# -----------------------------
config_path = ROOT_DIR / "config/settings.yaml"
config = yaml.safe_load(open(config_path))

bucket_name = os.getenv("GCS_BUCKET")
bq_dataset = config["bigquery"]["dataset"]

now = datetime.now()
date_folder = now.strftime("%Y-%m-%d")
timestamp = now.strftime("%Y%m%d_%H%M")

raw_dir = ROOT_DIR / "data/raw" / date_folder
raw_dir.mkdir(parents=True, exist_ok=True)


# %%
# -----------------------------
# 1. Ingestion: 獲取資料 (Raw Data)
# -----------------------------
logger.info("=== STEP 1 - A: Ingestion Started 取得 Top 50 股票列表 ===")

# A. 取得 Top 50 股票列表 (現在會包含正確的 .TW/.TWO 後綴)
top50_tickers = get_top_stocks_by_market_value(
    excluded_industry=config["top_stocks"].get("excluded_industry", []),
    top_n=config["top_stocks"].get("top_n", 50),
)
logger.info(f"Target Tickers: {len(top50_tickers)}")
# print(top50_tickers[:5]) # Debug 用：檢查是否已有後綴

# %%
logger.info("=== STEP 1 - B: Ingestion Started 抓取股價(Raw) ===")

# B. 抓取股價 (Raw)
start = config["yfinance"]["start"]
end = config["yfinance"]["end"] or date.today().strftime("%Y-%m-%d")

try:
    # 注意：因為 stock_selector 已經加上後綴，get_daily_close_prices_data 裡面
    # 如果有寫死 ".TW" 的邏輯，記得要移除，直接使用傳入的 tickers
    df_close_raw = get_daily_close_prices_data(top50_tickers, start, end)
    
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
# -----------------------------
# 2. Transformation: 清洗與轉置
# -----------------------------
logger.info("=== STEP 2: Transformation Started ===")

try:
    df_cleaned_price = DataTransformer.process_market_data(df_close_raw)
    
    # 簡單的資料驗證
    if df_cleaned_price.empty:
        raise ValueError("Transformed data is empty!")
    
    # 儲存 Processed Data
    processed_dir = ROOT_DIR / "data/processed" / date_folder
    processed_dir.mkdir(parents=True, exist_ok=True)
    processed_path = processed_dir / f"fact_price_{timestamp}.parquet"
    df_cleaned_price.to_parquet(processed_path)
    
except Exception as e:
    logger.error(f"Transformation Failed: {e}")
    sys.exit(1)

# %%
# -----------------------------
# 3. Loading: 寫入 BigQuery (Data Warehouse)
# -----------------------------
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
# %%
