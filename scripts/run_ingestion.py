# %%
import os
import sys
from pathlib import Path
from datetime import datetime, date
import yaml
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

# 匯入模組
from ingestion.get_close_price import get_daily_close_prices_data
from ingestion.finlab_fetcher import FinLabFetcher
from ingestion.stock_selector import get_top_stocks_by_market_value
from utils.gcs import upload_file
from utils.logger import logger

# %%
# -----------------------------
# 設定 config
# -----------------------------
config_path = ROOT_DIR / "config/settings.yaml"
config = yaml.safe_load(open(config_path))
bucket = os.getenv("GCS_BUCKET")

# %%
# -----------------------------
# 時間與目錄設定
# -----------------------------
now = datetime.now()
date_folder = now.strftime("%Y-%m-%d")
timestamp = now.strftime("%Y%m%d_%H%M")

raw_dir = ROOT_DIR / "data/raw" / date_folder
raw_dir.mkdir(parents=True, exist_ok=True)

# %%
# -----------------------------
# 取得前50大台股
# -----------------------------
top50_tickers = get_top_stocks_by_market_value(
    excluded_industry=config["top_stocks"].get("excluded_industry", []),
    pre_list_date=config["top_stocks"].get("pre_list_date", None),
    top_n=config["top_stocks"].get("top_n", 50),
)
logger.info(f"Top 50 Taiwan stocks: {top50_tickers}")
print(top50_tickers)

# %%
# -----------------------------
# YFinance 抓每日收盤價
# -----------------------------
start = config["yfinance"]["start"]
end = config["yfinance"]["end"] or date.today().strftime("%Y-%m-%d")

try:
    df_close = get_daily_close_prices_data(top50_tickers, start, end)

    # parquet 檔名: top50_close_YYYYMMDD_HHMM.parquet
    filename = f"top50_close_{timestamp}.parquet"
    local_path = raw_dir / filename
    df_close.to_parquet(local_path)

    gcs_path = f"raw/yfinance/{date_folder}/{filename}"
    upload_file(bucket, local_path, gcs_path)
    logger.info(f"Uploaded top 50 close prices to GCS: {gcs_path}")

except Exception as e:
    logger.error(f"Error fetching YFinance close prices: {e}")

# %%
# -----------------------------
# FinLab 財報 (示範)
# -----------------------------
if config["finlab"].get("enabled", True):
    FinLabFetcher.finlab_login()
    logger.info("Fetching FinLab demo dataset: 現金及約當現金")
    df_finlab = FinLabFetcher.fetch_demo_financials()

    finlab_filename = f"finlab_cash_equivalents_{timestamp}.parquet"
    local_path = raw_dir / finlab_filename
    df_finlab.to_parquet(local_path)

    gcs_path = f"raw/finlab/{date_folder}/{finlab_filename}"
    upload_file(bucket, local_path, gcs_path)
    logger.info(f"Uploaded FinLab dataset to GCS: {gcs_path}")

logger.info("Week 1 ingestion pipeline completed successfully!")
