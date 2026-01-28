## Finance Data Pipeline

一個用來抓取台股資料、清洗轉換後上傳到 BigQuery 的簡單 ETL pipeline。  
資料來源包含：
- **FinLab**：選出市值前 N 大且符合條件的台股清單
- **yfinance**：下載多檔股票的歷史收盤價
- **Pandas / NumPy**：資料轉換與效能優化
- **Google Cloud Storage / BigQuery**：作為資料湖與資料倉儲

---

### 專案結構 (重點)

- `scripts/run_etl_pipeline.py`：主 ETL 腳本，負責串起整個流程
- `ingestion/`  
  - `stock_selector.py`：使用 FinLab 挑選 Top N 市值股票（較舊版本，可參考邏輯）
  - `finlab_fetcher.py`：封裝 FinLab 登入與取得 Top N 市值股票清單 (`FinLabFetcher.fetch_top_stocks_by_market_value`)
  - `yfinance_fetcher.py`：封裝 yfinance 抓價：單檔 `fetch` 與多檔 `fetch_daily_close_prices`（提供給 pipeline 使用）
  - `get_close_price.py`：較舊的 yfinance 抓價函式版本（可以作為範例 / 備用）
  - `base_fetcher.py`：抓取器基底類別
- `processing/transformer.py`：將 wide format 價格資料轉成 long format，計算日報酬
- `utils/`  
  - `google_cloud_storage.py`：GCS 上傳與下載
  - `google_cloud_bigquery.py`：將資料上傳至 BigQuery，支援 upsert (暫存表 + MERGE)
  - `google_cloud_platform.py`：檢查 / 建立 `gcp_keys` 金鑰目錄並確認金鑰存在
  - `logger.py`：實務化 logging 設定（支援 LOG_LEVEL / LOG_DIR，輸出到 console 與輪替檔案）

---

### 環境需求

- Python 版本：**3.10+** 建議
- 作業系統：macOS / Linux / WSL 皆可

安裝相依套件：

```bash
pip install -r requirements.txt
```

---

### 環境變數與設定

專案依賴 `.env` 以及 GCP 金鑰與自訂設定檔。

#### 1. `.env`

在專案根目錄建立 `.env` 檔，至少包含：

```env
FINLAB_API_TOKEN=你的_finlab_token
GCP_PROJECT_ID=你的_gcp_project_id
GCS_BUCKET=你的_gcs_bucket_name
LOG_LEVEL=INFO              # 選填：DEBUG/INFO/WARNING/ERROR/CRITICAL
LOG_DIR=./logs              # 選填：自訂 log 目錄，預設為專案根目錄 logs/
```

`scripts/run_etl_pipeline.py` 會透過 `python-dotenv` 自動載入這些變數。

#### 2. GCP 金鑰 (`gcp_keys/`)

- 在專案根目錄建立 `gcp_keys/` 資料夾 (程式會自動建立，但你也可以手動建立)
- 將 **GCP Service Account JSON 金鑰** 放到 `gcp_keys/` 下，例如：  
  - `gcp_keys/my-gcp-key.json`
- `utils/google_cloud_platform.py` 會：
  - 確保 `gcp_keys/` 存在
  - 在該資料夾下建立 `.gitignore` 並忽略 `*.json`
  - 選擇最後修改時間最新的 JSON 作為使用金鑰

> 注意：根目錄的 `.gitignore` 也會忽略 `gcp_keys/` 與該目錄下的 JSON，避免金鑰被 commit。

#### 3. 設定檔 `config/settings.yaml`

主流程會讀取 `config/settings.yaml`，建議結構如下 (可依需求調整)：

```yaml
top_stocks:
  excluded_industry: []        # 要排除的產業列表
  pre_list_date: "2015-01-01"  # 上市日期需早於此日期
  top_n: 50                    # 市值前 N 大

yfinance:
  start: "2018-01-01"
  end: null                    # 或指定結束日，例如 "2024-12-31"

bigquery:
  dataset: "your_dataset_name"
```

---

### 執行 ETL Pipeline

確定以下條件都已完成：

- 已建立 `.env` 並填入 `FINLAB_API_TOKEN`, `GCP_PROJECT_ID`, `GCS_BUCKET`
- 已將 GCP Service Account 金鑰放入 `gcp_keys/`
- 已建立 `config/settings.yaml`
- 已安裝 requirements

執行：

```bash
python scripts/run_etl_pipeline.py
```

流程包含三個步驟：

1. **Ingestion**
   - 使用 FinLab 取得 Top N 市值股票清單 (`ingestion/stock_selector.py`)
   - 使用 yfinance 抓取這些股票的歷史收盤價 (`ingestion/get_close_price.py`)
   - 將 raw parquet 檔寫入 `data/raw/{YYYY-MM-DD}/`，並上傳到 GCS
2. **Transformation**
   - 將 wide format 轉為 long format (`processing/transformer.py`)
   - 補齊缺失值、計算 daily return，並輸出 parquet 至 `data/processed/{YYYY-MM-DD}/`
3. **Loading**
   - 透過 `utils/google_cloud_bigquery.py` 將資料 upsert 到 BigQuery `fact_daily_price` 表

---

### 開發與除錯建議

- 若在 GCS / BigQuery 權限相關步驟遇到 `invalid_grant` 或驗證失敗，可在終端機執行：

  ```bash
  gcloud auth application-default login
  ```

- 可先在互動式環境 (例如 Jupyter / VSCode Notebook) 單獨測試：
  - `get_top_stocks_by_market_value`
  - `get_daily_close_prices_data`
  - `Transformer.process_market_data`
  - `load_to_bigquery`

---

### 待辦 / 可改進方向

- 在 GCS / BigQuery 操作上增加重試機制與更完整的錯誤處理
- 封裝更通用的 CLI 介面（例如 `python -m scripts.run_etl_pipeline --start 2020-01-01 --end 2024-01-01`）
- 增加單元測試與 CI pipeline

