## Finance Data Pipeline

一個用來抓取台股資料、清洗轉換後上傳到 BigQuery 的 ETL pipeline  
資料來源包含：
- **FinLab**：選出市值前 N 大且符合條件的台股清單（universe）
- **yfinance**：下載多檔股票的歷史 OHLCV 價量資料（使用 `auto_adjust=True` 處理除權息）
- **Pandas / NumPy**：資料轉換與效能優化
- **Google Cloud Storage / BigQuery**：作為資料湖與資料倉儲

---

### 目前專案可以做到的事

#### 一、主流程：可重現回測用的 ETL（僅 interval 模式）

以**固定市值日 + 區間**為核心，從抓資料到進 BigQuery 一條龍，產出可供回測與分析使用的資料。

| 階段 | 可做到的事 |
|------|------------|
| **Ingestion** | 用 FinLab 依**指定市值日**篩選 Top N 股票（可排除產業、上市日期），取得 **universe**（含 `delist_date` 若 FinLab 有）；用 yfinance 抓這些股票的 **OHLCV**（開高低收量），並處理除權息（`auto_adjust=True`）。 |
| **Transformation** | 對 OHLCV 做清洗、補值、計算 **daily_return**，並標記 **is_suspended / is_limit_up / is_limit_down**（交易可行性）。 |
| **Loading** | 將 raw / processed parquet 寫到本地 `data/raw/interval/`、`data/processed/interval/`，可選上傳 **GCS**；再寫入 **BigQuery**（upsert 價量表、truncate 維度與輔助表）。 |

#### 二、BigQuery 產出內容

| 表／用途 | 說明 |
|----------|------|
| **fact_price_*** | 價量事實表：date、stock_id、open/high/low/close、volume、daily_return、is_suspended、is_limit_up、is_limit_down。 |
| **dim_universe_*** | 維度表：該市值日的 Top N 股票清單（含 delist_date 等）。 |
| **dim_calendar** | 交易日曆（由價量日期產生，供回測對齊）。 |
| **fact_benchmark_daily** | 基準指數（預設加權 ^TWII）日收盤與日報酬。 |
| **dim_backtest_config** | 回測預設參數（手續費、證交稅等）。 |
| **fact_factor_***（可選） | 財報／基本面因子日頻資料（需 `--with-factors` 且設定 `factors.factor_names`）。 |

#### 三、資料輸入輸出與回測／因子分析對照

專案採「固定市值日 + 區間」設計，產出的輸入／輸出與檔案**已符合**一般回測與因子分析所需；對照如下。

**輸入**

| 項目 | 來源 | 說明 |
|------|------|------|
| 市值日 + 區間 | CLI / 設定檔 | `--market-value-date`、`--start`、`--end` 固定選股與價量區間，避免前視偏差。 |
| 選股條件 | `config/settings.yaml`、CLI | `top_n`、`excluded_industry`、`pre_list_date`。 |
| 價量 | yfinance | OHLCV，`auto_adjust=True` 處理除權息。 |
| Universe | FinLab | 指定市值日的 Top N 清單（含 `delist_date` 若 FinLab 有）。 |
| 因子（可選） | FinLab | `factors.factor_names` + `--with-factors` 寫入 BigQuery。 |
| 基準指數 | yfinance | `benchmark.index_ids`（如 ^TWII）。 |
| 回測參數 | 設定檔 | `backtest_config`（手續費、證交稅等）。 |

**產出檔案與用途**

| 產出 | 位置／表名 | 回測用 | 因子分析用 |
|------|------------|--------|------------|
| 價量事實表 | `fact_price_mv{date}_s{start}_e{end}_top{n}` | ✓ 報酬、OHLCV、交易可行性 | ✓ 報酬／價量 |
| Universe 維度表 | `dim_universe_mv{date}_top{n}` | ✓ 標的清單、delist_date | ✓ 標的範圍 |
| 交易日曆 | `dim_calendar` | ✓ 對齊交易日 | ✓ 對齊日期 |
| 基準指數 | `fact_benchmark_daily` | ✓ 績效比較 | — |
| 回測參數 | `dim_backtest_config` | ✓ 手續費／稅 | — |
| 因子表（可選） | `fact_factor_mv{date}_s..._top{n}` | ✓ 選股／加權 | ✓ 因子值、排名 |
| Raw / Processed Parquet | `data/raw/interval/`、`data/processed/interval/` | 備援、重跑 | 備援、重跑 |

**回測所需對照**

- 價量與日報酬：`fact_price_*`（date, stock_id, open/high/low/close, volume, daily_return）。
- 交易可行性：`fact_price_*`（is_suspended, is_limit_up, is_limit_down）。
- 標的清單與下市日：`dim_universe_*`（stock_id, delist_date 等）。
- 交易日對齊：`dim_calendar`（date, is_trading_day）。
- 基準與成本：`fact_benchmark_daily`、`dim_backtest_config`。
- 可重現性：固定市值日 + 區間，同一組參數產出一致。

**因子分析所需對照**

- 因子值：`fact_factor_*`（date, stock_id, factor_name, value）或程式內 `get_factor_data` / `fetch_factors_daily`。
- 價量／報酬：同上 `fact_price_*`。
- 單因子／多因子排名：程式內 `rank_stocks_by_factor`、`calculate_weighted_rank`（見下方「因子相關」）。

**小結**：目前資料輸入、輸出與產生的檔案足以支援回測與因子分析。專案已實作三項優化：（1）**滾動回測**：可用 `--market-value-dates 2024-01-15,2024-02-15,...` 一次跑多個市值日 ETL；（2）**本地檔名**：raw／processed parquet 檔名含 `mv{日期}_top{n}`，與 BigQuery 表名對應；（3）**因子表並存**：同一組 (mv, start, end, top_n) 可透過 `--factor-table-suffix`（或設定檔 `factors.factor_table_suffix`）並存多組因子表。

#### 四、因子相關（供回測／選股用）

- **抓因子**：從 FinLab 取 `fundamental_features:{因子名}`，用 `.deadline()` 轉成財報截止日。
- **季頻→日頻**：用交易日序列 merge + 向前填補（ffill），展開成每日一筆，再 melt 成 long（date, stock_id, factor_name, value）。
- **季度對齊**：`convert_quarter_to_dates` / `convert_date_to_quarter` 對齊台灣財報揭露區間。
- **單因子排名**：`rank_stocks_by_factor`（每日依因子值排名，正／負相關可選）。
- **多因子加權排名**：`calculate_weighted_rank`（多個已排名表 × 權重加總後再排名）。
- **查因子清單**：`list_factors_by_type("fundamental_features")` 列出可用的財報因子名稱。


#### 五、CLI 與設定可控制的事

- **必填**：`--market-value-date` 或 `--market-value-dates`、`--start`、`--end`（固定回測區間與選股基準）。
- **選股**：`--top-n`、`--excluded-industry`、`--pre-list-date`。
- **輸出**：`--dataset`（BigQuery dataset）、`--skip-gcs`（只留本地）、`--with-factors`（一併寫入因子表）。
- **可略過**：`--skip-benchmark`、`--skip-calendar`。

設定檔 `config/settings.yaml` 可預設：top_stocks、yfinance 區間、bigquery dataset、factors.factor_names、benchmark index_ids、backtest_config 等。

#### 六、其他能力

- **GCP**：檢查/建立 `gcp_keys/`、選最新金鑰；上傳 GCS、寫入 BigQuery（含 upsert 邏輯）。
- **日誌**：logger 支援 LOG_LEVEL / LOG_DIR，輸出到 console 與輪替檔案。
- **重試**：`utils/retry.py` 通用重試（指數退避 + jitter）。
- **測試**：pytest 涵蓋 FinLab、yfinance、transformer、CLI、GCS、BigQuery、retry 等。
- **CI**：GitHub Actions 在 push/PR 時跑 `pytest`（多版 Python）。

**一句話**：專案可依「固定市值日 + 區間」跑完整 ETL，產出可重現回測用的價量、universe、交易日曆、基準、回測參數，並可選寫入財報因子；程式內可做因子取得、季頻→日頻展開、單／多因子加權排名；透過 CLI 與設定檔控制選股與輸出，並以 GCS + BigQuery 為資料湖與倉儲，搭配測試與 CI 維持品質。

---

### 專案結構 (重點)

- `scripts/run_etl_pipeline.py`：主 ETL 腳本，負責串起整個流程（FinLab universe + yfinance OHLCV + BigQuery）
- `ingestion/`  
  - `finlab_fetcher.py`：FinLab 登入與 Top N 市值 **universe**（含 `delist_date` 若 FinLab 有提供）
  - `finlab_factor_fetcher.py`：財報／基本面因子抓取並展開至日頻（供 `--with-factors` 使用）
  - `yfinance_fetcher.py`：OHLCV 抓價 `fetch_daily_ohlcv_data`、基準指數 `fetch_benchmark_daily`
  - `base_fetcher.py`：抓取器基底類別
- `processing/transformer.py`：OHLCV 清洗、日報酬、交易可行性標記（`is_suspended` / `is_limit_up` / `is_limit_down`）
- `utils/`  
  - `google_cloud_storage.py`：GCS 上傳與下載
  - `google_cloud_bigquery.py`：將資料上傳至 BigQuery，支援 upsert (暫存表 + MERGE)
  - `google_cloud_platform.py`：檢查 / 建立 `gcp_keys` 金鑰目錄並確認金鑰存在
  - `logger.py`：實務化 logging 設定（支援 LOG_LEVEL / LOG_DIR，輸出到 console 與輪替檔案）
  - `retry.py`：通用重試工具（指數退避 + jitter）
  - `cli.py`：CLI 參數解析與設定合併工具
- `test/`：pytest 單元測試與測試工具
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

在專案根目錄建立 `.env` 檔，可複製 `.env.exemple` 填寫：

```env
# Google Cloud Platform Configuration
GCP_PROJECT_ID=你的_gcp_project_id
GCS_BUCKET=你的_gcs_bucket_name

FINLAB_API_TOKEN=你的_finlab_token   
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
  dataset: "your_dataset_name"  # 可用 {top_n} / {_top_n} 自動代換

factors:
  factor_names: []             # 例: ["營業利益", "營業收入"]，搭配 --with-factors 落地 BigQuery
  factor_table_suffix: null    # 選填，因子表名後綴，同一組可並存多組 (例: value, momentum)

benchmark:
  index_ids: ["^TWII"]         # 基準指數，可加 "^TWOII" 櫃買

backtest_config:               # 回測層預設（手續費／稅），寫入 dim_backtest_config
  fee_bps: 30
  tax_bps: 10
```

---

### 執行 ETL Pipeline

確定以下條件都已完成：

- 已建立 `.env` 並填入 `FINLAB_API_TOKEN`（FinLab 驗證碼）、`GCP_PROJECT_ID`, `GCS_BUCKET`
- 已將 GCP Service Account 金鑰放入 `gcp_keys/`
- 已建立 `config/settings.yaml`
- 已安裝 requirements

執行：

```bash
python scripts/run_etl_pipeline.py
```

可選參數（CLI 介面）：

```bash
# 區間模式（固定市值基準日期，供回測可重現）
python -m scripts.run_etl_pipeline --market-value-date 2024-01-15 --start 2020-01-01 --end 2024-01-01
```

邏輯說明：
- 使用「**指定市值日期**」挑 Top N，再抓指定區間價格，確保回測可重現、減少生存者偏誤。

常用參數：
- `--market-value-date`：單一市值基準日期（與 `--market-value-dates` 二擇一）
- `--market-value-dates`：多個市值日，逗號分隔（例：`2024-01-15,2024-02-15`），一次跑多期 ETL 供滾動回測
- `--start` / `--end`：指定 yfinance 下載區間（必填）
- `--top-n`：指定市值前 N 大
- `--excluded-industry`：排除產業（可重複指定）
- `--pre-list-date`：上市日期需早於指定日期
- `--dataset`：覆寫 BigQuery dataset
- `--skip-gcs`：略過上傳 GCS（僅保留本地輸出）
- `--with-factors`：一併抓取財報因子並寫入 `fact_factor*`
- `--factor-table-suffix`：因子表名後綴，同一組 (mv, start, end, top_n) 可並存多組因子（例：`value`、`momentum`）
- `--skip-benchmark`：略過基準指數寫入
- `--skip-calendar`：略過交易日曆寫入

流程包含三個步驟：

1. **Ingestion**
   - 使用 FinLab 取得 Top N 市值股票 **universe** (`FinLabFetcher.fetch_top_stocks_universe`)
   - 使用 yfinance 抓取這些股票的歷史 OHLCV 價量資料 (`YFinanceFetcher.fetch_daily_ohlcv_data`)
   - 將 raw parquet 寫入 `data/raw/interval/{YYYY-MM-DD}/`，檔名含 `mv{日期}_top{n}` 便於與 BigQuery 表名對應（例：`mv20240115_top50_ohlcv_raw_2020-01-01_to_2024-01-01_*.parquet`），並可上傳 GCS
2. **Transformation**
   - 使用 `Transformer.process_ohlcv_data` 清洗 OHLCV long format
   - 補齊缺失值、計算 `daily_return`，輸出 parquet 至 `data/processed/interval/{YYYY-MM-DD}/`（檔名含 `mv{日期}_top{n}`，例：`fact_price_ohlcv_mv20240115_top50_2020-01-01_to_2024-01-01_*.parquet`）
   - 上傳 processed 檔到 GCS `data/processed/interval/{YYYY-MM-DD}/`
3. **Loading**
   - 寫入 BigQuery（使用 upsert，避免重複列）
     - `{dataset}_interval.fact_price_mv{market_value_date}_s{start}_e{end}_top{top_n}`
   - 同時寫入：
     - **universe**（含 `delist_date` 若 FinLab 有）：`dim_universe_mv{date}_top{n}`
     - **交易日曆**（由價量日期產生）：`dim_calendar`（除非 `--skip-calendar`）
     - **基準指數**（加權等）：`fact_benchmark_daily`（除非 `--skip-benchmark`）
     - **回測層預設參數**（手續費／稅）：`dim_backtest_config`（由 `config/settings.yaml` 的 `backtest_config`）
     - **財報因子**（可選）：`fact_factor*`（需 `--with-factors` 且設定 `factors.factor_names`）

---

### 測試與更新資料

#### 1. 如何執行測試？

執行測試（建議在專案虛擬環境中）：

```bash
python -m pytest -q
```

成功時會顯示每個測試檔案的訊息，例如：`測試成功:  test_yfinance_fetcher.py`

主要測試涵蓋：
- `test_finlab_fetcher.py`：FinLab 登入與 `fetch_top_stocks_universe`
- `test_finlab_factor_fetcher.py`：`extend_factor_data`、`get_factor_data`、`fetch_factors_daily`、`convert_quarter_to_dates`、`convert_date_to_quarter`、`list_factors_by_type`
- `test_yfinance_fetcher.py`：`fetch_daily_ohlcv_data`、`fetch_benchmark_daily` 的欄位與資料結構
- `test_transformer.py`：`process_ohlcv_data` 的清洗與日報酬計算
- `test_factor_ranking.py`：`rank_stocks_by_factor`、`calculate_weighted_rank`
- `test_cli.py`：`parse_args`、`resolve_params`
- `test_run_etl_pipeline_cli.py`：CLI 參數解析、檔名與 BigQuery 命名規則、GCS 路徑
- `test_base_fetcher.py`：`BaseFetcher.save_local`
- `test_google_cloud_bigquery.py`：`load_to_bigquery`
- `test_google_cloud_platform.py`：`check_gcp_environment`
- `test_google_cloud_storage.py`：`upload_file`、`download_file`
- `test_retry.py`：`run_with_retry`

**測試涵蓋對照（模組 → 函式 → 測試檔）**

| 模組 | 函式 | 測試檔 |
|------|------|--------|
| `ingestion/finlab_fetcher.py` | `finlab_login`, `fetch_top_stocks_universe` | `test_finlab_fetcher.py` |
| `ingestion/finlab_factor_fetcher.py` | `extend_factor_data`, `get_factor_data`, `fetch_factors_daily`, `convert_quarter_to_dates`, `convert_date_to_quarter`, `list_factors_by_type` | `test_finlab_factor_fetcher.py` |
| `ingestion/yfinance_fetcher.py` | `fetch_daily_ohlcv_data`, `fetch_benchmark_daily` | `test_yfinance_fetcher.py` |
| `ingestion/base_fetcher.py` | `save_local`（`fetch` 為抽象方法） | `test_base_fetcher.py` |
| `processing/transformer.py` | `process_ohlcv_data` | `test_transformer.py` |
| `processing/factor_ranking.py` | `rank_stocks_by_factor`, `calculate_weighted_rank` | `test_factor_ranking.py` |
| `utils/cli.py` | `parse_args`, `resolve_params`, `load_config` | `test_cli.py` |
| `utils/google_cloud_bigquery.py` | `load_to_bigquery` | `test_google_cloud_bigquery.py` |
| `utils/google_cloud_platform.py` | `check_gcp_environment` | `test_google_cloud_platform.py` |
| `utils/google_cloud_storage.py` | `upload_file`, `download_file` | `test_google_cloud_storage.py` |
| `utils/retry.py` | `run_with_retry` | `test_retry.py` |
| `scripts/run_etl_pipeline.py` | `main`（整合流程） | `test_run_etl_pipeline_cli.py` |

> `utils/logger.py`（`configure_logger` 等）多為設定用，通常不另寫單元測試。

#### 2. 如何更新一批新的資料到 BigQuery？

產生可重現回測用的區間資料：

```bash
python -m scripts.run_etl_pipeline \
  --market-value-date 2024-01-15 \
  --start 2020-01-01 \
  --end 2024-01-01 \
  --top-n 50
```

- 會以 2024-01-15 的市值排名產生固定 universe，寫入  
  `{dataset}_interval.dim_universe_mv2024-01-15_top50`
- 同一批股票在 2020-01-01 ~ 2024-01-01 的 OHLCV + `daily_return`，寫入  
  `{dataset}_interval.fact_price_mv20240115_s20200101_e20240101_top50`

> **建議**：因子分析 / 回測時，使用 `fact_price_*` + `dim_universe_*`，可減少生存者偏誤並確保結果可重現。

---

### CI Pipeline

本專案使用 GitHub Actions 執行 CI：
- 結果頁面：https://github.com/kimie-log/finance-data-pipeline/actions/workflows/ci.yml
- 設定檔：`.github/workflows/ci.yml`
- 觸發條件：`main` 分支 push 與 PR
- 內容：安裝依賴後執行 `python -m pytest -q`
- 支援版本：Python 3.10 / 3.11 / 3.12 / 3.13 / 3.14

---

### 資料結果是否會變動？

是的，結果可能會隨時間變動，原因包含：
- **市值排名會變動**：FinLab 取最新市值，Top N 可能每日不同  
- **資料時間區間會更新**：若 `yfinance.end` 為 `null`，會以當天日期為結束日

若要固定結果，請使用 `--market-value-date` 與 `--start` / `--end` 固定市值日與區間，並在設定檔中固定 `end` 日期

---

### 開發與除錯建議

- 若在 GCS / BigQuery 權限相關步驟遇到 `invalid_grant` 或驗證失敗，可在終端機執行：

  ```bash
  gcloud auth application-default login
  ```

- 可先在互動式環境 (例如 Jupyter / VSCode Notebook) 單獨測試：
  - `FinLabFetcher.fetch_top_stocks_universe`
  - `YFinanceFetcher.fetch_daily_ohlcv_data`、`YFinanceFetcher.fetch_benchmark_daily`
  - `Transformer.process_ohlcv_data`
  - `load_to_bigquery`

---
