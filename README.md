# Finance Data Pipeline

本專案目標為製作台股價量與因子資料的 ETL pipeline，支援**單因子、多因子分析**與**多因子回測**，目的為量化投資前期的分析與回測做更加簡易的方式進行

---

以下分成四項功能進行介紹

1. ETL pipeline
2. 單因子分析
3. 多因子分析
4. 多因子回測

建議順序：
**ETL → 單因子分析（根據分析選擇因子） → 多因子分析（驗證因子）→ 多因子回測（驗證策略）**

## ETL pipeline

`scripts/run_etl_pipeline.py`  
分為 Extract、Transform、Load 三個部分說明

### 擷取(Extract):

-   使用 `finlab` API 取得因子資料，篩選如下：

    -   排除特定產業
    -   篩選市場別為上市的股票
    -   篩選指定日期前的上市股
    -   若給定市值基準日，則取該日或之前最近一筆有資料的日期
    -   合併市值資料與公司資料
    -   按市值排序並取前 top_n 筆

-   使用 `yfinance` API 取得價量資料
    -   給定指定開始、結束日期
    -   載入 finlab 選定的 top_n 股票資料
    -   空值用 `ffill()` 填補
    -   重命名欄位(統一為小寫英文)

### 轉換(Transform)

原 `yfinance` 的價量資料做轉換

-   複製輸入資料、統一欄位名稱：
    -   欄位 datetime → date
    -   欄位 asset → stock_id
-   欄位型別正規化：
    -   date 轉成 datetime 型別
    -   stock_id 全部轉成字串
    -   欄位 open, high, low, close, volume 強制轉為數字，無法轉換的則轉換為 NaN
-   排序:
    -   依 stock_id, date 排序(確保每檔股票的時間序列有正確先後順序)
-   缺值處理:
    -   欄位 open, high, low, close:以 同一檔股票的時間序列為單位，用 `ffill()` 向前填補
    -   將 volume 缺值填成 0
-   移除無效列：
    -   刪掉 close 為 NaN 的列：確保每列至少有收盤價，才能算日報酬
-   計算日報酬 daily_return：
    -   對每檔股票的 close 做 `pct_change()`:得到每日相對前一日的百分比變化
-   交易可行性標記（啟發式）:
    -   先初始化：is_suspended（是否停牌）、is_limit_up（是否漲停）、is_limit_down（是否跌停）
    -   建立條件 mask_same：volume == 0 且 open == close 且 high == low （代表當天無量且 OHLC 全部同價，可能是停牌或漲跌停）
    -   若 mask_same 有任何 True：先計算「前一日收盤價」、再依條件標記
-   欄位重組與輸出：按固定順序取出欄位，組成 final_df 並回傳作為寫入 BigQuery fact_price 與後續回測的輸入

### 載入(Load)

-   local data:
    -   raw :原始下載資料
    -   processed : 清洗 OHLCV 後的價量資料
-   將所有本地的 raw and processed 資料上傳至指定的 google cloud storage and BigQuery
    -   Dataset 命名：{base}\_s{YYYYMMDD}\_e{YYYYMMDD}\_mv{YYYYMMDD}，例：tw_top_50_stock_data_s20170516_e20210515_mv20170516
    -   fact_price：價量事實表，upsert 避免重複列
    -   dim_universe：該市值日的 Top N 股票清單（含 delist_date 等)
    -   dim_calendar：交易日曆，由價量日期產生，供回測對齊交易日
    -   fact_benchmark_daily：基準指數（預設 ^TWII）日收盤與日報酬
    -   fact_factor（可選）：財報因子日頻資料

## 單因子分析

`script/run_single_factor_analysis`

-   使用 ETL 產生的價量（fact_price）與因子（fact_factor 或本地 parquet）
-   針對一個或多個單因子，逐一跑 Alphalens（Quantiles、IC、tear sheet 等）
-   輸出至 `data/single_factor_analysis_reports` 包含分析另存 pdf, png, txt

-   分析建議：可針對各個因子的產出查看最佳投組與最差投組差異大的因子，選取合適的因子組成因子組合，進行多因子分析

## 多因子分析

### 多因子分析的目的

**驗證一組因子（或因子組合）是否對未來報酬有預測力**。做法是：先把多個因子合併成一個「綜合分數」或「主成分」，再交給 Alphalens 計算分位數報酬、IC、tear sheet，用來判斷這組因子是否值得拿去做回測與實盤。

本專案提供兩種**決策方式**來產生「綜合分數」：

| 模式                         | 說明                                           | 決策邏輯                                                                                     |
| ---------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **加權排名 (weighted_rank)** | 多個因子各自對股票排名，再依權重加總後重新排名 | 每日／每期：各因子先排名 → 加權加總 → 再排名 → 得到每檔股票的綜合排名，排名高者視為「看好」  |
| **主成分分析 (pca)**         | 多個因子合併後做 PCA，取前幾條主成分當作新因子 | 用少數主成分（如 PC2、PC4）代表多因子資訊，再對主成分做 Alphalens 分析，看哪條主成分有預測力 |

兩者都是「先從多因子得到一個分數／主成分，再評估該分數與未來報酬的關係」，差別在於分數的計算方式：加權排名是**排名加權**，PCA 是**線性組合降維**

#### 加權排名模式 (weighted_rank) 的決策流程

1. **選因子**：在 config 或 CLI 指定 `--factors "A,B,C,..."`（或從 factors_list.json 載入）。
2. **每個因子先排名**：對每個交易日，依因子值對股票做排名；`positive_corr=true` 表示因子值愈大排名愈前（愈看好）。
3. **組合與權重**：可指定 `combo_size`（例如 5 表示「五因子組合」），系統會列舉所有 N 取 K 的組合；每個組合內各因子依 `weights` 加權（未指定則等權）。
4. **加權排名**：同一組合內，各因子排名 × 權重加總，再對加總值重新排名，得到「加權綜合排名」。
5. **Alphalens 評估**：把加權綜合排名當成「因子」，交給 Alphalens 計算分位數報酬、IC、tear sheet；產出 PDF/PNG 至 `data/multi_factor_analysis_reports/`。
6. **決策解讀**：看 IC、分位數報酬、tear sheet 判斷該因子組合是否有預測力；若有，再進入回測驗證。

#### PCA 模式的決策流程

1. **選因子**：同上，指定多個因子。
2. **合併與標準化**：多因子合併為 (日期, 股票) × 因子值矩陣，缺值處理後做標準化。
3. **PCA**：對標準化矩陣做 PCA，取得主成分（PC1, PC2, …）；主成分數量由 `n_components` 或預設「因子數 − 1」決定。
4. **選主成分**：用 `pcs` 指定要分析的主成分編號（如 "2,4" 表示只對 PC2、PC4 跑 Alphalens）。
5. **Alphalens 評估**：對每條選定的主成分跑 Alphalens，產出報表。
6. **決策解讀**：看哪條主成分具預測力，可作為後續回測或選股的依據。

### 因子要如何選擇

多因子分析前，需先決定「用哪幾個因子」。本專案因子來自 **FinLab**（財報／基本面），可從以下方向著手：

| 方向               | 說明                                                                                                                                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **看得到什麼因子** | 執行 `python -m factors.list_factors` 可列出 FinLab 提供的因子；清單會寫入 `factors/factors_list.json` 的 `fundamental_features`，可直接從中挑選名稱。 |
| **經濟意義**       | 挑有邏輯、可解讀的因子，例如：營運現金流、歸屬母公司淨利、ROE、營收／獲利成長率等，避免純數據挖礦。                                                    |
| **分散與相關性**   | 多因子組合宜涵蓋不同面向（獲利能力、成長性、品質、槓桿等），因子彼此不要高度重複；若多個因子高度相關，加權排名時資訊重疊，效果有限。                   |
| **先少後多**       | 先選 3 ～ 5 個因子跑加權排名分析，看 IC、分位數報酬；有預測力再擴充或微調權重，避免一次塞太多因子難以解讀。                                            |
| **用分析結果篩選** | 跑完 Alphalens 後，看哪個因子組合（或 PCA 的哪條主成分）IC 高、分位數報酬單調、tear sheet 合理，再以該組合進入回測。                                   |

**實務流程建議**：

1. 執行 `python -m factors.list_factors` 更新並查看 `factors/factors_list.json`。
2. 從中挑 3 ～ 5 個因子（例如：營運現金流、歸屬母公司淨利、ROE 稅後、營業利益成長率、稅後淨利成長率）。
3. 執行多因子分析（加權排名或 PCA），看報表。
4. 依 IC、分位數報酬調整因子清單或權重，再跑一次；通過後再用同一組因子跑多因子回測驗證。

因子名稱須與 **ETL 寫入 fact_factor 的欄位名** 或 **FinLab API 回傳的名稱** 一致（可參考 `factors_list.json` 內寫法）。

---

## 3. 多因子回測的決策邏輯

回測腳本 `run_multi_factor_backtest` 實作的是：**依多因子加權排名，定期（按季度）更新持股組合，做多排名前段、做空排名後段**。

### 3.1 換倉、加權排名與交易決策

-   **換倉頻率**：以**季度**為單位。系統會列出區間內所有季度（如 2017-Q2、2017-Q3…），每個季度取該季內的因子資料。
-   **每季決策步驟**：
    1. 先將各因子轉為日頻 `datetime, asset, value`，對缺值做前向填補（ffill），形成連續序列。
    2. 在該季度的日期區間內，對每個交易日、每個因子分別做橫截面排名（`FactorRanking.rank_stocks_by_factor`），得到單因子 rank。
    3. 依該季度指定的權重（或預設等權）使用 `FactorRanking.calculate_weighted_rank` 做**加權排名**，得到每檔股票在該季中的綜合排名（weighted_rank）。
    4. 將各季的加權排名結果串接起來（跨季），再與清洗後的價量資料合併，作為 Backtrader 的輸入。
-   **交易決策（Backtrader 策略）**：策略 `FactorRankStrategy` 會在**每天**依當日 rank 做決策：
    -   **做多**：排名最高的 `buy_n` 檔（預設 20）。
    -   **做空**：排名最低的 `sell_n` 檔（預設 20）。
    -   每檔分配資金由 `each_cash` 計算（初始資金 × 0.9 / (buy_n + sell_n)），保留約 10% 現金。
-   **rank 缺值處理**：合併後若某天／某檔股票沒有排名，rank 會被設為 999999，策略視為「無信號」，不會被選入做多／做空。

因此，「決策流程」可以理解為：**每季用多因子加權排名產生當季的綜合 rank，將 rank 透過 ffill 套用到每個交易日，策略則在每日依 rank 做多前 N、做空後 N**；回測結果由 PyFolio 產出完整 tear sheet。

### 3.2 與多因子分析（Alphalens）的對應關係

-   **分析階段（Alphalens）**：先用 `run_multi_factor_analysis` 驗證「這組因子（或主成分）是否有預測力」，主要看分位數報酬與 IC。
-   **回測階段（Backtrader + PyFolio）**：再用同一套「多因子加權排名」邏輯（相同因子、權重、`positive_corr` 等），在歷史價量上做多／做空，檢驗實際報酬、回撤與風險指標。

---

### 快速開始

1. **環境設定**
   - 在專案根目錄新增 `.env` 檔案（複製 `.env.example` 內容並填入）：
     - `FINLAB_API_TOKEN`：FinLab API Token
     - `GCP_PROJECT_ID`：GCP 專案 ID（若使用 BigQuery）
     - `GCS_BUCKET`：GCS Bucket 名稱（若要上傳報表）
   - 在專案根目錄新增 `gcp_keys/` 目錄，並將 GCP 服務帳戶 JSON 金鑰放入（需開啟 BigQuery 和 Storage 管理權限）

2. **安裝依賴**
   ```bash
   pip install -r requirements.txt
   ```

3. **ETL Pipeline：取得價量與因子資料**
   ```bash
   python -m scripts.run_etl_pipeline \
     --market-value-date 2017-05-16 \
     --start 2017-05-16 \
     --end 2021-05-15 \
     --top-n 50 \
     --with-factors
   ```
   - 主要參數：
     - `--market-value-date`：市值基準日（YYYY-MM-DD）
     - `--start` / `--end`：分析區間起始／結束日期
     - `--top-n`：市值前 N 大股票（預設 50）
     - `--with-factors`：是否同時抓取因子資料
   - 輸出：BigQuery Dataset `{base}_s{YYYYMMDD}_e{YYYYMMDD}_mv{YYYYMMDD}` 與本地 `data/raw/`、`data/processed/` Parquet

4. **單因子分析：逐一檢視各因子表現**
   ```bash
   python -m scripts.run_single_factor_analysis \
     --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
     --start 2017-05-16 \
     --end 2021-05-15 \
     --factors "營運現金流,歸屬母公司淨利" \
     --auto-find-local
   ```
   - 主要參數：
     - `--dataset`：BigQuery Dataset ID（與 ETL 輸出一致）
     - `--start` / `--end`：分析區間
     - `--factors`：因子名稱，逗號分隔
     - `--auto-find-local`：自動在 `data/processed` 尋找最新 Parquet（優先使用本地資料）
     - `--quantiles`：分位數數量（預設 5）
     - `--periods`：前瞻期間，逗號分隔（預設 "1,5,10"）
   - 輸出：`data/single_factor_analysis_reports/`（PDF / PNG / summary）

5. **多因子分析：驗證因子組合預測力**
   
   **加權排名模式**：
   ```bash
   python -m scripts.run_multi_factor_analysis \
     --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
     --start 2017-05-16 \
     --end 2021-05-15 \
     --mode weighted_rank \
     --factors "營運現金流,歸屬母公司淨利,ROE稅後,營業利益成長率,稅後淨利成長率" \
     --combo-size 5 \
     --auto-find-local
   ```
   
   **PCA 模式**：
   ```bash
   python -m scripts.run_multi_factor_analysis \
     --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
     --mode pca \
     --factors "營業利益,營運現金流,ROE稅後,營業利益成長率,稅後淨利成長率" \
     --pcs 2,4 \
     --start 2017-05-16 \
     --end 2021-05-15 \
     --auto-find-local
   ```
   - 主要參數：
     - `--mode`：`weighted_rank`（加權排名）或 `pca`（主成分分析）
     - `--factors`：因子名稱，逗號分隔
     - `--combo-size`：加權排名模式中每個組合的因子數（預設 5）
     - `--pcs`：PCA 模式中要分析的主成分編號，逗號分隔（預設 "2,4"）
     - `--weights`：加權排名權重，逗號分隔（未指定則等權）
   - 輸出：`data/multi_factor_analysis_reports/`（每個組合或主成分一份報表）

6. **多因子回測：驗證策略歷史表現**
   ```bash
   python -m scripts.run_multi_factor_backtest \
     --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
     --factors "營運現金流,歸屬母公司淨利,稅後淨利成長率" \
     --start 2017-05-16 \
     --end 2021-05-15 \
     --buy-n 20 \
     --sell-n 20 \
     --initial-cash 20000000 \
     --auto-find-local
   ```
   - 主要參數：
     - `--factors`：因子名稱，逗號分隔
     - `--weights`：各因子權重，逗號分隔（未指定則等權）
     - `--buy-n` / `--sell-n`：做多／做空檔數（預設各 20）
     - `--initial-cash`：初始資金（預設 20,000,000）
     - `--commission`：手續費率（預設 0.001）
     - `--positive-corr`：因子與收益正相關（預設 True）
   - 輸出：`data/multi_factor_backtest_reports/`（PyFolio tear sheet：PDF / PNG）

**提示**：
- 所有腳本支援從 `config/settings.yaml` 讀取預設參數，CLI 參數會覆寫設定檔
- 使用 `--auto-find-local` 可優先使用本地 Parquet，加快執行速度
- 報告會自動上傳至 GCS（需設定 `GCS_BUCKET`），加 `--skip-gcs` 可略過上傳
