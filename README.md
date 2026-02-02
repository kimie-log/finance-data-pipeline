# Finance Data Pipeline

台股價量與因子資料的 ETL pipeline，支援**多因子分析**與**多因子回測**。本說明以「多因子分析如何進行」與「決策如何形成」為重點。

---

## 1. 多因子分析在做什麼

多因子分析的目的，是**驗證一組因子（或因子組合）是否對未來報酬有預測力**。做法是：先把多個因子合併成一個「綜合分數」或「主成分」，再交給 Alphalens 計算分位數報酬、IC、tear sheet，用來判斷這組因子是否值得拿去做回測與實盤。

本專案提供兩種**決策方式**來產生「綜合分數」：

| 模式                         | 說明                                           | 決策邏輯                                                                                     |
| ---------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **加權排名 (weighted_rank)** | 多個因子各自對股票排名，再依權重加總後重新排名 | 每日／每期：各因子先排名 → 加權加總 → 再排名 → 得到每檔股票的綜合排名，排名高者視為「看好」  |
| **主成分分析 (pca)**         | 多個因子合併後做 PCA，取前幾條主成分當作新因子 | 用少數主成分（如 PC2、PC4）代表多因子資訊，再對主成分做 Alphalens 分析，看哪條主成分有預測力 |

兩者都是「先從多因子得到一個分數／主成分，再評估該分數與未來報酬的關係」，差別在於分數的計算方式：加權排名是**排名加權**，PCA 是**線性組合降維**。

---

## 2. 多因子分析流程與決策步驟

### 2.1 資料從哪裡來

-   **價量**：BigQuery（ETL 寫入的 fact_price）或本地 Parquet、或由 ETL 產出後用 `--auto-find-local` 自動尋找。
-   **因子**：BigQuery（fact_factor）、本地 Parquet，或 `--from-finlab-api` 從 FinLab API 即時抓取。

### 2.2 加權排名模式 (weighted_rank) 的決策流程

1. **選因子**：在 config 或 CLI 指定 `--factors "A,B,C,..."`（或從 factors_list.json 載入）。
2. **每個因子先排名**：對每個交易日，依因子值對股票做排名；`positive_corr=true` 表示因子值愈大排名愈前（愈看好）。
3. **組合與權重**：可指定 `combo_size`（例如 5 表示「五因子組合」），系統會列舉所有 N 取 K 的組合；每個組合內各因子依 `weights` 加權（未指定則等權）。
4. **加權排名**：同一組合內，各因子排名 × 權重加總，再對加總值重新排名，得到「加權綜合排名」。
5. **Alphalens 評估**：把加權綜合排名當成「因子」，交給 Alphalens 計算分位數報酬、IC、tear sheet；產出 PDF/PNG 至 `data/multi_factor_analysis_reports/`。
6. **決策解讀**：看 IC、分位數報酬、tear sheet 判斷該因子組合是否有預測力；若有，再進入回測驗證。

### 2.3 PCA 模式的決策流程

1. **選因子**：同上，指定多個因子。
2. **合併與標準化**：多因子合併為 (日期, 股票) × 因子值矩陣，缺值處理後做標準化。
3. **PCA**：對標準化矩陣做 PCA，取得主成分（PC1, PC2, …）；主成分數量由 `n_components` 或預設「因子數 − 1」決定。
4. **選主成分**：用 `pcs` 指定要分析的主成分編號（如 "2,4" 表示只對 PC2、PC4 跑 Alphalens）。
5. **Alphalens 評估**：對每條選定的主成分跑 Alphalens，產出報表。
6. **決策解讀**：看哪條主成分具預測力，可作為後續回測或選股的依據。

### 2.4 因子要如何選擇

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

回測腳本 `run_multi_factor_backtest` 實作的是：**依多因子加權排名，定期換倉、做多排名前段、做空排名後段**。

### 3.1 換倉與排名如何決定

-   **換倉頻率**：以**季度**為單位。系統會列出區間內所有季度（如 2017-Q2、2017-Q3…），每個季度取該季內的因子資料。
-   **每季決策步驟**：
    1. 各因子在該季內對股票做排名（`FactorRanking.rank_stocks_by_factor`）。
    2. 依權重做加權排名（`FactorRanking.calculate_weighted_rank`），得到每檔股票在該季的綜合排名。
    3. 排名結果與價量合併，供 Backtrader 使用。
-   **交易決策**：策略 `FactorRankStrategy` 依**當日**排名：
    -   **做多**：排名最高的 `buy_n` 檔（預設 20）。
    -   **做空**：排名最低的 `sell_n` 檔（預設 20）。
    -   每檔分配資金由 `each_cash` 計算（初始資金 × 0.9 / (buy_n + sell_n)）。

因此，「決策」就是：**每季用多因子加權排名更新綜合排名，每日依該排名做多前 N、做空後 N**；回測結果由 PyFolio 產出 tear sheet。

### 3.2 與多因子分析的對應關係

-   **分析階段**：用 Alphalens 驗證「這組因子（或主成分）是否有預測力」。
-   **回測階段**：用同一套「多因子加權排名」邏輯，在歷史價量上做多／做空，驗證實際報酬與風險。

建議順序：**ETL → 多因子分析（驗證因子）→ 多因子回測（驗證策略）**。

---

## 4. 整體使用流程

```
環境設定 (.env、gcp_keys、pip install)
         │
         ▼
ETL (run_etl_pipeline) → 價量 + 因子寫入 BigQuery / 本地 Parquet
         │
         ├──────────────────┬──────────────────┐
         ▼                  ▼                  ▼
  多因子分析            多因子回測
  run_multi_factor_     run_multi_factor_
  analysis              backtest
  (weighted_rank/PCA)   (季度加權排名 + 多/空)
  → Alphalens 報表      → PyFolio tear sheet
```

-   **ETL**：必做，產出分析與回測所需的價量與因子資料。
-   **多因子分析**：選做，用來篩選與驗證因子或主成分。
-   **多因子回測**：選做，用來驗證「多因子加權排名 + 做多／做空」的歷史表現。

---

## 5. 快速開始

### 環境與 ETL

```bash
pip install -r requirements.txt
# .env：FINLAB_API_TOKEN, GCP_PROJECT_ID, GCS_BUCKET
# gcp_keys/：Service Account JSON

python -m scripts.run_etl_pipeline \
  --market-value-date 2017-05-16 --start 2017-05-16 --end 2021-05-15 \
  --top-n 50 --with-factors
```

### 多因子分析（加權排名）

```bash
python -m scripts.run_multi_factor_analysis \
  --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
  --start 2017-05-16 --end 2021-05-15 \
  --market-value-date 2017-05-16 \
  --mode weighted_rank --factors "營運現金流,歸屬母公司淨利,ROE稅後,營業利益成長率,稅後淨利成長率" \
  --auto-find-local
```

### 多因子分析（PCA）

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

### 單因子分析

```bash
python -m scripts.run_single_factor_analysis \
  --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
  --start 2017-05-16 --end 2021-05-15 \
  --factors "營運現金流,歸屬母公司淨利"
```

- **用途**：對給定的每一個單因子，分別跑 Alphalens（分位數報酬、IC、tear sheet）。
- **設定來源**：優先讀 `config.single_factor_analysis` 區塊；若未設定，部分欄位會沿用 `multi_factor_analysis`，`factors` 在皆未指定時會從 `etl.factors.factors_list` 指向的 `factors/factors_list.json` 載入 `fundamental_features`。
- **報告輸出**：本地 `data/single_factor_analysis_reports/...`，所有因子分析完成後，會統一將本次產生的報告批次上傳至 GCS（前提是 `.env` 設有 `GCS_BUCKET` 且未加 `--skip-gcs`）。

### 多因子回測

**統一因子設定**（所有季度使用相同因子與權重）：
```bash
python -m scripts.run_multi_factor_backtest \
  --dataset tw_top_50_stock_data_s20170516_e20210515_mv20170516 \
  --factors "營運現金流,歸屬母公司淨利,稅後淨利成長率" \
  --start 2017-05-16 --end 2021-05-15
```

**季度因子設定**（每季可指定不同因子組合與權重）：
在 `config/settings.yaml` 的 `multi_factor_backtest.quarterly_factors` 設定：
```yaml
multi_factor_backtest:
  quarterly_factors:
    "2017-Q1":
      - name: "營運現金流"
        weight: 0.2
      - name: "歸屬母公司淨利"
        weight: 0.2
      - name: "營業利益成長率"
        weight: 0.2
      - name: "稅前淨利成長率"
        weight: 0.2
      - name: "稅後淨利成長率"
        weight: 0.2
    "2017-Q2":
      - name: "營運現金流"
        weight: 0.3
      - name: "ROE稅後"
        weight: 0.7
  # factors 與 weights 作為預設值（未在 quarterly_factors 中指定的季度會使用此設定）
  factors: ["營運現金流", "歸屬母公司淨利", "稅後淨利成長率"]
```

執行時，系統會：
1. 對每個季度，若 `quarterly_factors` 中有該季度設定，則使用該季度的因子與權重
2. 若該季度未在 `quarterly_factors` 中，則使用 `factors` 與 `weights`（或等權）
3. 計算每季的加權排名後合併，進行回測

更多參數見 `config/settings.yaml` 註解；CLI 傳入會覆寫設定檔。

---

## 6. 技術與專案結構摘要

| 類別       | 技術／檔案                                                                    |
| ---------- | ----------------------------------------------------------------------------- |
| 資料       | FinLab、yfinance、BigQuery、GCS                                               |
| 多因子分析 | Alphalens（alphalens-reloaded）；PCA 用 scikit-learn                          |
| 回測       | Backtrader、PyFolio（pyfolio-reloaded）                                       |
| 排名邏輯   | `factors/factor_ranking.py`（rank_stocks_by_factor、calculate_weighted_rank） |
| 回測策略   | `strategies/factor_rank_strategy.py`（依 rank 做多 buy_n / 做空 sell_n）      |

```
scripts/    run_etl_pipeline.py, run_multi_factor_analysis.py, run_single_factor_analysis.py, run_multi_factor_backtest.py
factors/    factor_ranking.py, finlab_factor_fetcher.py, factors_list.json
strategies/ data_feed.py, factor_rank_strategy.py
config/     settings.yaml
```

測試：`python -m pytest -q`。  
若 Alphalens 圖表為空白，可於 VS Code 以 #%% 互動執行分析腳本內相關區塊。

---

## 7. 注意事項與實務對齊

以下為多因子分析與回測實務上需注意的邏輯與設定，建議執行前確認。

### 7.1 資料與路徑

| 項目                  | 說明                                                                                                                                                                                                  |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Dataset 命名**      | ETL 產出的 BigQuery dataset 為 `{base}_s{開始}_e{結束}_mv{市值日}`（例：`tw_top_50_stock_data_s20170516_e20210515_mv20170516`）。分析與回測的 `--dataset` 須與此一致。                                |
| **--auto-find-local** | 會在 `data/processed` 下找**最新**的 `fact_price*.parquet` 與 `fact_factor*.parquet`（依修改時間）。若目錄內有多組資料，可能用到非預期的檔案；必要時改用手動指定 `--local-price` / `--local-factor`。 |
| **本地因子格式**      | 本地因子 Parquet 須含 `date`、`stock_id`、`factor_name`、`value`（與 ETL 產出一致）；`load_factor_data` 會依 `factor_name` 篩選單一因子。                                                             |

### 7.2 財報因子與 Point-in-Time

-   財報因子為**季頻**，本專案以 **ffill（向前填補）** 展開為日頻。實務上財報有**揭露延遲**（例：Q1 財報於 5 月後才公布），若未使用「財報公布日」或 deadline 對齊，回測可能用到**當時尚未公布的數據**，造成前視偏差。
-   FinLab 的 `deadline()` 已對齊揭露時點；ETL 與分析／回測若皆用同一套 FinLab 日頻資料，可降低此風險。若自行產出因子，請確保依公布日做 point-in-time 對齊。

### 7.3 回測：做空與換倉

-   **做空**：策略對「排名最低」的 `sell_n` 檔呼叫 `sell()`。Backtrader 預設可開空倉，但須確認 broker 支援（例如設定足夠保證金／融券）。若僅要做多，可將 `sell_n` 設為 0。
-   **換倉頻率**：目前為**每季**計算加權排名，但合併價量後會以 **ffill** 將排名填到每個交易日；策略 `next()` **每日**依當日 rank 決定買賣。因因子為季頻 ffill，同一季內排名多數不變，效果接近「每季換倉、每日檢查」；若未來改為日頻因子，則會變成每日換倉。

### 7.4 多因子分析與回測一致性

-   分析階段用 **Alphalens** 驗證「加權排名或主成分」與未來報酬的關係；回測階段用**同一組因子與權重**做加權排名並做多／做空。建議兩邊的 `--factors`、`--weights`、`positive_corr` 一致，回測結果才對應到「分析通過的那組設定」。
-   分析有 **combo_size** 會列舉多個 N 取 K 組合；回測則用**單一組**因子（CLI 的 `--factors`），不會自動列舉組合，需手動選定要回測的因子清單。

### 7.5 其他

-   **手續費設定**：實際回測僅使用 `config/settings.yaml` 的 `multi_factor_backtest.commission`（如 0.001 表示 0.1% 手續費）。若需證交稅或更細的買賣稅費，可於策略或 broker 中自行擴充。
-   **缺漏排名**：策略中 `rank == 999999` 視為缺漏，該標的不會被選入做多／做空，避免將無排名資料納入交易。
