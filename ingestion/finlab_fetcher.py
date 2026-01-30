import os
import pandas as pd
from finlab import data, login
import finlab
import pandas as pd
from typing import List, Annotated

'''
封裝 FinLab 資料抓取與登入邏輯
'''


class FinLabFetcher:
    @staticmethod
    def finlab_login():
        # 支援 .env 裡的 FINLAB_API_TOKEN，自動帶入不跳出驗證碼輸入
        token = os.getenv("FINLAB_API_TOKEN")
        if not token:
            raise ValueError(
                "未設定 FinLab 驗證碼。請在 .env 中設定 FINLAB_API_TOKEN 或 （ https://ai.finlab.tw/api_token 取得）。"
            )
        # 執行一次登入，讓後續 data.get 可正常存取
        finlab.login(token)

    @staticmethod
    def fetch_top_stocks_universe(
        excluded_industry: Annotated[List[str], "需要排除的特定產業類別列表"] = [],
        pre_list_date: Annotated[str, "上市日期須早於此指定日期"] | None = None,
        top_n: Annotated[int, "市值前 N 大的公司"] | None = None,
        market_value_date: Annotated[str, "市值基準日期 (YYYY-MM-DD)"] | None = None,
    ) -> pd.DataFrame:
        '''
        函式說明：
        取得市值前 N 大之完整 universe 資料表，包含：
        - stock_id, company_name, list_date, industry, market
        - market_value, market_value_date, rank, top_n

        供 BigQuery 建立 universe 維度表使用；含 delist_date（下市日）若 FinLab 有提供。
        '''
        basic_cols = ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
        company_info = data.get("company_basic_info")
        if "下市日期" in company_info.columns:
            basic_cols = basic_cols + ["下市日期"]
        company_info = company_info[basic_cols]

        if excluded_industry:
            company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]

        if pre_list_date:
            company_info = company_info[company_info["市場別"] == "sii"]
            company_info = company_info[company_info["上市日期"] < pre_list_date]

        if not top_n:
            raise ValueError("top_n is required when building universe.")

        df_market_value = data.get("etl:market_value").copy()
        df_market_value.index = pd.to_datetime(df_market_value.index)

        if market_value_date:
            target_date = pd.to_datetime(market_value_date)
            candidate_dates = df_market_value.index[df_market_value.index <= target_date]
            if candidate_dates.empty:
                raise ValueError(f"No market value data before {market_value_date}")
            selected_date = candidate_dates.max()
        else:
            selected_date = df_market_value.index.max()

        latest_market_value = (
            df_market_value.loc[selected_date]
            .rename("market_value")
            .reset_index()
        )
        latest_market_value.columns = ["stock_id", "market_value"]

        # 合併市值與公司資訊，依市值排序
        universe = pd.merge(latest_market_value, company_info, on="stock_id")
        universe = universe.sort_values(by="market_value", ascending=False).head(top_n)

        # 新增衍生欄位與欄位命名標準化（避免 BigQuery 中混用中英文欄位名）
        universe = universe.reset_index(drop=True)
        universe["market_value_date"] = selected_date.strftime("%Y-%m-%d")
        universe["rank"] = universe.index + 1
        universe["top_n"] = top_n

        rename_map = {
            "公司名稱": "company_name",
            "上市日期": "list_date",
            "產業類別": "industry",
            "市場別": "market",
        }
        if "下市日期" in universe.columns:
            rename_map["下市日期"] = "delist_date"
        universe = universe.rename(columns=rename_map)
        if "delist_date" not in universe.columns:
            universe["delist_date"] = None  # 缺值用 None，避免 pd.NA 導致 load_table_from_dataframe 轉 PyArrow 時出錯
        else:
            universe["delist_date"] = universe["delist_date"].replace({pd.NA: None})
        # 日期欄位轉字串，避免 datetime/NaT 導致 load_table_from_dataframe 轉 PyArrow 時出錯
        for date_col in ["list_date", "delist_date"]:
            if date_col in universe.columns:
                universe[date_col] = universe[date_col].apply(
                    lambda x: None if pd.isna(x) or x is None else (x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
                )

        return universe
    