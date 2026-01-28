import os
import pandas as pd
from finlab import data, login
import finlab
import pandas as pd
from typing import List, Annotated



class FinLabFetcher:
    @staticmethod
    def finlab_login():
        token = os.getenv("FINLAB_API_TOKEN")
        finlab.login(token)


    @staticmethod
    def fetch_top_stocks_by_market_value(
        excluded_industry: Annotated[List[str], "需要排除的特定產業類別列表"] = [],
        pre_list_date: Annotated[str, "上市日期須早於此指定日期"] | None = None,
        top_n: Annotated[int, "市值前 N 大的公司"] | None = None,
    ) -> Annotated[List[str], "符合條件的公司代碼列表"]:
        '''
        函式說明：
        取得市值前 N 大的公司代碼列表，並根據指定條件進行過濾
        '''
        # 取得公司基本資訊
        company_info = data.get("company_basic_info")[
            ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
        ]
        
        # 過濾產業
        if excluded_industry:
            company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]
        
        # 過濾上市日期 & 市場別 (僅上市公司)
        if pre_list_date:
            company_info = company_info[company_info["市場別"] == "sii"]
            company_info = company_info[company_info["上市日期"] < pre_list_date]

        # 取得市值並過濾 Top N
        if top_n:
            df_market_value = data.get("etl:market_value")
            latest_market_value = df_market_value.iloc[-1].rename("market_value").reset_index()
            latest_market_value.columns = ["stock_id", "market_value"]
            
            company_info = pd.merge(latest_market_value, company_info, on="stock_id")
            company_info = company_info.sort_values(by="market_value", ascending=False).head(top_n)

            return company_info.head(top_n)["stock_id"].tolist()
        else:
            return company_info["stock_id"].tolist()
    