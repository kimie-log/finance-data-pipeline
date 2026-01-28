import pandas as pd
from typing import List, Annotated
from finlab import data

def get_top_stocks_by_market_value(
    excluded_industry: Annotated[List[str], "需要排除的特定產業類別列表"] = [],
    pre_list_date: Annotated[str, "上市日期須早於此指定日期"] | None = None,
    top_n: Annotated[int, "市值前 N 大的公司"] | None = None,
) -> Annotated[List[str], "符合條件的公司代碼列表"]:

    # 1. 取得公司基本資訊
    # 注意：必須包含 '市場別' 欄位，才能區分上市/上櫃
    company_info = data.get("company_basic_info")[
        ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
    ]
    
    # 2. 過濾產業
    if excluded_industry:
        company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]
    
    # 3. 過濾上市日期
    if pre_list_date:
        company_info = company_info[company_info["上市日期"] < pre_list_date]

    # 4. 取得市值並過濾 Top N
    if top_n:
        df_market_value = data.get("etl:market_value")
        latest_market_value = df_market_value.iloc[-1].rename("market_value").reset_index()
        latest_market_value.columns = ["stock_id", "market_value"]
        
        company_info = pd.merge(latest_market_value, company_info, on="stock_id")
        company_info = company_info.sort_values(by="market_value", ascending=False).head(top_n)

    # -------------------------------------------------------
    # 關鍵修正邏輯：處理 Yahoo Finance 後綴
    # 上市 (sii) -> .TW
    # 上櫃 (otc) -> .TWO
    # -------------------------------------------------------
    def append_suffix(row):
        stock_id = row["stock_id"]
        market = row["市場別"] # finlab 資料中，'sii'=上市, 'otc'=上櫃
        
        # 轉小寫比較比較保險
        if str(market).lower() == "otc":
            return f"{stock_id}.TWO"
        else:
            return f"{stock_id}.TW"

    tickers = company_info.apply(append_suffix, axis=1).tolist()
    
    return tickers