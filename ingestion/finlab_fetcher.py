import os
from typing import Annotated, List
import pandas as pd
from finlab import data, login
import yfinance as yf



class FinLabFetcher:
    @staticmethod
    def finlab_login():
        # 已透過 .env 設定 TOKEN
        login(os.getenv("FINLAB_API_TOKEN"))

    @staticmethod
    def fetch_demo_financials():
        df = data.get("financial_statement:現金及約當現金").deadline()
        return df


    @staticmethod
    def get_daily_close_prices_data(
        stock_symbols: Annotated[List[str], "股票代碼列表"],
        start_date: Annotated[str, "起始日期", "YYYY-MM-DD"],
        end_date: Annotated[str, "結束日期", "YYYY-MM-DD"],
        is_tw_stock: Annotated[bool, "stock_symbols 是否是台灣股票"] = True,
    ) -> Annotated[
        pd.DataFrame,
        "每日股票收盤價資料表",
        "索引是日期(DatetimeIndex格式)",
        "欄位名稱包含股票代碼",
    ]:
        # 如果是台灣股票，在每個代碼後加 .TW
        if is_tw_stock:
            stock_symbols = [
                f"{symbol}.TW" if ".TW" not in symbol else symbol
                for symbol in stock_symbols
            ]

        # 下載收盤價
        stock_data = yf.download(stock_symbols, start=start_date, end=end_date)["Close"]

        # 只有一支股票時轉成 DataFrame
        if len(stock_symbols) == 1:
            stock_data = pd.DataFrame(stock_data)
            stock_data.columns = stock_symbols

        # 向前填補缺失值
        stock_data = stock_data.ffill()

        # 去掉 .TW
        stock_data.columns = stock_data.columns.str.replace(".TW", "", regex=False)
        return stock_data
