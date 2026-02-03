"""
yfinance 價量與基準指數抓取模組 

封裝 yfinance 下載行為，統一欄位命名（小寫英文）與 long format，
供後續 Transformer 與 BigQuery 使用。台股自動補 .TW 後綴；除權息以 auto_adjust=True 處理  
"""
from __future__ import annotations

from typing import Annotated, List

import pandas as pd
import yfinance as yf


class YFinanceFetcher:
    """
    yfinance 價量與基準指數抓取  

    所有方法為 staticmethod。產出欄位與型別對齊下游 Transformer.process_ohlcv_data
    與 BigQuery fact_price / fact_benchmark_daily 寫入需求  
    """

    @staticmethod
    def fetch_daily_ohlcv_data(
        stock_symbols: Annotated[List[str], "股票代碼列表，例：['2330','2317']"],
        start_date: Annotated[str, "區間起始日 YYYY-MM-DD"],
        end_date: Annotated[str, "區間結束日 YYYY-MM-DD"],
        is_tw_stock: Annotated[bool, "True 時自動為代碼補 .TW 後綴"] = True,
    ) -> pd.DataFrame:
        """
        取得指定股票在給定區間的每日 OHLCV 價量資料，long format  

        實務：台股需 is_tw_stock=True 以補 .TW；yfinance 單一 ticker 會回傳 MultiIndex 欄位，
        已於內部 droplevel 並統一為 datetime, asset, open, high, low, close, volume（小寫），
        以利 BigQuery 與 Transformer.process_ohlcv_data。除權息以 auto_adjust=True 處理 

        Args:
            stock_symbols: 股票代碼列表 
            start_date: 下載區間起始日 
            end_date: 下載區間結束日 
            is_tw_stock: 是否為台股；True 時自動補 .TW 

        Returns:
            DataFrame 欄位：datetime, asset, open, high, low, close, volume（小寫） 
        """
        tickers = stock_symbols
        if is_tw_stock:
            tickers = [
                f"{symbol}.TW" if ".TW" not in symbol else symbol
                for symbol in stock_symbols
            ]

        # 抓取價量資料
        all_stock_data = pd.concat(
            [
                pd.DataFrame(
                    yf.download(symbol, start=start_date, end=end_date, auto_adjust=True)
                )
                .droplevel("Ticker", axis=1)
                .assign(asset=symbol.split(".")[0])
                .reset_index()
                .rename(columns={"Date": "datetime"})
                .ffill()
                for symbol in tickers
            ],
            ignore_index=True,
        )

        # 選取需要的欄位
        all_stock_data = all_stock_data[
            ["Open", "High", "Low", "Close", "Volume", "datetime", "asset"]
        ]

        # 重命名欄位(統一為小寫英文)
        all_stock_data.columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "datetime",
            "asset",
        ]

        return all_stock_data.reset_index(drop=True)

    @staticmethod
    def fetch_benchmark_daily(
        index_ids: Annotated[List[str], "指數代碼列表，例：['^TWII','^TWOII']"],
        start_date: Annotated[str, "區間起始日 YYYY-MM-DD"],
        end_date: Annotated[str, "區間結束日 YYYY-MM-DD"],
    ) -> pd.DataFrame:
        """
        抓取基準指數日收盤與日報酬，供回測層 fact_benchmark_daily 寫入 

        實務：index_id 會去掉前綴 ^ 寫入 index_id 欄；單一 ticker 時 yfinance 可能回傳
        MultiIndex 欄位，已以 squeeze 轉為 1-d 再寫入 close / daily_return 

        Args:
            index_ids: 指數代碼，如 ^TWII 
            start_date: 下載區間起始日 
            end_date: 下載區間結束日 

        Returns:
            欄位 date, index_id, close, daily_return；無資料時回傳空 DataFrame（含上述欄位） 
        """
        out = []
        for idx_id in index_ids:
            raw = yf.download(idx_id, start=start_date, end=end_date, auto_adjust=True)
            if raw.empty:
                continue
            raw = raw.reset_index().rename(columns={"Date": "date"})
            raw["index_id"] = idx_id.lstrip("^")
            close_col = raw["Close"]
            close_1d = close_col.squeeze() if isinstance(close_col, pd.DataFrame) else close_col
            raw["close"] = pd.to_numeric(close_1d, errors="coerce")
            raw["daily_return"] = raw["close"].pct_change()
            out.append(raw[["date", "index_id", "close", "daily_return"]])
        if not out:
            return pd.DataFrame(columns=["date", "index_id", "close", "daily_return"])
        return pd.concat(out, ignore_index=True)
