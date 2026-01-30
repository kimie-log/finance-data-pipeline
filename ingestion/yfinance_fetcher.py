import yfinance as yf
import pandas as pd
from typing import Annotated, List

'''
封裝 yfinance 抓取行為，集中處理欄位清洗與格式統一
'''


class YFinanceFetcher:
    @staticmethod
    def fetch_daily_ohlcv_data(
        stock_symbols: Annotated[List[str], "股票代碼列表"],
        start_date: Annotated[str, "起始日期", "YYYY-MM-DD"],
        end_date: Annotated[str, "結束日期", "YYYY-MM-DD"],
        is_tw_stock: Annotated[bool, "stock_symbols 是否是台灣股票"] = True,
    ) -> Annotated[
        pd.DataFrame,
        "價量資料表 (long format)",
        "欄位包含 datetime, asset, open, high, low, close, volume",
    ]:
        '''
        函式說明：
        取得指定股票在給定日期範圍內的每日 OHLCV 價量資料（long format）
        '''
        # 若為台股，自動補上 .TW 後綴，與 close-only 版本一致
        tickers = stock_symbols
        if is_tw_stock:
            tickers = [
                f"{symbol}.TW" if ".TW" not in symbol else symbol
                for symbol in stock_symbols
            ]

        all_stock_data = pd.concat(
            [
                pd.DataFrame(
                    yf.download(symbol, start=start_date, end=end_date, auto_adjust=True)
                )
                # yfinance 對單一 ticker 會用 MultiIndex columns (欄位, Ticker)，先移除 Ticker 層
                .droplevel("Ticker", axis=1)
                # 加上 asset 欄位（不含 .TW 後綴）
                .assign(asset=symbol.split(".")[0])
                .reset_index()
                .rename(columns={"Date": "datetime"})
                .ffill()
                for symbol in tickers
            ],
            ignore_index=True,
        )

        # 保留並重新命名欄位，統一為小寫英文字以利 BigQuery 與後續處理
        all_stock_data = all_stock_data[
            ["Open", "High", "Low", "Close", "Volume", "datetime", "asset"]
        ]
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
        index_ids: Annotated[List[str], "指數代碼列表，如 ^TWII"],
        start_date: Annotated[str, "起始日期", "YYYY-MM-DD"],
        end_date: Annotated[str, "結束日期", "YYYY-MM-DD"],
    ) -> Annotated[
        pd.DataFrame,
        "欄位: date, index_id, close, daily_return",
    ]:
        """抓取基準指數日收盤與日報酬，供回測層使用。"""
        out = []
        for idx_id in index_ids:
            raw = yf.download(idx_id, start=start_date, end=end_date, auto_adjust=True)
            if raw.empty:
                continue
            raw = raw.reset_index().rename(columns={"Date": "date"})
            raw["index_id"] = idx_id.lstrip("^")
            # yf.download 單一 ticker 時可能回傳 MultiIndex 欄位，raw["Close"] 變 2-d，pd.to_numeric 須收 1-d
            close_col = raw["Close"]
            close_1d = close_col.squeeze() if isinstance(close_col, pd.DataFrame) else close_col
            raw["close"] = pd.to_numeric(close_1d, errors="coerce")
            raw["daily_return"] = raw["close"].pct_change()
            out.append(raw[["date", "index_id", "close", "daily_return"]])
        if not out:
            return pd.DataFrame(columns=["date", "index_id", "close", "daily_return"])
        return pd.concat(out, ignore_index=True)

