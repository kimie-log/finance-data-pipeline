import yfinance as yf
import pandas as pd
from typing import Annotated, List

class YFinanceFetcher:
    @staticmethod
    def fetch(ticker, start, end):
        df = yf.download(ticker, start=start, end=end)
        return df

    @staticmethod
    def fetch_daily_close_prices(
        stock_symbols: Annotated[List[str], "股票代碼列表"],
        start_date: Annotated[str, "起始日期", "YYYY-MM-DD"],
        end_date: Annotated[str, "結束日期", "YYYY-MM-DD"] | None,
        is_tw_stock: Annotated[bool, "stock_symbols 是否是台灣股票"] = True,
    ) -> Annotated[
        pd.DataFrame,
        "每日股票收盤價資料表",
        "索引是日期(DatetimeIndex格式)",
        "欄位名稱為純股票代碼 (去除後綴)",
    ]:
        '''
        函式說明：
        取得多支股票在指定日期區間的每日收盤價資料
        '''
        
        print(f"Downloading yfinance data for {len(stock_symbols)} tickers...")

        # 如果是台灣股票，則在每個股票代碼後加上 ".TW"
        if is_tw_stock:
            stock_symbols = [
                f"{symbol}.TW" if ".TW" not in symbol else symbol
                for symbol in stock_symbols
            ]
            
        # 1. 下載資料 (不再修改 stock_symbols，完全信任傳入的列表)
        # auto_adjust=True 會自動處理除權息價格，通常對回測較方便，若需原始價格可設為 False
        stock_data = yf.download(stock_symbols, start=start_date, end=end_date, auto_adjust=True)

        # 2. 處理資料結構 (只取 Close)
        # 先處理單支股票 Series，避免直接存取 .columns 造成錯誤
        if isinstance(stock_data, pd.Series):
            stock_data = stock_data.to_frame()
            stock_data.columns = stock_symbols
        else:
            # yfinance 若下載多檔股票，columns 會是 MultiIndex (Price, Ticker)
            if isinstance(stock_data.columns, pd.MultiIndex):
                try:
                    # 優先取 Close，若 auto_adjust=True 有時會直接回傳修正後價格，視版本而定
                    # 這裡做個防呆，確保取到收盤價
                    target_col = "Close" if "Close" in stock_data.columns.levels[0] else stock_data.columns.levels[0][0]
                    stock_data = stock_data[target_col]
                except Exception as e:
                    print(f"Error extracting Close price: {e}")
                    return pd.DataFrame()
            elif "Close" in stock_data.columns:
                stock_data = stock_data["Close"]

            # 3. 處理單支股票的特殊情況 (Series -> DataFrame)
            if isinstance(stock_data, pd.Series):
                stock_data = stock_data.to_frame()
                stock_data.columns = stock_symbols

        # 4. 處理缺失值 (Forward Fill)
        stock_data = stock_data.ffill()

        # 5. 移除全空的欄位 (避免下載失敗的股票佔用欄位)
        stock_data = stock_data.dropna(axis=1, how='all')

        # 6. 清洗欄位名稱
        # 使用 Regex 同時移除結尾的 .TW (例: "2330.TW" -> "2330")
        stock_data.columns = stock_data.columns.str.replace(r'\.TW$', '', regex=True)

        return stock_data
        
        
