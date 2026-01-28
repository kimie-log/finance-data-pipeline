import yfinance as yf
import pandas as pd
from typing import Annotated, List

def get_daily_close_prices_data(
    stock_symbols: Annotated[List[str], "股票代碼列表 (需已包含 .TW 或 .TWO 後綴)"],
    start_date: Annotated[str, "起始日期", "YYYY-MM-DD"],
    end_date: Annotated[str, "結束日期", "YYYY-MM-DD"] | None,
) -> Annotated[
    pd.DataFrame,
    "每日股票收盤價資料表",
    "索引是日期(DatetimeIndex格式)",
    "欄位名稱為純股票代碼 (去除後綴)",
]:
    """
    函式說明:
    獲取指定股票清單(stock_symbols)在給定日期範圍內每日收盤價。
    注意：stock_symbols 必須包含正確後綴 (.TW 或 .TWO)，此函式不再自動添加。
    """
    
    print(f"Downloading yfinance data for {len(stock_symbols)} tickers...")

    # 1. 下載資料 (不再修改 stock_symbols，完全信任傳入的列表)
    # auto_adjust=True 會自動處理除權息價格，通常對回測較方便，若需原始價格可設為 False
    df = yf.download(stock_symbols, start=start_date, end=end_date, auto_adjust=True)

    # 2. 處理資料結構 (只取 Close)
    # yfinance 若下載多檔股票，columns 會是 MultiIndex (Price, Ticker)
    if isinstance(df.columns, pd.MultiIndex):
        try:
            # 優先取 Close，若 auto_adjust=True 有時會直接回傳修正後價格，視版本而定
            # 這裡做個防呆，確保取到收盤價
            target_col = "Close" if "Close" in df.columns.levels[0] else df.columns.levels[0][0]
            df = df[target_col]
        except Exception as e:
            print(f"Error extracting Close price: {e}")
            return pd.DataFrame()
    elif "Close" in df.columns:
        df = df["Close"]

    # 3. 處理單支股票的特殊情況 (Series -> DataFrame)
    if isinstance(df, pd.Series):
        df = df.to_frame()
        df.columns = stock_symbols

    # 4. 處理缺失值 (Forward Fill)
    df = df.ffill()

    # 5. 移除全空的欄位 (避免下載失敗的股票佔用欄位)
    df = df.dropna(axis=1, how='all')

    # 6. 清洗欄位名稱
    # 使用 Regex 同時移除結尾的 .TW 或 .TWO (例: "2330.TW" -> "2330", "5347.TWO" -> "5347")
    df.columns = df.columns.str.replace(r"\.TW(O)?$", "", regex=True)

    return df