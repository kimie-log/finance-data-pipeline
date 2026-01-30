import pandas as pd
import numpy as np
import gc
from typing import Annotated
from utils.logger import logger


class Transformer:
    @staticmethod
    def process_ohlcv_data(
        df_raw: Annotated[
            pd.DataFrame,
            "yfinance 抓取的原始 OHLCV 價量資料 (long format)",
        ]
    ) -> Annotated[
        pd.DataFrame,
        "處理後的 OHLCV 市場資料 (long format, 含 daily_return)",
    ]:
        '''
        函式說明：
        將 OHLCV 價量資料整理為 fact table 需要的結構：
        - 正規化欄位名稱與型別
        - 依股票與日期排序
        - 計算日報酬率 daily_return
        '''

        logger.info(f"Starting OHLCV transformation. Input shape: {df_raw.shape}")

        df = df_raw.copy()

        # 欄位標準化：datetime -> date, asset -> stock_id
        df.rename(columns={"datetime": "date", "asset": "stock_id"}, inplace=True)

        # 型別與排序處理
        df["date"] = pd.to_datetime(df["date"])
        df["stock_id"] = df["stock_id"].astype(str)

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 依股票與時間排序，方便 groupby 計算
        df.sort_values(by=["stock_id", "date"], inplace=True, ignore_index=True)

        # 補齊缺失值：價格欄位以前值補齊，volume 缺失視為 0
        price_cols = ["open", "high", "low", "close"]
        df[price_cols] = df.groupby("stock_id", sort=False)[price_cols].ffill()
        df["volume"] = df["volume"].fillna(0)

        # 移除仍為 NaN 的 close（代表該股票整段無有效資料）
        df.dropna(subset=["close"], inplace=True)

        # 計算日報酬率：以收盤價為基準
        df["daily_return"] = (
            df.groupby("stock_id", sort=False)["close"]
            .pct_change()
            .astype(np.float32)
        )

        # 交易可行性標記：無外部來源時以簡單啟發式標記（volume=0 且 OHLC 同價視為可能停牌／漲跌停）
        df["is_suspended"] = 0
        df["is_limit_up"] = 0
        df["is_limit_down"] = 0
        mask_same = (df["volume"] == 0) & (df["open"] == df["close"]) & (df["high"] == df["low"])
        if mask_same.any():
            prev_close = df.groupby("stock_id", sort=False)["close"].shift(1)
            df.loc[mask_same & (df["close"] > prev_close), "is_limit_up"] = 1
            df.loc[mask_same & (df["close"] < prev_close), "is_limit_down"] = 1
            df.loc[mask_same & (df["close"] == prev_close), "is_suspended"] = 1

        # 最終欄位順序
        final_cols = [
            "date",
            "stock_id",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "daily_return",
            "is_suspended",
            "is_limit_up",
            "is_limit_down",
        ]
        final_df = df[final_cols].copy()

        logger.info(
            "OHLCV Market Data Transformed. Final Memory Usage: %.2f MB",
            final_df.memory_usage().sum() / 1024**2,
        )

        # 釋放中間物件
        del df
        gc.collect()

        return final_df