"""
資料轉換模組：OHLCV 清洗與交易可行性標記

將 yfinance 抓取的原始價量資料轉換為 BigQuery fact_price 表所需格式
包含欄位正規化、缺失值處理、日報酬計算與交易可行性標記（停牌、漲跌停）
產出欄位與型別對齊後續 BigQuery 寫入與回測需求
"""
from __future__ import annotations

import gc
from typing import Annotated

import numpy as np
import pandas as pd

from utils.logger import logger


class Transformer:
    """
    OHLCV 資料轉換與清洗

    所有方法為 staticmethod。產出欄位與型別對齊 BigQuery fact_price 寫入需求，
    包含 daily_return 計算與交易可行性標記（is_suspended, is_limit_up, is_limit_down）
    """

    @staticmethod
    def process_ohlcv_data(
        df_raw: Annotated[
            pd.DataFrame,
            "yfinance 抓取的原始 OHLCV 價量資料 (long format)，欄位：datetime, asset, open, high, low, close, volume",
        ],
    ) -> pd.DataFrame:
        """
        將 OHLCV 價量資料整理為 fact_price 表所需結構

        實務：欄位命名標準化（datetime→date, asset→stock_id）、型別轉換、缺失值處理（價格 ffill、volume 填 0）、
        日報酬計算（pct_change）、交易可行性標記（啟發式：volume=0 且 OHLC 同價時判斷停牌／漲跌停）
        產出欄位順序固定，以利 BigQuery 寫入與後續查詢

        Args:
            df_raw: yfinance 原始資料，long format，欄位含 datetime, asset, open, high, low, close, volume

        Returns:
            DataFrame 欄位：date, stock_id, open, high, low, close, volume, daily_return,
            is_suspended, is_limit_up, is_limit_down。已依 stock_id, date 排序；無效資料（close 為 NaN）已移除

        Note:
            - daily_return 為 float32 以節省記憶體
            - 交易可行性標記為啟發式，無外部資料源時使用；有外部來源時建議覆寫
            - 處理後會執行 gc.collect() 釋放記憶體
        """
        logger.info(f"Starting OHLCV transformation. Input shape: {df_raw.shape}")

        df = df_raw.copy()

        # 欄位命名標準化：datetime → date，asset → stock_id
        df.rename(columns={"datetime": "date", "asset": "stock_id"}, inplace=True)

        # 型別轉換：date 轉成 datetime，stock_id 轉成字串
        df["date"] = pd.to_datetime(df["date"])
        df["stock_id"] = df["stock_id"].astype(str)

        # 量欄位強制轉為數字，無法轉換的則轉換為 NaN
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 排序：依 stock_id, date 排序
        df.sort_values(by=["stock_id", "date"], inplace=True, ignore_index=True)

        # 價格欄位向前填補：以同一檔股票的時間序列為單位，用 `ffill()` 向前填補
        price_cols = ["open", "high", "low", "close"]
        df[price_cols] = df.groupby("stock_id", sort=False)[price_cols].ffill()
        df["volume"] = df["volume"].fillna(0)

        # 移除無效列：刪掉 close 為 NaN 的列：確保每列至少有收盤價，才能算日報酬
        df.dropna(subset=["close"], inplace=True)

        # 計算日報酬：對每檔股票的 close 做 `pct_change()` 得到每日相對前一日的百分比變化
        df["daily_return"] = (
            df.groupby("stock_id", sort=False)["close"]
            .pct_change()
            .astype(np.float32)
        )

        # 交易可行性標記：volume=0 且 OHLC 同價時判斷停牌／漲跌停
        df["is_suspended"] = 0
        df["is_limit_up"] = 0
        df["is_limit_down"] = 0
        mask_same = (df["volume"] == 0) & (df["open"] == df["close"]) & (df["high"] == df["low"])
        # 若 mask_same 有任何 True，則計算前一日收盤價
        if mask_same.any():
            prev_close = df.groupby("stock_id", sort=False)["close"].shift(1)
            df.loc[mask_same & (df["close"] > prev_close), "is_limit_up"] = 1
            df.loc[mask_same & (df["close"] < prev_close), "is_limit_down"] = 1
            df.loc[mask_same & (df["close"] == prev_close), "is_suspended"] = 1

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

        del df
        gc.collect()

        return final_df
