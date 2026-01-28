import pandas as pd
import numpy as np
import gc
from typing import Annotated
from utils.logger import logger

class Transformer:

    @staticmethod
    def process_market_data(
        df_raw: Annotated[pd.DataFrame, "yfinance 抓取的原始股價資料 (wide format)"]
    ) -> Annotated[pd.DataFrame, "處理後的市場資料 (long format)"]:
        
        logger.info(f"Starting transformation. Input shape: {df_raw.shape}")

        # 1. Wide to Long
        df = df_raw.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
        df_melted = df.melt(id_vars=['date'], var_name='stock_id', value_name='close')
        
        # 釋放原始巨大的 wide dataframe
        del df, df_raw
        gc.collect()

        # 2. 型別轉換與優化 (float64 -> float32)
        df_melted['date'] = pd.to_datetime(df_melted['date'])
        df_melted['close'] = pd.to_numeric(df_melted['close'], errors='coerce').astype(np.float32)
        df_melted['stock_id'] = df_melted['stock_id'].astype(str)
        
        # 3. 排序 (為 FFill 做準備)
        df_melted.sort_values(by=['stock_id', 'date'], inplace=True, ignore_index=True)

        # 4. 處理缺失值
        # 使用 groupby ffill，這是最佔記憶體的步驟
        df_melted['close'] = df_melted.groupby('stock_id', sort=False)['close'].ffill()
        df_melted.dropna(subset=['close'], inplace=True)

        # 5. 計算 Daily Return (同樣使用 float32)
        df_melted['daily_return'] = (
            df_melted.groupby('stock_id', sort=False)['close']
            .pct_change()
            .astype(np.float32)
        )

        # 6. 最終整理 (過濾掉不需要的欄位並釋放記憶體)
        final_df = df_melted[['date', 'stock_id', 'close', 'daily_return']].copy()
        
        del df_melted
        gc.collect()
        
        logger.info(f"Market Data Transformed. Final Memory Usage: {final_df.memory_usage().sum() / 1024**2:.2f} MB")
        return final_df