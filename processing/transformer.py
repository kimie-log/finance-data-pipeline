import pandas as pd
from typing import Annotated
from utils.logger import logger


class Transformer:

    @staticmethod
    def process_market_data(
        df_raw: Annotated[pd.DataFrame, "yfinance 抓取的原始股價資料 (wide format)"]
    ) -> Annotated[pd.DataFrame, "處理後的市場資料 (long format)"]:
        """
        將寬表格轉為長表格，並執行以下清洗流程：
        1. Melt (Wide to Long)
        2. Type Casting & Sorting (排序是 FFill 的前提)
        3. Handling Missing Data (Forward Fill + Dropna)
        4. Feature Engineering (Daily Return)
        """
        logger.info("Starting transformation for Market Data...")

        # 重設索引與轉置 (Wide to Long)
        df = df_raw.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
        df_melted = df.melt(id_vars=['date'], var_name='stock_id', value_name='close')

        # 型別轉換
        df_melted['date'] = pd.to_datetime(df_melted['date'])
        df_melted['close'] = pd.to_numeric(df_melted['close'], errors='coerce')
        
        # 排序：先按股票、再按日期排序
        df_melted = df_melted.sort_values(by=['stock_id', 'date']).reset_index(drop=True)

        # 處理缺失值 (Forward Fill)
        initial_nulls = df_melted['close'].isnull().sum()
        if initial_nulls > 0:
            # 對每個 stock_id 獨立進行前向填補
            df_melted['close'] = df_melted.groupby('stock_id')['close'].ffill()
            
            # 如果填補後還有 null (該股票開頭就是 NaN)，則移除
            remaining_nulls = df_melted['close'].isnull().sum()
            if remaining_nulls > 0:
                df_melted = df_melted.dropna(subset=['close'])
                logger.info(f"Dropped {remaining_nulls} rows that couldn't be filled at start.")
            
            logger.info(f"Handled {initial_nulls - remaining_nulls} nulls via Forward Fill.")

        # 衍生特徵計算 (在排序好的基礎上計算)
        # pct_change 必須在 groupby 之後 (避免 A 股票的第一筆報酬率算到 B 股票的最後一筆)
        df_melted['daily_return'] = df_melted.groupby('stock_id')['close'].pct_change()

        # 最終整理欄位
        final_df = df_melted[['date', 'stock_id', 'close', 'daily_return']].copy()
        
        logger.info(f"Market Data Transformed. Shape: {final_df.shape}")
        return final_df