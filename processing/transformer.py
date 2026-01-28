import pandas as pd
import numpy as np
from typing import List
from utils.logger import logger

class DataTransformer:

    @staticmethod
    def process_market_data(df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        將 yfinance 的寬表格轉為長表格，並處理缺失值
        Input: Index=Date, Columns=Stock_IDs (Close prices)
        Output: Columns=[date, stock_id, close, return_pct]
        """
        logger.info("Starting transformation for Market Data...")

        # 1. 重設索引，準備 Melt (寬轉長)
        df = df_raw.reset_index()
        
        # 確保日期欄位名稱一致
        if 'Date' in df.columns:
            df = df.rename(columns={'Date': 'date'})
        
        # 2. Melt: 將股票代號從欄位轉成資料列 (这是 SQL Table 的標準格式)
        # 假設 raw data 的 columns 除了 'date' 以外都是股票代號
        df_melted = df.melt(id_vars=['date'], var_name='stock_id', value_name='close')

        # 3. 資料型態轉換 (Type Casting)
        df_melted['date'] = pd.to_datetime(df_melted['date'])
        df_melted['close'] = pd.to_numeric(df_melted['close'], errors='coerce') # 強制轉浮點數，錯誤變 NaN

        # 4. 異常處理與數據清洗 (Handling Missing Data)
        # 檢查缺失值
        initial_rows = len(df_melted)
        null_count = df_melted['close'].isnull().sum()
        
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in Close Price.")
            # 策略 A: 移除缺失值 (若是完全沒交易)
            df_melted = df_melted.dropna(subset=['close'])
            # 策略 B (可選): 若是中間缺值，可考慮 Forward Fill (需先按 stock_id, date 排序)
            # df_melted['close'] = df_melted.groupby('stock_id')['close'].ffill()

        dropped_rows = initial_rows - len(df_melted)
        logger.info(f"Data Cleaning: Dropped {dropped_rows} rows due to null/invalid data.")

        # 5. 增加衍生特徵 (Feature Engineering 範例)
        # 計算日報酬率 (需先排序)
        df_melted = df_melted.sort_values(by=['stock_id', 'date'])
        df_melted['daily_return'] = df_melted.groupby('stock_id')['close'].pct_change()

        # 6. 最後整理 Schema
        final_df = df_melted[['date', 'stock_id', 'close', 'daily_return']].copy()
        
        logger.info(f"Market Data Transformed. Shape: {final_df.shape}")
        return final_df

    @staticmethod
    def process_financial_data(df_raw: pd.DataFrame, metric_name: str) -> pd.DataFrame:
        """
        處理 FinLab 財報資料 (示範)
        """
        logger.info(f"Transforming Financial Data: {metric_name}")
        
        # 假設 FinLab 回傳的是 MultiIndex 或類似結構，這裡做正規化
        # 這裡視 FinLab 回傳格式而定，通常需要 reset_index 並 rename
        df = df_raw.reset_index()
        
        # 簡單標準化欄位名稱
        df.columns = [str(c).lower().replace(' ', '_') for c in df.columns]
        
        # 填充 NaN (例如財報空值補 0，視會計意義而定)
        df = df.fillna(0)
        
        return df