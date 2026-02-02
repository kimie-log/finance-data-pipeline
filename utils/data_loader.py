"""
資料載入工具：從 BigQuery 或本地 Parquet 檔案讀取資料

優先使用本地檔案（效率較高），若不存在則從 BigQuery 查詢。
支援讀取價量資料（fact_price）和因子資料（fact_factor）。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Optional

import numpy as np
import pandas as pd
from google.cloud import bigquery

from utils.logger import logger


def load_price_data(
    dataset_id: Annotated[str, "BigQuery Dataset ID"],
    start_date: Annotated[str, "起始日期 YYYY-MM-DD"],
    end_date: Annotated[str, "結束日期 YYYY-MM-DD"],
    local_parquet_path: Annotated[Optional[str], "本地 parquet 檔案路徑（可選）"] = None,
    use_local_first: Annotated[bool, "是否優先使用本地檔案"] = True,
) -> pd.DataFrame:
    """
    從 BigQuery 或本地 Parquet 檔案讀取價量資料（fact_price）

    Args:
        dataset_id: BigQuery Dataset ID
        start_date: 起始日期 YYYY-MM-DD
        end_date: 結束日期 YYYY-MM-DD
        local_parquet_path: 本地 parquet 檔案路徑（可選）
        use_local_first: 是否優先使用本地檔案（預設 True，效率較高）

    Returns:
        DataFrame 欄位：date, stock_id, open, high, low, close, volume, daily_return,
        is_suspended, is_limit_up, is_limit_down
    """
    # 優先使用本地檔案
    if use_local_first and local_parquet_path:
        local_path = Path(local_parquet_path)
        if local_path.exists():
            logger.info(f"從本地檔案讀取價量資料: {local_path}")
            df = pd.read_parquet(local_path)
            # 過濾日期範圍
            df["date"] = pd.to_datetime(df["date"])
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            logger.info(f"讀取完成: {len(df)} 筆資料，{df['stock_id'].nunique()} 檔股票")
            return df

    # 從 BigQuery 查詢
    logger.info(f"從 BigQuery 讀取價量資料: {dataset_id}.fact_price")
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID 環境變數未設定")

    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT 
        date,
        stock_id,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        is_suspended,
        is_limit_up,
        is_limit_down
    FROM `{project_id}.{dataset_id}.fact_price`
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    ORDER BY date, stock_id
    """

    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    logger.info(f"讀取完成: {len(df)} 筆資料，{df['stock_id'].nunique()} 檔股票")
    return df


def load_factor_data(
    dataset_id: Annotated[str, "BigQuery Dataset ID"],
    factor_name: Annotated[str, "因子名稱"],
    start_date: Annotated[str, "起始日期 YYYY-MM-DD"],
    end_date: Annotated[str, "結束日期 YYYY-MM-DD"],
    local_parquet_path: Annotated[Optional[str], "本地 parquet 檔案路徑（可選）"] = None,
    use_local_first: Annotated[bool, "是否優先使用本地檔案"] = True,
    factor_table: Annotated[str, "因子表名稱"] = "fact_factor",
) -> pd.DataFrame:
    """
    從 BigQuery 或本地 Parquet 檔案讀取因子資料（fact_factor）

    Args:
        dataset_id: BigQuery Dataset ID
        factor_name: 因子名稱
        start_date: 起始日期 YYYY-MM-DD
        end_date: 結束日期 YYYY-MM-DD
        local_parquet_path: 本地 parquet 檔案路徑（可選）
        use_local_first: 是否優先使用本地檔案（預設 True，效率較高）
        factor_table: 因子表名稱（預設 "fact_factor"，可帶後綴如 "fact_factor_value"）

    Returns:
        DataFrame，MultiIndex (date, stock_id)，單一欄位為因子值
    """
    # 優先使用本地檔案
    if use_local_first and local_parquet_path:
        local_path = Path(local_parquet_path)
        if local_path.exists():
            logger.info(f"從本地檔案讀取因子資料: {local_path}")
            df = pd.read_parquet(local_path)
            # 過濾日期範圍和因子名稱
            df["date"] = pd.to_datetime(df["date"])
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            if "factor_name" in df.columns:
                df = df[df["factor_name"] == factor_name]
            # 轉換為 MultiIndex
            if "date" in df.columns and "stock_id" in df.columns:
                # 尋找因子值欄位（可能是 value 或其他名稱）
                factor_cols = [c for c in df.columns if c not in ["date", "stock_id", "factor_name"]]
                if factor_cols:
                    factor_col = factor_cols[0]
                    df_result = df.set_index(["date", "stock_id"])[[factor_col]]
                    df_result.columns = [factor_name]
                    logger.info(f"讀取完成: {len(df_result)} 筆因子資料")
                    return df_result

    # 從 BigQuery 查詢
    logger.info(f"從 BigQuery 讀取因子資料: {dataset_id}.{factor_table}")
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID 環境變數未設定")

    client = bigquery.Client(project=project_id)

    # 檢查表結構（可能有多種格式）
    query_check = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{factor_table}'
    """
    try:
        columns_df = client.query(query_check).to_dataframe()
        columns = columns_df["column_name"].tolist()
    except Exception:
        # 如果查詢失敗，假設標準格式
        columns = ["date", "stock_id", "factor_name", "factor_value"]

    # 根據表結構構建查詢（使用參數化查詢避免中文編碼問題）
    if "factor_name" in columns:
        # 標準格式：date, stock_id, factor_name, value
        # 使用參數化查詢處理中文因子名稱
        query = f"""
        SELECT 
            date,
            stock_id,
            value as factor_value
        FROM `{project_id}.{dataset_id}.{factor_table}`
        WHERE DATE(date) >= @start_date AND DATE(date) <= @end_date
            AND factor_name = @factor_name
        ORDER BY date, stock_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("factor_name", "STRING", factor_name),
            ]
        )
    else:
        # 假設因子名稱直接作為欄位名（需要轉義欄位名）
        # 使用反引號包起來，並使用參數化查詢處理日期
        factor_name_escaped = factor_name.replace("`", "\\`")
        query = f"""
        SELECT 
            date,
            stock_id,
            `{factor_name_escaped}` as factor_value
        FROM `{project_id}.{dataset_id}.{factor_table}`
        WHERE DATE(date) >= @start_date AND DATE(date) <= @end_date
        ORDER BY date, stock_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )

    df = client.query(query, job_config=job_config).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])

    # 轉換為 MultiIndex（欄位名統一為 factor_value，然後重命名為因子名稱）
    if "factor_value" in df.columns:
        df = df.set_index(["date", "stock_id"])[["factor_value"]]
        df.columns = [factor_name]
    else:
        # 如果沒有 factor_value 欄位，使用第一個數值欄位
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            df = df.set_index(["date", "stock_id"])[[numeric_cols[0]]]
            df.columns = [factor_name]
        else:
            raise ValueError(f"無法找到因子值欄位")
    
    logger.info(f"讀取完成: {len(df)} 筆因子資料")
    return df
