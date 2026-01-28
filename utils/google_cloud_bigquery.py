import os
import gc
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from utils.logger import logger
from typing import Annotated

def load_to_bigquery(
    df: Annotated[pd.DataFrame, "要上傳到 BigQuery 的 DataFrame"],
    dataset_id: Annotated[str, "BigQuery Dataset ID"],
    table_id: Annotated[str, "BigQuery Table ID"],
    if_exists: Annotated[str, "BigQuery 如何處理已存在的資料"] = 'upsert'
):
    """
    函數說明：
    使用 暫存表 + MERGE 的方式將 DataFrame 上傳到 BigQuery，支援 Upsert 功能
    """
    try:
        project_id = os.getenv("GCP_PROJECT_ID")
        client = bigquery.Client(project=project_id)

        # 確保 Dataset 存在
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-east1"
            client.create_dataset(dataset)
            logger.info(f"Created dataset: {dataset_id}")

        # 如果使用者只想簡單 append 或 truncate
        if if_exists != 'upsert':
            job_config = bigquery.LoadJobConfig(write_disposition=f"WRITE_{if_exists.upper()}")
            client.load_table_from_dataframe(df, dataset_ref.table(table_id), job_config=job_config).result()
            return

        # 執行 Upsert (Merge) 邏輯
        staging_table_id = f"{table_id}_staging_{pd.Timestamp.now().strftime('%H%M%S')}"
        staging_table_ref = dataset_ref.table(staging_table_id)
        target_table_ref = dataset_ref.table(table_id)

        # 將資料上傳到暫存表 (Truncate 確保暫存表乾淨)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, staging_table_ref, job_config=job_config).result()

        # 執行 MERGE SQL
        # 主鍵是 date 和 stock_id
        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` T
        USING `{project_id}.{dataset_id}.{staging_table_id}` S
        ON T.date = S.date AND T.stock_id = S.stock_id
        WHEN MATCHED THEN
            UPDATE SET close = S.close, daily_return = S.daily_return
        WHEN NOT MATCHED THEN
            INSERT (date, stock_id, close, daily_return) 
            VALUES (date, stock_id, close, daily_return)
        """
        
        # 檢查目標表是否存在，不存在則直接從暫存表建立
        try:
            client.get_table(target_table_ref)
            query_job = client.query(merge_sql)
            query_job.result()
            logger.info(f"Upsert completed for {table_id}")
        except NotFound:
            # 如果目標表根本不存在，直接把暫存表重新命名或複製過去
            logger.info(f"Target table {table_id} not found, creating from staging...")
            client.copy_table(staging_table_ref, target_table_ref).result()

        # 刪除暫存表
        client.delete_table(staging_table_ref, not_found_ok=True)

    except Exception as e:
        logger.error(f"BigQuery Load Error: {e}")
        raise
    finally:
        gc.collect() # 執行完畢強制回收記憶體