import os
import gc
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core import exceptions as gcp_exceptions
import pandas as pd
from utils.logger import logger
from utils.retry import run_with_retry
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
    retryable_exceptions = (
        gcp_exceptions.ServiceUnavailable,
        gcp_exceptions.DeadlineExceeded,
        gcp_exceptions.InternalServerError,
        gcp_exceptions.TooManyRequests,
        gcp_exceptions.Aborted,
        gcp_exceptions.GatewayTimeout,
    )

    staging_table_ref = None

    try:
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            logger.error("Missing required environment variable: GCP_PROJECT_ID")
            raise ValueError("GCP_PROJECT_ID is not set")

        client = bigquery.Client(project=project_id)

        # 確保 Dataset 存在
        dataset_ref = client.dataset(dataset_id)
        try:
            run_with_retry(
                lambda: client.get_dataset(dataset_ref),
                action_name=f"BigQuery get dataset {dataset_id}",
                retry_exceptions=retryable_exceptions,
            )
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-east1"
            run_with_retry(
                lambda: client.create_dataset(dataset),
                action_name=f"BigQuery create dataset {dataset_id}",
                retry_exceptions=retryable_exceptions,
            )
            logger.info("Created dataset: %s", dataset_id)

        # 如果使用者只想簡單 append 或 truncate
        if if_exists != 'upsert':
            job_config = bigquery.LoadJobConfig(write_disposition=f"WRITE_{if_exists.upper()}")
            run_with_retry(
                lambda: client.load_table_from_dataframe(
                    df, dataset_ref.table(table_id), job_config=job_config
                ).result(),
                action_name=f"BigQuery load {dataset_id}.{table_id}",
                retry_exceptions=retryable_exceptions,
            )
            return

        # 執行 Upsert (Merge) 邏輯
        staging_table_id = f"{table_id}_staging_{pd.Timestamp.now().strftime('%H%M%S')}"
        staging_table_ref = dataset_ref.table(staging_table_id)
        target_table_ref = dataset_ref.table(table_id)

        # 將資料上傳到暫存表 (Truncate 確保暫存表乾淨)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        run_with_retry(
            lambda: client.load_table_from_dataframe(
                df, staging_table_ref, job_config=job_config
            ).result(),
            action_name=f"BigQuery load staging {dataset_id}.{staging_table_id}",
            retry_exceptions=retryable_exceptions,
        )

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
            run_with_retry(
                lambda: client.get_table(target_table_ref),
                action_name=f"BigQuery get table {dataset_id}.{table_id}",
                retry_exceptions=retryable_exceptions,
            )
            run_with_retry(
                lambda: client.query(merge_sql).result(),
                action_name=f"BigQuery merge {dataset_id}.{table_id}",
                retry_exceptions=retryable_exceptions,
            )
            logger.info("Upsert completed for %s", table_id)
        except NotFound:
            # 如果目標表根本不存在，直接把暫存表重新命名或複製過去
            logger.info("Target table %s not found, creating from staging...", table_id)
            run_with_retry(
                lambda: client.copy_table(staging_table_ref, target_table_ref).result(),
                action_name=f"BigQuery copy {dataset_id}.{staging_table_id} -> {table_id}",
                retry_exceptions=retryable_exceptions,
            )

        # 刪除暫存表
        run_with_retry(
            lambda: client.delete_table(staging_table_ref, not_found_ok=True),
            action_name=f"BigQuery delete staging {dataset_id}.{staging_table_id}",
            retry_exceptions=retryable_exceptions,
        )

    except Exception as e:
        logger.exception(
            "BigQuery Load Error (dataset=%s, table=%s, mode=%s): %s",
            dataset_id,
            table_id,
            if_exists,
            e,
        )
        raise
    finally:
        if staging_table_ref is not None:
            try:
                run_with_retry(
                    lambda: client.delete_table(staging_table_ref, not_found_ok=True),
                    action_name=f"BigQuery cleanup staging {dataset_id}.{staging_table_id}",
                    retry_exceptions=retryable_exceptions,
                )
            except Exception:
                logger.warning(
                    "Failed to cleanup staging table %s (ignored).",
                    staging_table_ref.table_id,
                )
        gc.collect() # 執行完畢強制回收記憶體