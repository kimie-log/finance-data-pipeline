import io
import os
import gc
import numpy as np
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
    # 避免 pd.NA / NaT 導致 load_table_from_dataframe 轉 PyArrow 時出錯（arg must be a list, tuple, 1-d array, or Series）
    df = df.replace({pd.NA: None, pd.NaT: None}).copy()
    # object 欄位：缺值改 None，非純量（list/dict 等）轉字串，確保每欄為單純 1-d 純量序列
    for col in df.columns:
        if df[col].dtype == object:
            def _scalarize(x):
                if pd.isna(x) or x is None:
                    return None
                if isinstance(x, (list, dict)):
                    return str(x)
                return x
            df[col] = df[col].map(_scalarize)
    # 全為 None 的 object 欄位改空字串，避免 PyArrow 推斷 schema 時失敗
    for col in df.columns:
        if df[col].dtype == object and df[col].isna().all():
            df[col] = ""

    # BigQuery 常見可重試錯誤，集中管理便於統一處理
    retryable_exceptions = (
        gcp_exceptions.ServiceUnavailable,
        gcp_exceptions.DeadlineExceeded,
        gcp_exceptions.InternalServerError,
        gcp_exceptions.TooManyRequests,
        gcp_exceptions.Aborted,
        gcp_exceptions.GatewayTimeout,
    )

    # 暫存表參考，用於 finally 的清理邏輯
    staging_table_ref = None

    try:
        # 從環境變數取得專案 ID，避免在程式碼中硬編碼
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            logger.error("Missing required environment variable: GCP_PROJECT_ID")
            raise ValueError("GCP_PROJECT_ID is not set")

        # 建立 BigQuery client
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
            # 若 Dataset 不存在則建立，避免後續任務失敗
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-east1"
            run_with_retry(
                lambda: client.create_dataset(dataset),
                action_name=f"BigQuery create dataset {dataset_id}",
                retry_exceptions=retryable_exceptions,
            )
            logger.info("Created dataset: %s", dataset_id)

        # 如果使用者只想簡單 append 或 truncate：改走 Parquet 再載入，避免 load_table_from_dataframe 轉換 object 欄位時出錯
        if if_exists != 'upsert':
            # 欄位名正規化為字串（BigQuery 不接受 tuple 如 ('date','')），重複時加後綴
            def _col_name(c):
                return c[0] if isinstance(c, tuple) else str(c)

            seen = {}
            def _unique_key(col):
                k = _col_name(col)
                if k in seen:
                    seen[k] += 1
                    return f"{k}_{seen[k]}"
                seen[k] = 0
                return k

            # 從純 Python list 重建 DataFrame，確保每欄為 1-d 序列，避免 "arg must be a list, tuple, 1-d array, or Series"
            data = {}
            for col in df.columns:
                key = _unique_key(col)
                if df[col].dtype == object:
                    data[key] = ["" if (x is None or pd.isna(x)) else str(x) for x in df[col]]
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    s = pd.to_datetime(df[col], errors="coerce")
                    data[key] = [(x.strftime("%Y-%m-%d") if pd.notna(x) else "") for x in s]
                else:
                    data[key] = df[col].tolist()
            _df = pd.DataFrame(data)
            job_config = bigquery.LoadJobConfig(
                write_disposition=f"WRITE_{if_exists.upper()}",
                source_format=bigquery.SourceFormat.PARQUET,
            )
            table_ref = dataset_ref.table(table_id)

            def _write_and_load():
                buf = io.BytesIO()
                _df.to_parquet(buf, index=False, engine="pyarrow")
                buf.seek(0)
                return client.load_table_from_file(buf, table_ref, job_config=job_config).result()

            run_with_retry(
                _write_and_load,
                action_name=f"BigQuery load {dataset_id}.{table_id} (parquet)",
                retry_exceptions=retryable_exceptions,
            )
            return

        # 執行 Upsert (Merge) 邏輯
        # 使用暫存表避免直接在目標表做大量寫入造成鎖定或部分失敗
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
        # 主鍵是 date 和 stock_id，更新 OHLCV、daily_return、交易可行性欄位
        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` T
        USING `{project_id}.{dataset_id}.{staging_table_id}` S
        ON T.date = S.date AND T.stock_id = S.stock_id
        WHEN MATCHED THEN
            UPDATE SET
                T.open = S.open,
                T.high = S.high,
                T.low = S.low,
                T.close = S.close,
                T.volume = S.volume,
                T.daily_return = S.daily_return,
                T.is_suspended = S.is_suspended,
                T.is_limit_up = S.is_limit_up,
                T.is_limit_down = S.is_limit_down
        WHEN NOT MATCHED THEN
            INSERT (date, stock_id, open, high, low, close, volume, daily_return, is_suspended, is_limit_up, is_limit_down)
            VALUES (S.date, S.stock_id, S.open, S.high, S.low, S.close, S.volume, S.daily_return, S.is_suspended, S.is_limit_up, S.is_limit_down)
        """
        
        # 檢查目標表是否存在，不存在則直接從暫存表建立
        try:
            run_with_retry(
                lambda: client.get_table(target_table_ref),
                action_name=f"BigQuery get table {dataset_id}.{table_id}",
                retry_exceptions=retryable_exceptions,
            )
            # 目標表存在時進行 MERGE
            run_with_retry(
                lambda: client.query(merge_sql).result(),
                action_name=f"BigQuery merge {dataset_id}.{table_id}",
                retry_exceptions=retryable_exceptions,
            )
            logger.info("Upsert completed for %s", table_id)
        except NotFound:
            # 如果目標表根本不存在，直接把暫存表複製過去（單一 source，須指定 location 與 dataset 一致）
            logger.info("Target table %s not found, creating from staging...", table_id)
            job_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
            run_with_retry(
                lambda: client.copy_table(
                    staging_table_ref,
                    target_table_ref,
                    job_config=job_config,
                    location="asia-east1",
                ).result(),
                action_name=f"BigQuery copy {dataset_id}.{staging_table_id} -> {table_id}",
                retry_exceptions=retryable_exceptions,
            )

        # 刪除暫存表，避免佔用儲存空間與成本
        run_with_retry(
            lambda: client.delete_table(staging_table_ref, not_found_ok=True),
            action_name=f"BigQuery delete staging {dataset_id}.{staging_table_id}",
            retry_exceptions=retryable_exceptions,
        )

    except Exception as e:
        # 記錄完整上下文，便於除錯與追蹤
        logger.exception(
            "BigQuery Load Error (dataset=%s, table=%s, mode=%s): %s",
            dataset_id,
            table_id,
            if_exists,
            e,
        )
        raise
    finally:
        # 確保暫存表清理，即使前面流程失敗也嘗試回收資源
        if staging_table_ref is not None:
            try:
                run_with_retry(
                    lambda: client.delete_table(staging_table_ref, not_found_ok=True),
                    action_name=f"BigQuery cleanup staging {dataset_id}.{staging_table_id}",
                    retry_exceptions=retryable_exceptions,
                )
            except Exception:
                # 清理失敗只記錄警告，不中斷主流程
                logger.warning(
                    "Failed to cleanup staging table %s (ignored).",
                    staging_table_ref.table_id,
                )
        # 強制回收記憶體，避免大資料處理後的記憶體殘留
        gc.collect() # 執行完畢強制回收記憶體