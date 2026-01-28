import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import pandas as pd
from utils.logger import logger

def load_to_bigquery(df: pd.DataFrame, dataset_id: str, table_id: str, if_exists: str = 'append'):
    """
    將 DataFrame 上傳至 BigQuery，若 Dataset 不存在則自動建立。
    """
    try:
        # 1. 建立 Client
        project_id = os.getenv("GCP_PROJECT_ID")
        # 如果環境變數已有憑證，直接建立 Client 即可
        client = bigquery.Client(project=project_id)

        # 2. 檢查並建立 Dataset (自動化關鍵邏輯)
        dataset_ref = client.dataset(dataset_id)
        
        try:
            client.get_dataset(dataset_ref)
            # logger.info(f"Dataset {dataset_id} exists.")
        except NotFound:
            logger.warning(f"Dataset {dataset_id} not found. Creating it now...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-east1"  
            client.create_dataset(dataset)
            logger.info(f"Successfully created dataset: {dataset_id}")

        # 3. 設定 Table 參照
        table_ref = dataset_ref.table(table_id)

        # 4. 設定 Job Config
        job_config = bigquery.LoadJobConfig(
            autodetect=True, # 自動偵測 Schema (Float, String, Date...)
            write_disposition=f"WRITE_{if_exists.upper()}", # WRITE_APPEND 或 WRITE_TRUNCATE
        )

        logger.info(f"Uploading {len(df)} rows to {dataset_id}.{table_id}...")

        # 5. 執行上傳 Job
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        
        job.result()  # 等待 Job 完成
        
        # 6. 確認結果
        table = client.get_table(table_ref)
        logger.info(f"✅ Loaded {table.num_rows} rows to BigQuery: {dataset_id}.{table_id}")

    except Exception as e:
        logger.error(f"❌ BigQuery Load Error: {e}")
        raise