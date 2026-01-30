"""
scripts/run_etl_pipeline 的整合測試：CLI 參數驗證、流程整合、檔名與 BigQuery 命名規則。

驗證 ETL pipeline 主流程：參數驗證、FinLab universe 取得、yfinance OHLCV 抓取、
Transformer 轉換、BigQuery 寫入、GCS 上傳路徑、檔名格式、BigQuery 表名規則。
"""
from unittest import mock

import pandas as pd

import scripts.run_etl_pipeline as pipeline


def test_requires_market_value_date():
    """
    驗證缺少市值日期時直接回傳錯誤（exit code 1）並記錄錯誤日誌。

    實務：參數驗證失敗應提早終止，避免後續步驟浪費資源；錯誤訊息應清楚提示必填參數。
    """
    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.logger") as mock_logger:
                        with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                            mock_parse.return_value = mock.Mock()
                            mock_config.return_value = {}
                            mock_resolve.return_value = {
                                "market_value_dates": [],
                                "market_value_date": None,
                                "start_date": "2020-01-01",
                                "end_date": "2024-01-01",
                                "top_n": 50,
                                "excluded_industry": [],
                                "pre_list_date": None,
                                "dataset_id": "dataset",
                                "skip_gcs": True,
                                "skip_benchmark": True,
                                "skip_calendar": True,
                            }
                            mock_gcp.return_value = "key.json"

                            result = pipeline.main()

                            assert result == 1
                            mock_logger.error.assert_called()


def test_interval_calls_finlab_with_market_value_date():
    """
    驗證 interval 模式需帶入市值基準日期並呼叫 FinLab，完成完整 ETL 流程。

    實務：驗證主流程整合（universe → OHLCV → transform → BigQuery）；fact_price 與 dim_universe
    各寫入一次 BigQuery；mock 所有外部依賴以隔離測試。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 50,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "dataset",
        "skip_gcs": True,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("pandas.DataFrame.to_parquet"):
                                        mock_parse.return_value = mock.Mock()
                                        mock_config.return_value = {}
                                        mock_resolve.return_value = params
                                        mock_gcp.return_value = "key.json"
                                        mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                        mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                        mock_transformer.process_ohlcv_data.return_value = df_processed

                                        result = pipeline.main()

                                        assert result == 0
                                        mock_finlab.fetch_top_stocks_universe.assert_called_once_with(
                                            excluded_industry=[],
                                            pre_list_date=None,
                                            top_n=50,
                                            market_value_date="2024-01-15",
                                        )
                                        assert mock_bq.call_count == 2


def test_skip_gcs_does_not_upload():
    """
    驗證 skip_gcs=True 時不觸發任何 GCS 上傳，僅保留本地 parquet。

    實務：用於本地開發或測試，避免上傳到 GCS 產生成本；驗證條件判斷正確。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 50,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "dataset",
        "skip_gcs": True,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("scripts.run_etl_pipeline.upload_file") as mock_upload:
                                        with mock.patch("pandas.DataFrame.to_parquet"):
                                            mock_parse.return_value = mock.Mock()
                                            mock_config.return_value = {}
                                            mock_resolve.return_value = params
                                            mock_gcp.return_value = "key.json"
                                            mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                            mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                            mock_transformer.process_ohlcv_data.return_value = df_processed

                                            result = pipeline.main()

                                            assert result == 0
                                            mock_upload.assert_not_called()


def test_gcs_upload_paths_interval():
    """
    驗證 interval 模式上傳到 GCS 的路徑：data/raw/{date}/ 與 data/processed/{date}/。

    實務：路徑結構對齊資料湖分層（raw / processed）與日期資料夾，便於管理與查詢。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 50,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "dataset",
        "skip_gcs": False,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("scripts.run_etl_pipeline.upload_file") as mock_upload:
                                        with mock.patch("pandas.DataFrame.to_parquet"):
                                            mock_parse.return_value = mock.Mock()
                                            mock_config.return_value = {}
                                            mock_resolve.return_value = params
                                            mock_gcp.return_value = "key.json"
                                            mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                            mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                            mock_transformer.process_ohlcv_data.return_value = df_processed

                                            result = pipeline.main()

                                            assert result == 0
                                            assert mock_upload.call_count == 2
                                            raw_call, processed_call = mock_upload.call_args_list
                                            assert "data/raw/" in raw_call.args[2]
                                            assert "data/processed/" in processed_call.args[2]
                                            assert "/interval/" not in raw_call.args[2]
                                            assert "/interval/" not in processed_call.args[2]


def test_gcs_upload_paths_interval_mode():
    """
    驗證 interval 模式 GCS 上傳路徑：data/raw/{date}/ 與 data/processed/{date}/（與 test_gcs_upload_paths_interval 重複，可考慮合併）。

    實務：確保路徑結構一致，raw 與 processed 分別上傳到對應目錄。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 50,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "dataset",
        "skip_gcs": False,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("scripts.run_etl_pipeline.upload_file") as mock_upload:
                                        with mock.patch("pandas.DataFrame.to_parquet"):
                                            mock_parse.return_value = mock.Mock()
                                            mock_config.return_value = {}
                                            mock_resolve.return_value = params
                                            mock_gcp.return_value = "key.json"
                                            mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                            mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                            mock_transformer.process_ohlcv_data.return_value = df_processed

                                            result = pipeline.main()

                                            assert result == 0
                                            assert mock_upload.call_count == 2
                                            raw_call, processed_call = mock_upload.call_args_list
                                            assert "data/raw/" in raw_call.args[2]
                                            assert "data/processed/" in processed_call.args[2]
                                            assert "/interval/" not in raw_call.args[2]
                                            assert "/interval/" not in processed_call.args[2]


def test_bigquery_naming_interval_mode():
    """
    驗證 interval 模式的 BigQuery 命名規則：dataset_s{start}_e{end}_mv{date}、表名簡化（fact_price、dim_universe）。

    實務：參數移至 dataset 名稱，表名簡化；不同參數組合會有不同 dataset，避免覆蓋問題。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 30,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "my_dataset",
        "skip_gcs": True,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("pandas.DataFrame.to_parquet"):
                                        mock_parse.return_value = mock.Mock()
                                        mock_config.return_value = {}
                                        mock_resolve.return_value = params
                                        mock_gcp.return_value = "key.json"
                                        mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                        mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                        mock_transformer.process_ohlcv_data.return_value = df_processed

                                        result = pipeline.main()

                                        assert result == 0
                                        assert mock_bq.call_count == 2
                                        fact_call, universe_call = mock_bq.call_args_list
                                        _, fact_kwargs = fact_call
                                        _, uni_kwargs = universe_call
                                        assert fact_kwargs["dataset_id"] == "my_dataset_s20200101_e20240101_mv20240115"
                                        assert fact_kwargs["table_id"] == "fact_price"
                                        assert uni_kwargs["dataset_id"] == "my_dataset_s20200101_e20240101_mv20240115"
                                        assert uni_kwargs["table_id"] == "dim_universe"


def test_interval_filenames_include_date_range():
    """
    驗證 interval 模式本地檔名包含日期區間字串，便於辨識與對應 BigQuery 表名。

    實務：檔名含 {start}_to_{end}，與 BigQuery 表名 s{start}_e{end} 對應，重跑時易於識別。
    """
    params = {
        "market_value_dates": ["2024-01-15"],
        "market_value_date": "2024-01-15",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "top_n": 50,
        "excluded_industry": [],
        "pre_list_date": None,
        "dataset_id": "dataset",
        "skip_gcs": False,
        "skip_benchmark": True,
        "skip_calendar": True,
    }
    df_raw = pd.DataFrame({"2330": [100.0]}, index=pd.to_datetime(["2024-01-01"]))
    df_processed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "stock_id": ["2330"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000],
            "daily_return": [0.0],
            "is_suspended": [0],
            "is_limit_up": [0],
            "is_limit_down": [0],
        }
    )
    universe_df = pd.DataFrame({"stock_id": ["2330"]})

    with mock.patch("scripts.run_etl_pipeline.parse_args") as mock_parse:
        with mock.patch("scripts.run_etl_pipeline.load_config") as mock_config:
            with mock.patch("scripts.run_etl_pipeline.resolve_params") as mock_resolve:
                with mock.patch("scripts.run_etl_pipeline.check_gcp_environment") as mock_gcp:
                    with mock.patch("scripts.run_etl_pipeline.FinLabFetcher") as mock_finlab:
                        with mock.patch("scripts.run_etl_pipeline.YFinanceFetcher") as mock_yf:
                            with mock.patch("scripts.run_etl_pipeline.Transformer") as mock_transformer:
                                with mock.patch("scripts.run_etl_pipeline.load_to_bigquery") as mock_bq:
                                    with mock.patch("scripts.run_etl_pipeline.upload_file") as mock_upload:
                                        with mock.patch("pandas.DataFrame.to_parquet"):
                                            mock_parse.return_value = mock.Mock()
                                            mock_config.return_value = {}
                                            mock_resolve.return_value = params
                                            mock_gcp.return_value = "key.json"
                                            mock_finlab.fetch_top_stocks_universe.return_value = universe_df
                                            mock_yf.fetch_daily_ohlcv_data.return_value = df_raw
                                            mock_transformer.process_ohlcv_data.return_value = df_processed

                                            result = pipeline.main()

                                            assert result == 0
                                            raw_call, processed_call = mock_upload.call_args_list
                                            assert "2020-01-01_to_2024-01-01" in raw_call.args[2]
                                            assert "2020-01-01_to_2024-01-01" in processed_call.args[2]
