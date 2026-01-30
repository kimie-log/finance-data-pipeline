from unittest import mock

import pandas as pd

import scripts.run_etl_pipeline as pipeline


def test_requires_market_value_date():
    # 缺少市值日期時應直接回傳錯誤
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
    # interval 模式需帶入市值基準日期並呼叫 FinLab
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
                                        # Fact + Universe 兩次 BigQuery 寫入
                                        assert mock_bq.call_count == 2


def test_skip_gcs_does_not_upload():
    # skip_gcs=True 時不應觸發任何上傳
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
    # 應上傳到 data/raw/interval 與 data/processed/interval
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
                                            assert "data/raw/interval/" in raw_call.args[2]
                                            assert "data/processed/interval/" in processed_call.args[2]


def test_gcs_upload_paths_interval_mode():
    # interval 模式應上傳到 data/raw/interval 與 data/processed/interval
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
                                            assert "data/raw/interval/" in raw_call.args[2]
                                            assert "data/processed/interval/" in processed_call.args[2]


def test_bigquery_naming_interval_mode():
    # interval 模式的 BigQuery 命名規則驗證
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
                                        assert fact_kwargs["dataset_id"] == "my_dataset_interval"
                                        assert (
                                            fact_kwargs["table_id"]
                                            == "fact_price_mv20240115_s20200101_e20240101_top30"
                                        )
                                        assert uni_kwargs["dataset_id"] == "my_dataset_interval"
                                        assert (
                                            uni_kwargs["table_id"]
                                            == "dim_universe_mv20240115_top30"
                                        )


def test_interval_filenames_include_date_range():
    # interval 模式檔名應包含日期區間字串
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
