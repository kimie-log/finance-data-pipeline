from unittest import mock

import pandas as pd

from conftest import require_module


def _get_fetcher():
    # 確保 yfinance 已安裝，避免測試在缺依賴時直接壞掉
    require_module("yfinance", "pip install -r requirements.txt")
    from ingestion.yfinance_fetcher import YFinanceFetcher

    return YFinanceFetcher


def test_fetch_daily_ohlcv_data_long_format():
    # 準備：模擬兩檔股票的 OHLCV 多重欄位資料
    YFinanceFetcher = _get_fetcher()
    with mock.patch("ingestion.yfinance_fetcher.yf.download") as mock_download:
        index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
        cols = pd.MultiIndex.from_product(
            [["Open", "High", "Low", "Close", "Volume"], ["2330.TW"]],
            names=[None, "Ticker"],
        )
        data = [
            [100, 101, 99, 100, 1000],
            [101, 102, 100, 101, 1100],
        ]
        mock_download.return_value = pd.DataFrame(data, index=index, columns=cols)

        result = YFinanceFetcher.fetch_daily_ohlcv_data(
            stock_symbols=["2330"],
            start_date="2024-01-01",
            end_date="2024-01-03",
            is_tw_stock=True,
        )

        # 驗證：欄位名稱與必要欄位存在，且為 long format
        assert set(result.columns) == {
            "open",
            "high",
            "low",
            "close",
            "volume",
            "datetime",
            "asset",
        }
        assert set(result["asset"]) == {"2330"}


def test_fetch_benchmark_daily():
    """基準指數日收盤與日報酬：欄位為 date, index_id, close, daily_return。"""
    YFinanceFetcher = _get_fetcher()
    index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
    raw = pd.DataFrame(
        {"Close": [100.0, 101.0]},
        index=index,
    )
    with mock.patch("ingestion.yfinance_fetcher.yf.download", return_value=raw):
        result = YFinanceFetcher.fetch_benchmark_daily(
            index_ids=["^TWII"],
            start_date="2024-01-01",
            end_date="2024-01-03",
        )
    assert set(result.columns) == {"date", "index_id", "close", "daily_return"}
    assert result["index_id"].iloc[0] == "TWII"
    assert len(result) == 2
