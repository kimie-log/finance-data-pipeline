"""ingestion/finlab_factor_fetcher 的單元測試：季頻展開、季度對應、因子取得與查詢。"""
import sys
from unittest import mock

import pandas as pd
import pytest

from conftest import require_module

# 未安裝 finlab 時仍可載入模組（mock 後再 import）
if "finlab" not in sys.modules:
    _finlab_mock = mock.MagicMock()
    _finlab_mock.data = mock.MagicMock()
    sys.modules["finlab"] = _finlab_mock

from ingestion.finlab_factor_fetcher import (
    extend_factor_data,
    get_factor_data,
    convert_quarter_to_dates,
    convert_date_to_quarter,
    list_factors_by_type,
    fetch_factors_daily,
)


def test_extend_factor_data_ffill():
    """季頻展開至交易日：向前填補，只保留交易日區間內。"""
    factor_data = pd.DataFrame({
        "index": pd.to_datetime(["2024-01-01", "2024-01-10"]),
        "2330": [100.0, 110.0],
        "2317": [50.0, 55.0],
    })
    trading_days = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-10"]))
    result = extend_factor_data(factor_data, trading_days)
    assert len(result) == 4
    assert result["index"].min() == pd.Timestamp("2024-01-01")
    assert result["index"].max() == pd.Timestamp("2024-01-10")
    # 1/2, 1/3 應向前填補 1/1 的值
    assert (result[result["index"] == pd.Timestamp("2024-01-02")]["2330"].values == 100.0).all()
    assert (result[result["index"] == pd.Timestamp("2024-01-10")]["2330"].values == 110.0).all()


def test_convert_quarter_to_dates():
    """季度字串轉財報揭露區間日期。"""
    assert convert_quarter_to_dates("2013-Q1") == ("2013-05-16", "2013-08-14")
    assert convert_quarter_to_dates("2013-Q2") == ("2013-08-15", "2013-11-14")
    assert convert_quarter_to_dates("2013-Q3") == ("2013-11-15", "2014-03-31")
    assert convert_quarter_to_dates("2013-Q4") == ("2014-04-01", "2014-05-15")


def test_convert_date_to_quarter():
    """日期轉台灣財報季度字串。"""
    assert convert_date_to_quarter("2013-05-16") == "2013-Q1"
    assert convert_date_to_quarter("2013-08-14") == "2013-Q1"
    assert convert_date_to_quarter("2013-08-15") == "2013-Q2"
    assert convert_date_to_quarter("2013-11-15") == "2013-Q3"
    assert convert_date_to_quarter("2014-03-31") == "2013-Q3"
    assert convert_date_to_quarter("2014-04-01") == "2013-Q4"
    assert convert_date_to_quarter("2014-05-15") == "2013-Q4"


def test_get_factor_data_without_trading_days_returns_raw():
    """未給 trading_days 時回傳原始季頻表（需 mock data.get）。"""
    raw = pd.DataFrame(
        {"2330": [100.0], "2317": [50.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
    )
    raw.index.name = "index"

    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.get.return_value.deadline.return_value = raw
        result = get_factor_data(
            stock_symbols=["2330", "2317"],
            factor_name="營業利益",
            trading_days=None,
        )
        mock_data.get.assert_called_once()
        assert "2330" in result.columns and "2317" in result.columns
        assert len(result) == 1


def test_get_factor_data_with_trading_days_returns_long_format():
    """給 trading_days 時回傳 long format (datetime, asset, value)。"""
    raw = pd.DataFrame(
        {"2330": [100.0], "2317": [50.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
    )
    raw.index.name = "index"
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])

    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.get.return_value.deadline.return_value = raw
        result = get_factor_data(
            stock_symbols=["2330", "2317"],
            factor_name="營業利益",
            trading_days=trading_days,
        )
        assert set(result.columns) == {"datetime", "asset", "value"}
        assert len(result) == 4  # 2 days x 2 assets
        assert set(result["asset"]) == {"2330", "2317"}


def test_list_factors_by_type():
    """依資料型態列出因子名稱（mock data.search）。"""
    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.search.return_value = [{"items": ["營業利益", "營業收入", "淨利"]}]
        result = list_factors_by_type("fundamental_features")
        mock_data.search.assert_called_once()
        assert result == ["營業利益", "營業收入", "淨利"]


def test_fetch_factors_daily_empty_input():
    """factor_names 或 stock_ids 為空時回傳空 DataFrame。"""
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
    empty = fetch_factors_daily(
        stock_ids=[],
        factor_names=["營業利益"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        trading_days=trading_days,
    )
    assert empty.empty
    assert list(empty.columns) == ["date", "stock_id", "factor_name", "value"]

    empty2 = fetch_factors_daily(
        stock_ids=["2330"],
        factor_names=[],
        start_date="2024-01-01",
        end_date="2024-01-02",
        trading_days=trading_days,
    )
    assert empty2.empty


def test_fetch_factors_daily_columns():
    """fetch_factors_daily 回傳欄位為 date, stock_id, factor_name, value。"""
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
    long_df = pd.DataFrame({
        "datetime": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        "asset": ["2330", "2317"],
        "value": [100.0, 50.0],
    })

    with mock.patch("ingestion.finlab_factor_fetcher.get_factor_data", return_value=long_df):
        result = fetch_factors_daily(
            stock_ids=["2330", "2317"],
            factor_names=["營業利益"],
            start_date="2024-01-01",
            end_date="2024-01-02",
            trading_days=trading_days,
        )
        assert set(result.columns) == {"date", "stock_id", "factor_name", "value"}
        assert (result["factor_name"] == "營業利益").all()
