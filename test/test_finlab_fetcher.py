from unittest import mock

import pandas as pd
import pytest

from conftest import require_module


def _get_fetcher():
    # 確保 finlab 已安裝，避免測試在缺依賴時直接壞掉
    require_module("finlab", "pip install -r requirements.txt")
    from ingestion.finlab_fetcher import FinLabFetcher

    return FinLabFetcher


def test_finlab_login_uses_token():
    # 準備：mock 環境變數與 login
    FinLabFetcher = _get_fetcher()
    with mock.patch("ingestion.finlab_fetcher.os.getenv", return_value="token") as mock_getenv:
        with mock.patch("ingestion.finlab_fetcher.finlab.login") as mock_login:
            # 執行：登入流程
            FinLabFetcher.finlab_login()
            # 驗證：有讀取 token 並呼叫 login
            mock_getenv.assert_called_once()
            mock_login.assert_called_once_with("token")


def test_fetch_top_stocks_universe_basic():
    # 準備：公司與市值資料，驗證 universe 結構與排序
    FinLabFetcher = _get_fetcher()
    company_info = pd.DataFrame(
        {
            "stock_id": ["2330", "2317", "2603"],
            "公司名稱": ["A", "B", "C"],
            "上市日期": ["2000-01-01", "2010-01-01", "2015-01-01"],
            "產業類別": ["半導體", "電子", "航運"],
            "市場別": ["sii", "sii", "sii"],
        }
    )
    market_value = pd.DataFrame(
        {
            "2330": [10, 30],
            "2317": [20, 5],
            "2603": [5, 40],
        },
        index=pd.to_datetime(["2024-01-01", "2024-02-01"]),
    )

    def get_side_effect(key):
        if key == "company_basic_info":
            return company_info
        if key == "etl:market_value":
            return market_value
        raise KeyError(key)

    with mock.patch("ingestion.finlab_fetcher.data.get") as mock_get:
        mock_get.side_effect = get_side_effect

        universe = FinLabFetcher.fetch_top_stocks_universe(
            excluded_industry=["航運"],
            pre_list_date="2018-01-01",
            top_n=2,
            market_value_date="2024-02-01",
        )

        # 應只包含非航運且上市較早的前兩大市值
        assert list(universe["stock_id"]) == ["2330", "2317"]
        # 必要欄位存在
        for col in [
            "stock_id",
            "company_name",
            "list_date",
            "industry",
            "market",
            "market_value",
            "market_value_date",
            "rank",
            "top_n",
        ]:
            assert col in universe.columns
