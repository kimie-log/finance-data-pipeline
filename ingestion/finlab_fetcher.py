"""
FinLab 資料抓取與登入模組

封裝 FinLab API 登入與市值 Top N universe 取得邏輯，
產出欄位與型別符合 BigQuery 維度表與後續 ETL 使用
依賴環境變數 FINLAB_API_TOKEN（建議於 .env 設定）
"""
from __future__ import annotations

import os
from typing import Annotated, List

import pandas as pd
from finlab import data, login
import finlab


class FinLabFetcher:
    """
    FinLab 登入與 universe 抓取

    所有方法為 staticmethod，無需實例化
    登入後方可呼叫 data.get(...)；token 由環境變數讀取
    """

    @staticmethod
    def finlab_login() -> None:
        """
        使用環境變數 FINLAB_API_TOKEN 登入 FinLab，供後續 data.get 使用

        實務：於專案根目錄 .env 設定 FINLAB_API_TOKEN，避免互動輸入驗證碼
        未設定時拋出 ValueError，並提示取得 token 的網址
        """
        token = os.getenv("FINLAB_API_TOKEN")
        if not token:
            raise ValueError(
                "未設定 FinLab 驗證碼。請在 .env 中設定 FINLAB_API_TOKEN ）"
            )
        finlab.login(token)

    @staticmethod
    def fetch_top_stocks_universe(
        excluded_industry: Annotated[List[str], "要排除的產業類別列表，例：['金融業','建材營造']"] = [],
        pre_list_date: Annotated[str | None, "上市日期須早於此日 (YYYY-MM-DD)，僅篩上市"] = None,
        top_n: Annotated[int | None, "市值前 N 大筆數，必填"] = None,
        market_value_date: Annotated[str | None, "市值基準日 (YYYY-MM-DD)，用於選股與可重現回測"] = None,
    ) -> pd.DataFrame:
        """
        取得指定市值日的市值前 N 大股票 universe，供 BigQuery dim_universe 與後續 ETL 使用

        Args:
            excluded_industry: 排除的產業類別（FinLab 產業類別欄位）
            pre_list_date: 僅保留上市日期早於此日的股票；給定時會限定市場別為 sii（上市）
            top_n: 取市值前 N 筆；未給時會拋出 ValueError
            market_value_date: 市值基準日；若給定則取該日或之前最近一筆有資料的日期，未給則用最新日

        Returns:
            DataFrame 欄位含：stock_id, company_name, list_date, industry, market,
            market_value, market_value_date, rank, top_n, delist_date（若 FinLab 有提供）
            日期欄位已轉成 YYYY-MM-DD 字串或 None，以利 BigQuery / PyArrow 寫入

        Raises:
            ValueError: 未設定 top_n，或 market_value_date 之前無市值資料時
        """
        basic_cols = ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
        company_info = data.get("company_basic_info")
        if "下市日期" in company_info.columns:
            basic_cols = basic_cols + ["下市日期"]
        company_info = company_info[basic_cols]

        # 排除特定產業
        if excluded_industry:
            company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]

        # 僅保留上市日期早於 pre_list_date 的股票與市場別為 sii 的股票
        if pre_list_date:
            company_info = company_info[company_info["市場別"] == "sii"]
            company_info = company_info[company_info["上市日期"] < pre_list_date]

        if not top_n:
            raise ValueError("top_n is required when building universe.")

        # 取得市值資料
        df_market_value = data.get("etl:market_value").copy()
        df_market_value.index = pd.to_datetime(df_market_value.index)

        # 若給定市值基準日，則取該日或之前最近一筆有資料的日期
        if market_value_date:
            target_date = pd.to_datetime(market_value_date)
            candidate_dates = df_market_value.index[df_market_value.index <= target_date]
            if candidate_dates.empty:
                raise ValueError(f"No market value data before {market_value_date}")
            selected_date = candidate_dates.max()
        else:
            selected_date = df_market_value.index.max()

        latest_market_value = (
            df_market_value.loc[selected_date]
            .rename("market_value")
            .reset_index()
        )
        latest_market_value.columns = ["stock_id", "market_value"]

        # 合併市值資料與公司資料
        universe = pd.merge(latest_market_value, company_info, on="stock_id")
        # 按市值排序並取前 top_n 筆
        universe = universe.sort_values(by="market_value", ascending=False).head(top_n)
        # 重置索引
        universe = universe.reset_index(drop=True)
        universe["market_value_date"] = selected_date.strftime("%Y-%m-%d")
        universe["rank"] = universe.index + 1
        universe["top_n"] = top_n

        rename_map = {
            "公司名稱": "company_name",
            "上市日期": "list_date",
            "產業類別": "industry",
            "市場別": "market",
        }
        if "下市日期" in universe.columns:
            rename_map["下市日期"] = "delist_date"
        universe = universe.rename(columns=rename_map)
        if "delist_date" not in universe.columns:
            universe["delist_date"] = None
        else:
            universe["delist_date"] = universe["delist_date"].replace({pd.NA: None})

        # 日期欄位統一為 YYYY-MM-DD 字串或 None，避免 PyArrow/BigQuery 寫入時型別問題
        for date_col in ["list_date", "delist_date"]:
            if date_col in universe.columns:
                universe[date_col] = universe[date_col].apply(
                    lambda x: None if pd.isna(x) or x is None else (x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
                )

        return universe
