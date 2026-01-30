"""
FinLab 財報／基本面因子抓取，季頻展開至日頻後供 BigQuery fact_factor 使用。
邏輯對齊 utils_for_referance.get_factor_data / extend_factor_data；
季與日期對應對齊 convert_quarter_to_dates / convert_date_to_quarter。
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, List, Tuple

import pandas as pd
from finlab import data


def extend_factor_data(
    factor_data: pd.DataFrame,
    trading_days: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    將季頻因子表展開至交易日頻率，使用向前填補。
    對齊 utils_for_referance.extend_factor_data。
    factor_data 須含欄位 index（日期）與各股票代碼欄位。
    """
    trading_days_df = pd.DataFrame(trading_days, columns=["index"])
    extended = trading_days_df.merge(factor_data, on="index", how="outer")
    extended = extended.ffill()
    extended = extended[
        (extended["index"] >= trading_days_df["index"].min())
        & (extended["index"] <= trading_days_df["index"].max())
    ]
    return extended


def get_factor_data(
    stock_symbols: Annotated[List[str], "股票代碼列表"],
    factor_name: Annotated[str, "因子名稱，對應 fundamental_features:*"],
    trading_days: Annotated[pd.DatetimeIndex | None, "交易日序列；若給定則展開為日頻"] = None,
) -> pd.DataFrame:
    """
    從 FinLab 取得單一因子，可選展開至交易日頻率。
    對齊 utils_for_referance.get_factor_data。
    有 trading_days 時回傳 long format (datetime, asset, value)；否則回傳原始季頻表。
    """
    raw = data.get(f"fundamental_features:{factor_name}").deadline()
    if stock_symbols:
        cols = [c for c in raw.columns if c in stock_symbols]
        raw = raw[cols] if cols else raw
    if trading_days is None:
        return raw
    raw = raw.reset_index().rename(columns={"index": "date"})
    raw["date"] = pd.to_datetime(raw["date"])
    raw_index = raw.rename(columns={"date": "index"})
    extended = extend_factor_data(raw_index, trading_days)
    extended = extended.rename(columns={"index": "datetime"})
    melted = extended.melt(id_vars="datetime", var_name="asset", value_name="value")
    return melted.sort_values(["datetime", "asset"]).reset_index(drop=True)


def fetch_factors_daily(
    stock_ids: Annotated[List[str], "股票代碼列表"],
    factor_names: Annotated[List[str], "因子名稱，對應 fundamental_features:*"],
    start_date: str,
    end_date: str,
    trading_days: Annotated[pd.DatetimeIndex, "交易日序列，用於展開季頻至日頻"],
) -> pd.DataFrame:
    """
    取得多個因子的日頻資料（向前填補），輸出 long format (date, stock_id, factor_name, value) 供 BigQuery。
    內部呼叫 get_factor_data + extend_factor_data 邏輯。
    """
    if not factor_names or not stock_ids:
        return pd.DataFrame(columns=["date", "stock_id", "factor_name", "value"])

    trading_days = trading_days[(trading_days >= start_date) & (trading_days <= end_date)]
    rows = []
    for factor_name in factor_names:
        try:
            df = get_factor_data(stock_symbols=stock_ids, factor_name=factor_name, trading_days=trading_days)
        except Exception:
            continue
        if df.empty:
            continue
        df = df.rename(columns={"datetime": "date", "asset": "stock_id"})
        df["factor_name"] = factor_name
        df["date"] = pd.to_datetime(df["date"])
        rows.append(df[["date", "stock_id", "factor_name", "value"]])
    if not rows:
        return pd.DataFrame(columns=["date", "stock_id", "factor_name", "value"])
    return pd.concat(rows, ignore_index=True)


def convert_quarter_to_dates(
    quarter: Annotated[str, "年-季度字串，例如 2013-Q1"],
) -> Annotated[Tuple[str, str], "該季財報揭露區間 (start, end)"]:
    """
    將季度字串轉為財報揭露區間日期。對齊 utils_for_referance.convert_quarter_to_dates。
    ex: 2013-Q1 -> 2013-05-16, 2013-08-14
    """
    year, qtr = quarter.split("-")
    if qtr == "Q1":
        return f"{year}-05-16", f"{year}-08-14"
    if qtr == "Q2":
        return f"{year}-08-15", f"{year}-11-14"
    if qtr == "Q3":
        return f"{year}-11-15", f"{int(year) + 1}-03-31"
    if qtr == "Q4":
        return f"{int(year) + 1}-04-01", f"{int(year) + 1}-05-15"
    raise ValueError(f"Invalid quarter: {quarter}")


def convert_date_to_quarter(
    date: Annotated[str, "日期字串 YYYY-MM-DD"],
) -> Annotated[str, "對應季度字串"]:
    """
    將日期轉為台灣財報季度字串。對齊 utils_for_referance.convert_date_to_quarter。
    ex: 2013-05-16 -> 2013-Q1
    """
    d = datetime.strptime(date, "%Y-%m-%d").date()
    y, m, day = d.year, d.month, d.day
    if m == 5 and day >= 16 or m in (6, 7) or (m == 8 and day <= 14):
        return f"{y}-Q1"
    if m == 8 and day >= 15 or m in (9, 10) or (m == 11 and day <= 14):
        return f"{y}-Q2"
    if m == 11 and day >= 15 or m == 12:
        return f"{y}-Q3"
    if m in (1, 2) or (m == 3 and day <= 31):
        return f"{y - 1}-Q3"
    if m == 4 or (m == 5 and day <= 15):
        return f"{y - 1}-Q4"
    return f"{y}-Q1"


def list_factors_by_type(
    data_type: Annotated[str, "資料型態，例如 fundamental_features"],
) -> Annotated[List[str], "該型態下所有因子名稱"]:
    """
    依資料型態列出 FinLab 因子名稱。對齊 utils_for_referance.list_factors_by_type。
    """
    result = data.search(keyword=data_type, display_info=["name", "description", "items"])
    return list(result[0]["items"]) if result else []
