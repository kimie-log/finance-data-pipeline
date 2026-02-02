"""
Backtrader 自訂資料格式：在 OHLCV 上新增因子排名線。

供多因子回測使用，資料欄位需含 datetime, Open, High, Low, Close, Volume, rank。
"""

from __future__ import annotations

from backtrader.feeds import PandasData


class PandasDataWithRank(PandasData):
    """
    回測資料格式：在 PandasData 基礎上新增 rank 線。

    參數對應欄位：
        datetime -> datetime
        open -> Open
        high -> High
        low -> Low
        close -> Close
        volume -> Volume
        rank -> rank（因子排名，缺漏時可用 999999 表示排除）
        openinterest -> 不使用 (-1)
    """

    params = (
        ("datetime", "datetime"),
        ("open", "Open"),
        ("high", "High"),
        ("low", "Low"),
        ("close", "Close"),
        ("volume", "Volume"),
        ("rank", "rank"),
        ("openinterest", -1),
    )
    lines = ("rank",)
