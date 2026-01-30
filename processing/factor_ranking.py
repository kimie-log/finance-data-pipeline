"""
因子排名與加權排名，供回測／選股使用。
對齊 utils_for_referance.rank_stocks_by_factor、calculate_weighted_rank。
欄位命名：datetime=日期、asset=股票代碼、value=因子值、rank=排名。
"""
from typing import Annotated, List

import pandas as pd


def rank_stocks_by_factor(
    factor_df: Annotated[
        pd.DataFrame,
        "欄位含 datetime(日期)、asset(股票代碼)、value(因子值)",
    ],
    positive_corr: Annotated[bool, "因子與收益正相關=True，負相關=False"],
    rank_column: Annotated[str, "用來排序的欄位"] = "value",
    rank_result_column: Annotated[str, "排名結果欄位名"] = "rank",
) -> pd.DataFrame:
    """
    依因子值對股票做每日排名。正相關時小值排前；負相關時大值排前。
    對齊 utils_for_referance.rank_stocks_by_factor。
    """
    ranked = factor_df.copy()
    ranked = ranked.set_index("datetime")
    ranked[rank_result_column] = (
        ranked.groupby(level="datetime", sort=False)[rank_column]
        .rank(ascending=positive_corr)
        .fillna(0)
    )
    return ranked.reset_index()


def calculate_weighted_rank(
    ranked_dfs: Annotated[
        List[pd.DataFrame],
        "多個已排名的因子表，欄位含 datetime、asset、rank",
    ],
    weights: Annotated[List[float], "各因子權重，長度與 ranked_dfs 相同"],
    positive_corr: bool,
    rank_column: Annotated[str, "排名欄位名"] = "rank",
) -> pd.DataFrame:
    """
    多因子加權排名：先加權加總再排名，回傳 datetime、asset、weighted_rank。
    對齊 utils_for_referance.calculate_weighted_rank。
    """
    if len(ranked_dfs) != len(weights):
        raise ValueError("ranked_dfs 與 weights 長度須相同。")
    combined = None
    for i, df in enumerate(ranked_dfs):
        part = df[["datetime", "asset"]].copy()
        part[f"rank_{i}"] = df[rank_column] * weights[i]
        if combined is None:
            combined = part
        else:
            combined = combined.merge(
                part,
                on=["datetime", "asset"],
                how="outer",
            )
    combined = combined.dropna()
    combined["weighted"] = combined.filter(like="rank_").sum(axis=1)
    ranked = rank_stocks_by_factor(
        combined.reset_index(drop=True).rename(columns={"weighted": "value"}),
        positive_corr=positive_corr,
        rank_column="value",
        rank_result_column="weighted_rank",
    )
    return ranked[["datetime", "asset", "weighted_rank"]]
