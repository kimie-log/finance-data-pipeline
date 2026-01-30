"""processing/factor_ranking 的單元測試：rank_stocks_by_factor、calculate_weighted_rank。"""
import pandas as pd
import pytest

from processing.factor_ranking import rank_stocks_by_factor, calculate_weighted_rank


def test_rank_stocks_by_factor_positive_corr():
    """正相關時小值排前（ascending=True）。"""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "value": [30.0, 10.0, 20.0],
    })
    result = rank_stocks_by_factor(
        factor_df=df,
        positive_corr=True,
        rank_column="value",
        rank_result_column="rank",
    )
    assert "rank" in result.columns
    # 10 < 20 < 30 → rank 1, 2, 3
    r = result.set_index("asset")["rank"]
    assert r["B"] == 1.0 and r["C"] == 2.0 and r["A"] == 3.0


def test_rank_stocks_by_factor_negative_corr():
    """負相關時大值排前（ascending=False）。"""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "value": [30.0, 10.0, 20.0],
    })
    result = rank_stocks_by_factor(
        factor_df=df,
        positive_corr=False,
        rank_column="value",
        rank_result_column="rank",
    )
    # 30 > 20 > 10 → rank 1, 2, 3
    r = result.set_index("asset")["rank"]
    assert r["A"] == 1.0 and r["C"] == 2.0 and r["B"] == 3.0


def test_rank_stocks_by_factor_per_date():
    """每日分別排名。"""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]),
        "asset": ["A", "B", "A", "B"],
        "value": [1.0, 2.0, 2.0, 1.0],
    })
    result = rank_stocks_by_factor(
        factor_df=df,
        positive_corr=True,
        rank_column="value",
        rank_result_column="rank",
    )
    d1 = result[result["datetime"] == "2024-01-01"].set_index("asset")["rank"]
    d2 = result[result["datetime"] == "2024-01-02"].set_index("asset")["rank"]
    assert d1["A"] == 1.0 and d1["B"] == 2.0
    assert d2["B"] == 1.0 and d2["A"] == 2.0


def test_calculate_weighted_rank_length_mismatch():
    """ranked_dfs 與 weights 長度不同時應拋出 ValueError。"""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01"]),
        "asset": ["A"],
        "rank": [1.0],
    })
    with pytest.raises(ValueError, match="長度須相同"):
        calculate_weighted_rank(
            ranked_dfs=[df],
            weights=[0.5, 0.5],
            positive_corr=True,
            rank_column="rank",
        )


def test_calculate_weighted_rank_single_factor():
    """單一因子加權排名等同該因子排名。"""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        "asset": ["A", "B"],
        "rank": [1.0, 2.0],
    })
    result = calculate_weighted_rank(
        ranked_dfs=[df],
        weights=[1.0],
        positive_corr=True,
        rank_column="rank",
    )
    assert set(result.columns) == {"datetime", "asset", "weighted_rank"}
    r = result.set_index("asset")["weighted_rank"]
    assert r["A"] == 1.0 and r["B"] == 2.0


def test_calculate_weighted_rank_two_factors():
    """兩因子加權加總後再排名。"""
    df1 = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "rank": [1.0, 2.0, 3.0],
    })
    df2 = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "rank": [3.0, 2.0, 1.0],
    })
    result = calculate_weighted_rank(
        ranked_dfs=[df1, df2],
        weights=[0.5, 0.5],
        positive_corr=True,  # 加權總分小者排前
        rank_column="rank",
    )
    assert set(result.columns) == {"datetime", "asset", "weighted_rank"}
    # A: 0.5*1 + 0.5*3 = 2, B: 2, C: 0.5*3 + 0.5*1 = 2 → 並列
    assert result["weighted_rank"].min() >= 1.0 and result["weighted_rank"].max() <= 3.0
