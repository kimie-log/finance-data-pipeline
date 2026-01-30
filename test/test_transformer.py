import numpy as np
import pandas as pd

from processing.transformer import Transformer


def test_process_ohlcv_data_basic():
    # 準備：兩天兩檔，含缺失值的 OHLCV long format
    df_raw = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
            "asset": ["2330", "2330", "2317"],
            "open": [100.0, None, 50.0],
            "high": [101.0, None, 51.0],
            "low": [99.0, None, 49.0],
            "close": [100.0, None, 50.0],
            "volume": [1000, None, 500],
        }
    )

    result = Transformer.process_ohlcv_data(df_raw)

    # 欄位與型別驗證
    expected_cols = {
        "date",
        "stock_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "is_suspended",
        "is_limit_up",
        "is_limit_down",
    }
    assert set(result.columns) == expected_cols
    assert result["date"].dtype.kind == "M"

    # 缺失值應已補齊，volume 缺失補為 0 或原值
    assert not result["close"].isna().any()
