import numpy as np
import pandas as pd

from processing.transformer import Transformer


def test_process_market_data_basic():
    index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
    df_raw = pd.DataFrame(
        {
            "2330": [100.0, np.nan],
            "2317": [np.nan, 50.0],
        },
        index=index,
    )

    result = Transformer.process_market_data(df_raw)

    assert set(result.columns) == {"date", "stock_id", "close", "daily_return"}
    assert result["close"].dtype == np.float32
    assert result["date"].dtype.kind == "M"
    assert len(result) == 3

    close_2330 = result[result["stock_id"] == "2330"].sort_values("date")["close"].tolist()
    assert close_2330 == [100.0, 100.0]
