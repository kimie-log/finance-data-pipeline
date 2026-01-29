import pandas as pd

from conftest import require_any_module
from ingestion.base_fetcher import BaseFetcher


def test_save_local_writes_parquet(tmp_path):
    require_any_module(["pyarrow", "fastparquet"], "pip install -r requirements.txt")
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = tmp_path / "data.parquet"

    BaseFetcher().save_local(df, str(path))

    assert path.exists()
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, df)
