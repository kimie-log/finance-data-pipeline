import pandas as pd

class BaseFetcher:
    source: str

    def fetch(self) -> pd.DataFrame:
        raise NotImplementedError

    def save_local(self, df: pd.DataFrame, path: str):
        df.to_parquet(path, index=False)
        print(f"Data saved locally at {path}")