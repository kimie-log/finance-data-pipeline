import pandas as pd

'''
抽象化抓取介面，讓不同資料來源共用相同流程
'''

class BaseFetcher:
    source: str

    def fetch(self) -> pd.DataFrame:
        raise NotImplementedError

    def save_local(self, df: pd.DataFrame, path: str):
        # 統一使用 Parquet
        df.to_parquet(path, index=False)
        print(f"Data saved locally at {path}")