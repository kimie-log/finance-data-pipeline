import yfinance as yf
import pandas as pd

class YFinanceFetcher:
    @staticmethod
    def fetch(ticker, start, end):
        df = yf.download(ticker, start=start, end=end)
        return df
    
    
