import os
import pandas as pd
from finlab import data, login
import finlab



class FinLabFetcher:
    @staticmethod
    def finlab_login():
        token = os.getenv("FINLAB_API_TOKEN")
        finlab.login(token)