"""
依因子排名做多／做空的策略：排名高者做多、排名低者做空。

用於多因子加權排名回測，資料需含 rank 線（由 strategies.data_feed.PandasDataWithRank 提供）。
"""

from __future__ import annotations

from backtrader import Strategy


class FactorRankStrategy(Strategy):
    """
    根據因子排名買入與賣出股票。

    參數：
        buy_n: 做多檔數（取排名最高的 buy_n 檔）
        sell_n: 做空檔數（取排名最低的 sell_n 檔）
        each_cash: 每檔股票分配金額（買入／賣出時以此計算股數）
    """

    params = (
        ("buy_n", None),
        ("sell_n", None),
        ("each_cash", None),
    )

    def __init__(self):
        self.stocks = self.datas
        self.buy_positions = set()
        self.sell_positions = set()

    def next(self):
        # 當日有效排名（排除缺漏標記 999999）
        ranks = {
            data._name: data.rank[0]
            for data in self.stocks
            if data.rank[0] != 999999
        }
        sorted_ranks = sorted(ranks.items(), key=lambda x: x[1])

        buy_n_names = []
        if self.params.buy_n:
            buy_n_list = sorted_ranks[-self.params.buy_n :]
            buy_n_names = [name for name, _ in buy_n_list]

        sell_n_names = []
        if self.params.sell_n:
            sell_n_list = sorted_ranks[: self.params.sell_n]
            sell_n_names = [name for name, _ in sell_n_list]

        for data in self.stocks:
            name = data._name
            close_price = data.close[0]
            size = int(self.params.each_cash / close_price) if self.params.each_cash else 0
            
            if self.params.sell_n:
                if name in self.sell_positions and name not in sell_n_names:
                    self.close(data)
                    self.sell_positions.discard(name)
                elif name not in self.sell_positions and name in sell_n_names:
                    self.sell(data, size=size)
                    self.sell_positions.add(name)

            if self.params.buy_n:
                if name in self.buy_positions and name not in buy_n_names:
                    self.close(data)
                    self.buy_positions.discard(name)
                elif name not in self.buy_positions and name in buy_n_names:
                    self.buy(data, size=size)
                    self.buy_positions.add(name)
