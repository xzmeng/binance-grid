import pandas as pd
import json
from datetime import datetime, timedelta

class Analyzer:
    def __init__(self) -> None:
        filled_orders = db.getDb("filled_orders.json")
        orders = filled_orders.getAll()
        self.data = pd.DataFrame(orders, columns=["s", "p", "q", "Z", "S", "O", "T"])
        self.symbols = self.data.s.unique()

    def analyze_symbol(self, symbol):
        data = self.data.query("s == @symbol")
        data.reset_index(drop=True, inplace=True)
        data.columns = [
            "symbol",
            "price",
            "base_quantity",
            "quote_quantity",
            "side",
            "created_at",
            "filled_at",
        ]
        data = data.astype(
            {
                "symbol": str,
                "price": float,
                "base_quantity": float,
                "quote_quantity": float,
                "side": str,
                "created_at": int,
                "filled_at": int,
            }
        )
        data["created_at"] = pd.to_datetime(
            data["created_at"], unit="ms", utc=True
        ).dt.tz_convert("Asia/Shanghai")
        data["filled_at"] = pd.to_datetime(
            data["filled_at"], unit="ms", utc=True
        ).dt.tz_convert("Asia/Shanghai")

        data["buy"] = 1
        data.loc[data.side == "SELL", "buy"] = -1
        data["trade_base"] = data.buy * data.base_quantity
        data["trade_quote"] = data.buy * data.quote_quantity * (-1)
        data["trade_comm"] = data.quote_quantity * 0.001
        data["base"] = data.trade_base.cumsum()
        data["quote"] = data.trade_quote.cumsum()
        data["comm"] = data.trade_comm.cumsum()

        buy_indices = []
        sell_indices = []
        trade_pairs = []
        for i, side in enumerate(data.side):
            if side == "BUY":
                if sell_indices:
                    trade_pairs.append((i, sell_indices.pop()))
                else:
                    buy_indices.append(i)
            else:
                if buy_indices:
                    trade_pairs.append((buy_indices.pop(), i))
                else:
                    sell_indices.append(i)

        data["trade_base_profit"] = 0
        data["trade_quote_profit"] = 0

        for trade_pair in trade_pairs:
            open_index, close_index = min(trade_pair), max(trade_pair)
            open_trade = data.iloc[open_index]
            close_trade = data.iloc[close_index]
            trade_base_profit = close_trade.trade_base + open_trade.trade_base
            trade_quote_profit = close_trade.trade_quote + open_trade.trade_quote
            data.loc[close_index, "trade_base_profit"] = trade_base_profit
            trade_comm = (
                data.loc[open_index, "trade_comm"] + data.loc[close_index, "trade_comm"]
            )
            data.loc[close_index, "trade_quote_profit"] = (
                trade_quote_profit - trade_comm
            )

        data["base_profit"] = data.trade_base_profit.cumsum()
        data["quote_profit"] = data.trade_quote_profit.cumsum()

        data["profit_in_quote"] = data.base_profit * data.price + data.quote_profit
        data["value_change_in_quote"] = data.base * data.price + data.quote - data.comm

        del data["base_quantity"]
        del data["quote_quantity"]
        del data["created_at"]
        del data["buy"]
        data = data.reindex(
            columns=[
                "price",
                "side",
                "filled_at",
                "trade_base",
                "trade_quote",
                "trade_comm",
                "base",
                "quote",
                "comm",
                "trade_base_profit",
                "base_profit",
                "trade_quote_profit",
                "quote_profit",
                "profit_in_quote",
                "value_change_in_quote",
            ]
        )
        data.set_index("filled_at", inplace=True)
        return data

    def all_symbols(self):
        return [self.analyze_symbol(symbol) for symbol in self.symbols]

    def one_table(self, symbol):
        return self.analyze_symbol(symbol).to_html()

    def last_or_0(self, s):
        return 0 if s.empty else s[-1]

    def one_info(self, symbol):
        result = self.analyze_symbol(symbol)

        if len(result) == 0:
            days = hours = minutes = 0
        else:
            td = result.index[-1] - result.index[0]
            days, seconds = td.days, td.seconds
            mins = seconds // 60
            hours = mins // 60
            minutes = mins % 60

        dt = datetime.now() - timedelta(hours=1)
        one_hour = result[dt:]
        info = {
            "buy": sum(result.side == "BUY"),
            "sell": sum(result.side == "SELL"),
            "buy_1hour": sum(one_hour.side == "BUY"),
            "sell_1hour": sum(one_hour.side == 'SELL'),
            "profit": round(self.last_or_0(result.profit_in_quote), 2),
            "value change": round(self.last_or_0(result.value_change_in_quote), 2),
            "time": f"{days} days, {hours} hours, {minutes} minutes",
        }
        return info

    def info(self):
        info_dict = {symbol: self.one_info(symbol) for symbol in self.symbols}
        with open("config.json") as f:
            config = json.load(f)
        for symbol, info in info_dict.items():
            if symbol in config:
                info["step"] = config[symbol].get("step", "default")
                info["quote"] = config[symbol].get("quote", "default")
                info["status"] = "trading"
            else:
                info["status"] = "not trading"

        all_info = {}
        for key in ["buy", "sell", "profit", "value change"]:
            all_info[key] = round(sum(info[key] for info in info_dict.values()), 2)
        info_dict["ALL"] = all_info
        return info_dict

    def run(self):
        r = self.all_symbols()
        print(r)
