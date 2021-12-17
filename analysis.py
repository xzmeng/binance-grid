#%%
import os
import pandas as pd
import json
from tinydb import TinyDB
from datetime import datetime, timedelta
from binance import Client
from dotenv import load_dotenv

load_dotenv()

client = Client(
    api_key=os.environ.get("API_KEY"), api_secret=os.environ.get("API_SECRET")
)


class Analyzer:
    df_cache = {}
    filled_count = 0

    def __init__(self) -> None:
        filled = TinyDB("db.json").table("filled")
        if len(filled) == len(Analyzer.df_cache):
            self.df_cache = Analyzer.df_cache
        else:
            Analyzer.last_count = len(filled)
            Analyzer.df_cache = self.df_cache = {}
        self.data = pd.DataFrame(
            filled.all(), columns=["s", "p", "q", "Z", "S", "O", "T", "cancel"]
        )
        self.symbols = self.data.s.unique()
        self.prices = {}

    def all_states_table(self):
        df = self.all_states_df()
        df["last_trade_time"] = df.last_trade_time.dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ).str[:-3]
        return df.to_html()

    def symbol_state_table(self, symbol):
        state = self.one_symbol_state(symbol)
        state["last_trade_time"] = state["last_trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
        state = pd.DataFrame.from_records([state])
        return state.to_html()

    def symbol_trades_table(self, symbol):
        df = self.one_symbol_df(symbol)
        df.insert(2, "filled_at", df.index)
        df["created_at"] = df.created_at.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]
        df["filled_at"] = df.filled_at.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]
        return df.reset_index(drop=True).to_html()

    def all_states_df(self):
        df = pd.DataFrame.from_dict(self.all_states(), orient="index")
        df.loc["ALL", "value_change":"profit"] = df.loc[:, "value_change":"profit"].sum()
        df.loc["ALL", "buy":"sell_last_hour"] = df.loc[:, "buy":"sell_last_hour"].sum()
        df.loc["ALL", "last_trade_time"] = df.last_trade_time.max()
        df = df.astype({
            'buy': int,
            'sell': int,
            'buy_last_hour': int,
            'sell_last_hour': int,
        })
        return df

    def all_states(self):
        res = client.get_all_tickers()
        self.prices = {item['symbol']: float(item['price']) for item in res}
        return {s: self.one_symbol_state(s) for s in self.symbols}

    def one_symbol_state(self, symbol):
        df = self.one_symbol_df(symbol)
        last_trade = df.iloc[-1]
        one_hour_ago = pd.Timestamp.utcnow() - pd.Timedelta(1, "h")
        df_last_hour = df[one_hour_ago:]
        current_price = self.prices.get(
            symbol, float(client.get_ticker(symbol=symbol).get("lastPrice"))
        )
        value_change = (
            last_trade.base * current_price + last_trade.quote - last_trade.comm
        )
        state = {
            "value_change": value_change,
            "profit": round(last_trade.profit_in_quote, 2),
            "last_trade_time": df.index[-1],
            "last_trade_price": last_trade.price,
            "current_price": str(current_price).rstrip("0"),
            "buy": sum(df.side == "BUY"),
            "sell": sum(df.side == "SELL"),
            "buy_last_hour": sum(df_last_hour.side == "BUY"),
            "sell_last_hour": sum(df_last_hour.side == "SELL"),
        }
        return state

    def one_symbol_table(self, symbol):
        df = self.one_symbol_df(symbol)
        return df.to_html()

    def one_symbol_df(self, symbol):
        if symbol in self.df_cache:
            return self.df_cache[symbol]

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
            "cancel",
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
        s = pd.Series("", index=data.index)
        s[data.cancel == True] = "*"
        data["cancel"] = s

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
        del data["buy"]
        data = data.reindex(
            columns=[
                "cancel",
                "created_at",
                "symbol",
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
        self.df_cache[symbol] = data
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
            "sell_1hour": sum(one_hour.side == "SELL"),
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


# # Analyzer().analyze_symbol('DOGEUSDT')
# a = Analyzer()

# df = pd.DataFrame.from_dict(a.all_states(), orient='index')
# last_trade_time = df.last_trade_time.max()
# df.loc['ALL', 'buy':] = df.loc[:,'buy':].sum()
# df.loc['ALL', 'last_trade_time'] = df.last_trade_time.max()
# df
# # df.loc['ALL', 'last_trade_price':] = df.loc[:'ALL',]
# #%%

# client.get_ticker(symbol='DOGEUSDT')
