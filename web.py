import json
import pandas as pd
from flask import Flask, render_template, jsonify
from flask import g

from web_utils import Analyzer
from analysis import Analyzer as A

app = Flask(__name__)


# @app.route("/")
# def index():
#     info = Analyzer().info()
#     j = json.dumps(info, indent=2)
#     return render_template("index.html", text=j)

with open("config.json") as f:
    grids = json.load(f)
    config_symbols = list(grids.keys())


@app.context_processor
def inject_symbols():
    A().symbols
    return {
        "trading_symbols": config_symbols,
        "not_trading_symbols": [
            symbol for symbol in A().symbols if symbol not in config_symbols
        ],
    }


@app.route("/trades")
def table():
    return


@app.route("/trades/<symbol>")
def trades(symbol):
    table = Analyzer().one_table(symbol.upper())
    return render_template("index_table.html", html=table)


@app.route("/")
def test():
    a = A()
    return render_template("index_table.html", table=a.all_states_table())


@app.route("/<symbol>")
def symbol_detail(symbol):
    a = A()
    names = [symbol, symbol.upper(), symbol + "USDT", (symbol + "USDT").upper()]
    s = None
    for name in names:
        if name in a.symbols:
            s = name
    if not s:
        return f"{symbol} not found, available symbols are: {a.symbols}"

    return render_template(
        "symbol_detail.html",
        table_state=a.symbol_state_table(s),
        table_trades=a.symbol_trades_table(s),
    )
