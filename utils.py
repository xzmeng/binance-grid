import os
from decimal import Decimal as D
from binance import Client

DEFAULT_STEP = 0.005
DEFAULT_BOTTOM_RATIO = 0.95
DEFAULT_TOP_RATIO = 1.05

client = Client(os.environ.get("API_KEY"), os.environ.get("API_SECRET"))

step_sizes = {}
tick_sizes = {}
min_notionals = {}

res = client.get_exchange_info()

for symbol in res["symbols"]:
    name = symbol["symbol"]
    tick_sizes[name] = symbol["filters"][0]["tickSize"].rstrip("0").rstrip(".")
    step_sizes[name] = symbol["filters"][2]["stepSize"].rstrip("0").rstrip(".")
    min_notionals[name] = symbol["filters"][3]["minNotional"].rstrip("0").rstrip(".")


def quantize_qty(symbol, qty):
    return D(qty).quantize(D(step_sizes[symbol]))


def quantize_price(symbol, qty):
    return D(qty).quantize(D(tick_sizes[symbol]))


def get_quote(grid):
    if "quote" in grid:
        return grid["quote"]
    else:
        return D(min_notionals[grid["symbol"]]) * D("1.05")


def get_step(grid):
    return grid.get("step", DEFAULT_STEP)


def get_bottom(grid):
    if "bottom" in grid:
        bottom = grid["bottom"]
    elif "bottom_ratio" in grid:
        bottom = grid["start"] * grid["bottom_ratio"]
    else:
        bottom = grid["start"] * DEFAULT_BOTTOM_RATIO
    return bottom


def get_top(grid):
    if "top" in grid:
        top = grid["top"]
    elif "top_ratio" in grid:
        top = grid["start"] * grid["top_ratio"]
    else:
        top = grid["start"] * DEFAULT_TOP_RATIO
    return top
