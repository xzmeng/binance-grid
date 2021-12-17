from asyncio.events import get_running_loop
import os
import asyncio
import logging
import json
import signal

import tinydb

from logging.handlers import RotatingFileHandler
from telegram_handler import TelegramHandler

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException

from utils import quantize_qty, quantize_price
from utils import get_quote, get_step, get_bottom, get_top

logger = logging.getLogger("grid")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)


debug_handler = RotatingFileHandler("debug.log", maxBytes=2 ** 30, backupCount=1)
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(formatter)

t_handler = TelegramHandler(
    token="2142663522:AAFDWuFR1UTfpPJ0Lt5HiBFwm2MqLGMyE7M", chat_id=1057708206
)
t_handler.setLevel(logging.INFO)
t_handler.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(debug_handler)
logger.addHandler(t_handler)


class Main:
    def __init__(self, grids):
        self.client: AsyncClient = None
        self.grids: dict[str, dict] = grids
        self.filled = tinydb.TinyDB("db.json").table("filled")

        self.loop = None
        self.tasks = set()

    async def new_pair(self, grid: dict) -> None:
        await asyncio.gather(self.buy(grid), self.sell(grid))

    async def send_order(self, grid, side):
        symbol = grid["symbol"]
        ratio = 1 + get_step(grid) if side == "sell" else 1 - get_step(grid)
        price = quantize_price(symbol, grid["mid"] * ratio)

        if side == "buy" and price < get_bottom(grid):
            logger.warning(
                f"{symbol} break bottom, cancel buy, price: {price}, config: {grid}"
            )
            return
        if side == "sell" and price > get_top(grid):
            logger.warning(
                f"{symbol} break top, cancel sell, price: {price}, config: {grid}"
            )
            return

        qty = quantize_qty(symbol, get_quote(grid) / price)
        try:
            f = (
                self.client.order_limit_buy
                if side == "buy"
                else self.client.order_limit_sell
            )
            logger.debug(
                f"send order, symbol: {symbol}, side: {side}, price: {price}, quantity: {qty}"
            )
            order = await f(symbol=symbol, price=price, quantity=qty)
            grid[f"{side}_id"] = order["orderId"]
        except BinanceAPIException as e:
            if e.code == -2010 and "insufficient balance" in e.message:
                grid[f"{side}_id"] = None
                logger.warning(f"{symbol} insufficient balance, side: {side}")
            else:
                raise
        except Exception as e:
            logger.exception("unhandled exception happened in send_order()")
            logger.error(f"grid: {grid}, price: {price}, qty: {qty}, side: {side}")

    async def buy(self, grid) -> None:
        await self.send_order(grid, "buy")

    async def sell(self, grid) -> None:
        await self.send_order(grid, "sell")

    async def cancel_order(self, symbol, order_id):
        try:
            await self.client.cancel_order(symbol=symbol, orderId=order_id)
        except BinanceAPIException as e:
            if e.code == -2011 and e.message == "Unknown order sent.":
                Filled = tinydb.Query()
                ids = self.filled.update(
                    {"cancel": True},
                    (Filled.symbol == symbol) & (Filled.i == order_id),
                )
                if ids:
                    logger.warning(
                        "Failed to cancel the order but a filled record was found in the database."
                        "This should be because the orders on both sides were filled at the same time."
                        f"symbol: {symbol}, orderId: {order_id}."
                    )
                else:
                    logger.error(
                        "Failed to cancel the order and no filled record was found in the database."
                        "This should be because the order was unexpectedly cancelled."
                        f"symbol: {symbol}, orderId: {order_id}."
                    )
        except Exception:
            logger.exception("unhandled exception happened in cancel()")
            logger.error(f"symbol: {symbol}, orderId: {order_id}")

    async def handle_msg(self, msg):
        logger.debug(msg)
        if (
            msg["e"] == "executionReport"
            and msg["X"] == "FILLED"
            and msg["s"] in self.grids
        ):
            self.filled.insert(msg)
            grid = self.grids[msg["s"]]

            confirming = False
            if msg["i"] not in [grid.get("buy_id"), grid.get("sell_id")]:
                # Due to network delay, buy_id or sell_id may not be set in time
                # wait for 10 seconds to reduce this possibility
                logger.warning(
                    f"executionReport may not belong to grid. Wait for 5 seconds to confirm.\n{msg}"
                )
                confirming = True
                await asyncio.sleep(5)

            if msg["i"] in [grid.get("buy_id"), grid.get("sell_id")]:
                if confirming:
                    logger.warning(
                        f"executionReport has been confirmed to belong to grid. orderId: {msg['i']}"
                    )
                grid["mid"] = float(msg["p"])
                if msg["i"] == grid.get("buy_id"):
                    cancel_id = grid.get("sell_id")
                else:
                    cancel_id = grid.get("buy_id")

                # If the order pair were filled at the same time, the latter one will not
                # try to cancel or create new orders, but will only record the order
                # information for statistical analysis
                grid["buy_id"] = grid["sell_id"] = None

                if cancel_id:
                    asyncio.create_task(
                        self.cancel_order(symbol=msg["s"], order_id=cancel_id)
                    )
                asyncio.create_task(self.new_pair(grid))

    async def init(self):
        for grid in self.grids.values():
            sell_above, buy_below = grid.get("sell_above"), grid.get("buy_below")
            if sell_above and buy_below:
                logger.error(
                    f'{grid["symbol"]} sell_above and buy_below are set at the same time.'
                )
            elif sell_above:
                grid["mid"] = sell_above
                asyncio.create_task(self.sell(grid))
            elif buy_below:
                grid["mid"] = buy_below
                asyncio.create_task(self.buy(grid))
            else:
                grid["mid"] = grid["start"]
                asyncio.create_task(self.new_pair(grid))

    def cleanup(self):
        logger.warning("cleaning up")
        for grid in self.grids.values():
            buy_id, sell_id = grid.get("buy_id"), grid.get("sell_id")
            logger.debug(
                f'{grid["symbol"]} order to cancel: buy_id={buy_id}, sell_id={sell_id}'
            )
            if buy_id:
                asyncio.create_task(self.cancel_order(grid["symbol"], buy_id))
            if sell_id:
                asyncio.create_task(self.cancel_order(grid["symbol"], sell_id))

    async def main(self):
        logger.info("start")
        self.loop = get_running_loop()
        self.loop.add_signal_handler(signal.SIGINT, self.cleanup)
        self.loop.add_signal_handler(signal.SIGTERM, self.cleanup)
        self.client = await AsyncClient.create(
            os.environ.get("API_KEY"), os.environ.get("API_SECRET")
        )

        prices = await self.client.get_all_tickers()
        prices = {price["symbol"]: float(price["price"]) for price in prices}
        for grid in self.grids.values():
            grid["mid"] = grid["start"] = float(prices[grid["symbol"]])

        bsm = BinanceSocketManager(self.client)
        us = bsm.user_socket()
        async with us as s:
            asyncio.create_task(self.init())
            while True:
                msg = await s.recv()
                asyncio.create_task(self.handle_msg(msg))


def load_config():
    filename = os.environ.get("GRID_CONFIG_FILE", "config.json")
    with open(filename) as f:
        grids = json.load(f)
    for symbol, grid in grids.items():
        grid["symbol"] = symbol
    return grids


grids = load_config()
asyncio.run(Main(grids).main())
