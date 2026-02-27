import sqlite3 as sq
import asyncio
import ccxt
import time
import os
import sys
import threading
import datetime
import ccxt.async_support as ccxt_async
from tabulate import tabulate
from functools import wraps

exchange = ccxt.bitget()
thread_lock = threading.Lock()
price_dict = {}


async_exchange = ccxt_async.bitget()
_loop = asyncio.new_event_loop()
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()


def sqldb(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            db = sq.connect("portfolio.db")
            db.row_factory = sq.Row
            c = db.cursor()
            c.execute(
                "CREATE TABLE IF NOT EXISTS pf(TICKER varchar(30), QUANTITY decimal(38,18), BUY_PRICE decimal(38,18), BUY_TOTAL decimal(38,18), CURRENT_PRICE decimal(38,18), TOTAL_AMOUNT decimal(38,18), STATUS varchar(50))"
            )
            call_function = function(c, *args, **kwargs)
        finally:
            db.commit()
            db.close()
        return call_function
    return wrapper


def clear_price_dict():
    global price_dict
    price_dict.clear()


async def async_single_fetch(coin, ticker="USDT"):
    try:
        data = await async_exchange.fetch_ticker(f"{coin}/{ticker}")
        return coin, float(data['last'])
    except Exception as e:
        print(f"ERROR FOR COIN ({coin}/{ticker}): {e}")
        return coin, None


async def async_all_fetch(coins_list, ticker="USDT"):
    tasks = [async_single_fetch(coin, ticker) for coin in set(coins_list)]
    prices = await asyncio.gather(*tasks)
    return {k: v for k, v in prices if v is not None}


def run_async_fetch(coins_list, ticker="USDT"):
    future = asyncio.run_coroutine_threadsafe(
        async_all_fetch(coins_list, ticker), _loop
    )
    return future.result(timeout=10)


def get_price(coin, ticker="USDT"):
    global price_dict
    coin = coin.upper()
    ticker = ticker.upper()
    if price_dict.get(coin):
        return price_dict[coin]
    future = asyncio.run_coroutine_threadsafe(
        async_single_fetch(coin, ticker), _loop
    )
    coin, price = future.result(timeout=10)
    if price:
        price_dict[coin] = price
    return price


@sqldb
def update_price(c, coin, quantity, buy_price, current_price):
    buy_total_amount = buy_price * quantity
    current_price = get_price(coin)
    current_total_amount = current_price * quantity

    if buy_total_amount > current_total_amount:
        status = f"-{(buy_total_amount - current_total_amount):.8f}"
    elif current_total_amount > buy_total_amount:
        status = f"+{(current_total_amount - buy_total_amount):.8f}"
    else:
        status = "0"

    with thread_lock:
        c.execute(
            "UPDATE pf SET STATUS=?, TOTAL_AMOUNT=?, CURRENT_PRICE=? WHERE TICKER=? AND QUANTITY=? AND BUY_PRICE=?",
            (status, round(current_total_amount, 18), current_price, coin, quantity, buy_price)
        )


def show_portfolio(c, coin_name=None):
    global price_dict
    if coin_name:
        c.execute("SELECT * FROM pf WHERE TICKER=?", (coin_name,))
    else:
        c.execute("SELECT * FROM pf")

    all_rows = c.fetchall()
    if not all_rows:
        return "No DATA FOUND!"

    coins_list = list({x["TICKER"] for x in all_rows})
    price_dict = run_async_fetch(coins_list)

    threads = [
        threading.Thread(target=update_price, args=(x["TICKER"], x["QUANTITY"], x["BUY_PRICE"], x["CURRENT_PRICE"]))
        for x in all_rows
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    clear_price_dict()

    if coin_name:
        c.execute("SELECT * FROM pf WHERE TICKER=?", (coin_name,))
    else:
        c.execute("SELECT * FROM pf")

    nall = c.fetchall()
    head = ["COIN", "QTY", "BUY AT", "BUY TOTAL", "CURRENT AT", "TOTAL", "P & L"]
    td = [
        [
            xy['TICKER'],
            xy['QUANTITY'],
            f"${float(xy['BUY_PRICE']):.4f}",
            f"${float(xy['BUY_TOTAL']):.4f}",
            f"${float(xy['CURRENT_PRICE']):.4f}",
            f"${float(xy['TOTAL_AMOUNT']):.4f}",
            xy['STATUS']
        ]
        for xy in nall
    ]

    total = c.execute("SELECT SUM(STATUS) FROM pf").fetchone()
    return tabulate(td, head, tablefmt="grid", stralign="center", numalign="center"), total


@sqldb
def main(c):
    print("\nWhat you want to do? \n1. Coins add\n2. Coin remove\n3. View particular coin\n4. View all coins\n5. Clear all data\n6. Exit\n7. Live data")
    inpu = input("Select 1/2/3/4/5/6: ")

    if inpu in ("6", "6."):
        sys.exit(1)

    elif inpu in ("5", "5."):
        inp2 = input("ARE YOU SURE YOU WANT TO REMOVE ALL DATA? \n1. Yes\n2. No: ")
        if inp2 == "2":
            print("CANCELLED")
        else:
            c.execute("DELETE FROM pf")
            print("REMOVED ALL DATA!")

    elif inpu in ("4", "4."):
        result, _ = show_portfolio(c)
        print("\nHERE IS YOUR PORTFOLIO: \n")
        print(result)

    elif inpu in ("7", "7."):
        while True:
            try:
                result, total = show_portfolio(c)
                os.system("clear")
                print(f"\nLAST UPDATED: {datetime.datetime.now()}\n")
                print(result)
                print(f"\n\nTOTAL PROFIT/LOSS: {total[0]}")
                time.sleep(5)
            except KeyboardInterrupt:
                print()
                return

    elif inpu in ("1", "1."):
        t = input("ENTER COIN TICKER: ").upper()
        q = float(input("ENTER TOTAL QUANTITY: "))
        p = float(input("ENTER BUY PRICE: "))
        tp = round(p * q, 18)
        cp = get_price(t)
        c.execute(
            "INSERT INTO pf(TICKER, QUANTITY, BUY_PRICE, BUY_TOTAL, CURRENT_PRICE, TOTAL_AMOUNT) VALUES (?, ?, ?, ?, ?, ?)",
            (t, q, p, tp, cp, tp),
        )
        print("COIN ADDED SUCCESFULY!")

    elif inpu in ("2", "2."):
        d = input("ENTER TICKER TO REMOVE FROM LIST: ").upper()
        c.execute("SELECT * FROM pf WHERE TICKER = ?", (d,))
        f = c.fetchone()
        if f:
            con = input("ARE YOU SURE YOU WANT TO REMOVE?\n1. YES\n2. NO: ")
            if con == "1":
                c.execute("DELETE FROM pf WHERE TICKER = ?", (d,))
                print("REMOVED!")
            else:
                print("CANCELLED!")
        else:
            print("TICKER NOT FOUND IN DATABASE!")

    elif inpu in ("3", "3."):
        i = input("ENTER TICKER TO FIND ALL DATA: ").upper()
        call, _ = show_portfolio(c, coin_name=i)
        print(call)

    elif inpu == "drop.table":
        c.execute("DROP TABLE pf")


if __name__ == "__main__":
    try:
        while True:
            try:
                main()
            except KeyboardInterrupt:
                print("\nEXITING....")
                break
    finally:
        asyncio.run_coroutine_threadsafe(async_exchange.close(), _loop).result()
        _loop.call_soon_threadsafe(_loop.stop)
        _loop_thread.join()
