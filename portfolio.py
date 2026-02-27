import sqlite3 as sq

import ccxt
import time
import os
import sys
import threading
import datetime

from tabulate import tabulate
from functools import wraps

exchange = ccxt.bitget()

thread_lock = threading.Lock()

price_dict = {}

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


def get_price(coin, ticker="USDT"):
    global price_dict
    coin = coin.upper()
    ticker = ticker.upper()
    with thread_lock:
        if price_dict.get(coin):
            price = price_dict.get(coin)
        else:
            price = exchange.fetch_ticker(f"{coin}/{ticker}")
            print(f"FETCHING {coin}")
            price_dict[coin] = price
    return price

@sqldb
def update_price(c, coin, quantity, buy_price, current_price):
    pass
    buy_total_amount = buy_price * quantity
    cp = get_price(coin)
    current_price = float(cp['last'])
    current_total_amount = current_price * quantity

    if buy_total_amount > current_total_amount:
        status = f"LOSS: ${(buy_total_amount - current_total_amount):.8f}"
    elif current_total_amount > buy_total_amount:
        status = f"PROFIT: ${(current_total_amount - buy_total_amount):.8f}"
    else:
        status = "NO PROFIT/LOSS"

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

    all = c.fetchall()
    if not all:
        return "No DATA FOUND!"

    threads = []

    for x in all:
        threads.append(threading.Thread(target=update_price, args=(x["TICKER"], x["QUANTITY"], x["BUY_PRICE"], x["CURRENT_PRICE"])))

    for y in threads:
        y.start()

    for z in threads:
        z.join()

    clear_price_dict()

    if coin_name:
        c.execute("SELECT * FROM pf WHERE TICKER=?", (coin_name,))
    else:
        c.execute("SELECT * FROM pf")

    nall = c.fetchall()
    head = ["COIN", "QTY", "BUY AT", "BUY TOTAL", "CURRENT AT", "TOTAL", "STATUS"]
    td = []
    for xy in nall:
        td.append([
            xy['TICKER'],
            xy['QUANTITY'],
            f"${float(xy['BUY_PRICE']):.4f}",
            f"${float(xy['BUY_TOTAL']):.4f}",
            f"${float(xy['CURRENT_PRICE']):.4f}",
            f"${float(xy['TOTAL_AMOUNT']):.4f}",
            xy['STATUS']
        ])

    return tabulate(td, head, tablefmt="grid", stralign="center", numalign="center")

@sqldb
def main(c):
        print(
            "\nWhat you want to do? \n1. Coins add\n2. Coin remove\n3. View particular coin\n4. View all coins\n5. Clear all data\n6. Exit\n7. Live data"
        )
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
            result = show_portfolio(c)
            print("\nHERE IS YOUR PORTFOLIO: \n")
            print(result)

        elif inpu in ("7", "7."):
            result = show_portfolio(c)
            if result:
                lines = len(result.split("\n"))
                while True:
                    try:
                        result = show_portfolio(c)
                        os.system("clear")
                        print(f"\nLAST UPDATED: {datetime.datetime.now()}\n")
                        print(result)
                        time.sleep(2)
                        # print(f"\033[{lines+3}A", end="")
                    except KeyboardInterrupt:
                        print()
                        return

        elif inpu in ("1", "1."):
            t = input("ENTER COIN TICKER: ").upper()
            q = float(input("ENTER TOTAL QUANTITY: "))
            p = float(input("ENTER BUY PRICE: "))
            tp = round(p * q, 18)
            tick = get_price(t)
            cp = float(tick['last'])
            c.execute(
                "INSERT INTO pf(TICKER, QUANTITY, BUY_PRICE, BUY_TOTAL, CURRENT_PRICE, TOTAL_AMOUNT) VALUES (?, ?, ?, ?, ?, ?)",
                (t, q, p, tp, cp, tp),
            )
            print("COIN ADDED SUCCESFULY!")
            return

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
            call = show_portfolio(c, coin_name=i)
            print(call)

        elif inpu == "drop.table":
            c.execute("DROP TABLE pf")


if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print("\nEXITING....")
            break
