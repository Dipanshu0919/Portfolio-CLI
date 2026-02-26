import sqlite3 as sq

import ccxt
import time
import datetime

from tabulate import tabulate
from functools import wraps

exchange = ccxt.bitget()

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

def get_price(coin, ticker="USDT"):
    coin = coin.upper()
    ticker = ticker.upper()
    price = exchange.fetch_ticker(f"{coin}/{ticker}")
    return price


def show_portfolio(c):
    c.execute("SELECT * FROM pf")
    all = c.fetchall()
    if not all:
        print("NO DATA FOUND!")
        return None

    for x in all:
        tick = x['TICKER']
        try:
            n = get_price(tick)
            price = float(n['last'])
            c.execute(
                "UPDATE pf SET CURRENT_PRICE = ? WHERE TICKER = ?",
                (price, tick),
            )
            c.execute("SELECT * FROM pf WHERE TICKER = ?", (tick,))
            sel = c.fetchall()
            for ha in sel:
                quan  = float(ha['QUANTITY'])
                buypr = float(ha['BUY_PRICE'])
                curpr = float(ha['CURRENT_PRICE'])
                bt = quan * buypr
                ct = quan * curpr
                if bt > ct:
                    status = f"LOSS: ${(bt - ct):.8f}"
                elif ct > bt:
                    status = f"PROFIT: ${(ct - bt):.8f}"
                else:
                    status = "NO PROFIT/LOSS"
                c.execute(
                    "UPDATE pf SET STATUS = ?, TOTAL_AMOUNT = ? WHERE BUY_PRICE = ? AND TICKER = ?",
                    (status, round(ct, 18), buypr, tick)
                )
        except Exception as e:
            print(f"Error: {tick}: {e}")
            continue

    c.execute("SELECT * FROM pf")
    nall = c.fetchall()
    head = ["TICKER", "QTY", "BUY AT", "BUY TOTAL", "CURRENT AT", "TOTAL", "STATUS"]
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
def portfolio(c):
    while True:
        print(
            "\nWhat you want to do? \n1. Coins add\n2. Coin remove\n3. View particular coin\n4. View all coins\n5. Clear all data\n6. Exit\n7. Live data"
        )
        inpu = input("Select 1/2/3/4/5/6: ")

        if inpu in ("6", "6."):
            break

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
                        print(f"\nLAST UPDATED: {datetime.datetime.now()}\n")
                        print(result)
                        time.sleep(2)
                        print(f"\033[{lines+3}A", end="")
                    except KeyboardInterrupt:
                        print()
                        break

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
            c.execute("SELECT * FROM pf WHERE TICKER = ?", (i,))
            f = c.fetchone()
            if f:
                tick = get_price(i)
                price = float(tick['last'])
                c.execute("UPDATE pf SET CURRENT_PRICE = ? WHERE TICKER = ?", (price, i))
                c.execute("SELECT * FROM pf WHERE TICKER = ?", (i,))
                sel = c.fetchall()
                for ha in sel:
                    quan  = float(ha['QUANTITY'])
                    buypr = float(ha['BUY_PRICE'])
                    curpr = float(ha['CURRENT_PRICE'])
                    bt = quan * buypr
                    ct = quan * curpr
                    if bt > ct:
                        status = f"LOSS: ${(bt - ct):.8f}"
                    elif ct > bt:
                        status = f"PROFIT: ${(ct - bt):.8f}"
                    else:
                        status = "NO PROFIT/LOSS"
                    c.execute(
                        "UPDATE pf SET STATUS = ?, TOTAL_AMOUNT = ? WHERE BUY_PRICE = ? AND TICKER = ?",
                        (status, round(ct, 18), buypr, i)
                    )

                c.execute("SELECT * FROM pf WHERE TICKER = ?", (i,))
                fet = c.fetchall()
                head = ["TICKER", "QUANTITY", "BUY AT", "BUY TOTAL", "CURRENT AT", "TOTAL", "STATUS"]
                td = []
                for xy in fet:
                    td.append([
                        xy['TICKER'],
                        xy['QUANTITY'],
                        f"${float(xy['BUY_PRICE']):.8f}",
                        f"${float(xy['BUY_TOTAL']):.8f}",
                        f"${float(xy['CURRENT_PRICE']):.8f}",
                        f"${float(xy['TOTAL_AMOUNT']):.8f}",
                        xy['STATUS']
                    ])
                print(tabulate(td, head, tablefmt="grid", stralign="center", numalign="center"))
            else:
                print("TICKER NOT FOUND IN DATABASE!")

        elif inpu == "drop.table":
            c.execute("DROP TABLE pf")


if __name__ == "__main__":
    portfolio()
