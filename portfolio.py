import sqlite3 as sq

import ccxt
from tabulate import tabulate

ex = ccxt.bitget()


def portfolio():
    cu = sq.connect("portfolio.db")
    c = cu.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS pf(TICKER varchar(30), QUANTITY decimal(9,9), BUY_PRICE decimal(9,9), CURRENT_PRICE decimal(9,9), TOTAL_AMOUNT decimal(9,9), STATUS varchar(50))"
    )
    cu.commit()

    while True:
        print(
            "\nWhat you want to do? \n1. Coins add\n2. Coin remove\n3. View particular coin\n4. View all coins\n5. Clear all data\n6. Exit"
        )
        inpu = input("Select 1/2/3/4/5/6: ")

        if inpu == "6" or inpu == "6.":
            break

        elif inpu == "5" or inpu == "5.":
            inp2 = input("ARE YOU SURE YOU WANT TO REMOVE ALL DATA? \n1. Yes\n2. No: ")
            if inp2 == "2":
                print("CANCELLED")
            else:
                c.execute("DELETE FROM pf")
                cu.commit()
                print("REMOVED ALL DATA!")
                continue

        elif inpu == "4" or inpu == "4.":
            c.execute("SELECT * FROM pf")
            all = c.fetchall()
            if not all:
                print("NO DATA FOUND!")
                continue

            for x in all:
                tick = x[0]
                try:
                    n = ex.fetch_ticker(f"{tick}/USDT")
                    price = f"{float(n['last']):.2f}"
                    c.execute(
                        "UPDATE pf SET CURRENT_PRICE = ? WHERE TICKER = ?",
                        (price, tick),
                    )
                    c.execute(f"SELECT * FROM pf where TICKER = '{tick}'")
                    sel = c.fetchall()
                    for ha in sel:
                        quan = ha[1]
                        buypr = ha[2]
                        curpr = ha[3]
                        bt = quan * buypr
                        ct = quan * curpr
                        if bt > ct:
                            status = f"LOSS: {(bt - ct):.2f}"
                        elif ct > bt:
                            status = f"PROFIT: {(ct - bt):.2f}"
                        else:
                            status = "NO PROFIT/LOSS"
                        c.execute(
                            f"UPDATE pf SET STATUS = '{status}' WHERE BUY_PRICE={buypr} AND TICKER='{tick}'"
                        )
                except Exception as e:
                    print(f"Error: {tick}: {e}")
                    continue
            cu.commit()
            c.execute("SELECT * FROM pf")
            nall = c.fetchall()
            head = ["TICKER", "QTY", "BUY AT", "CURRENT AT", "TOTAL", "STATUS"]
            td = []
            for xy in nall:
                td.append(
                    [xy[0], xy[1], f"${xy[2]}", f"${xy[3]:.2f}", f"${xy[4]}", xy[5]]
                )

            print("\nHERE IS YOUR PORTFOLIO: \n")
            print(
                tabulate(
                    td, head, tablefmt="grid", stralign="center", numalign="center"
                )
            )

        elif inpu == "1" or inpu == "1.":
            t = input("ENTER COIN TICKER: ").upper()
            q = float(input("ENTER TOTAL QUANTITY: "))
            p = float(input("ENTER BUY PRICE: "))
            tp = round(p * q, 4)
            tick = ex.fetch_ticker(f"{t}/USDT")
            cp = f"{float(tick['last']):.2f}"
            c.execute(
                "INSERT INTO pf(TICKER, QUANTITY, BUY_PRICE, CURRENT_PRICE, TOTAL_AMOUNT) VALUES (?, ?, ?, ?, ?)",
                (t, q, p, cp, tp),
            )
            cu.commit()

        elif inpu == "2" or inpu == "2.":
            d = input("ENTER TICKER TO REMOVE FROM LIST: ").upper()
            c.execute(f"SELECT * FROM pf WHERE TICKER = '{d}'")
            f = c.fetchone()
            if f:
                con = input("ARE YOU SURE YOU WANT TO REMOVE?\n1. YES\n2. NO: ")
                if con == "1":
                    c.execute(f"DELETE FROM pf WHERE TICKER = '{d}'")
                    cu.commit()
                    print("REMOVED!")
                else:
                    print("CANCELLED!")
                    continue

        elif inpu == "3" or inpu == "3.":
            i = input("ENTER TICKER TO FIND ALL DATA: ").upper()
            c.execute(f"SELECT * FROM pf WHERE TICKER = '{i}'")
            f = c.fetchone()
            if f:
                tick = ex.fetch_ticker(f"{i}/USDT")
                price = f"{float(tick['last']):.2f}"
                c.execute(
                    "UPDATE pf SET CURRENT_PRICE = ? WHERE TICKER = ?", (price, i)
                )
                c.execute(f"SELECT * FROM pf where TICKER = '{i}'")
                sel = c.fetchall()
                for ha in sel:
                    quan = ha[1]
                    buypr = ha[2]
                    curpr = ha[3]
                    bt = quan * buypr
                    ct = quan * curpr
                    if bt > ct:
                        status = f"LOSS: {(bt - ct):.2f}"
                    elif ct > bt:
                        status = f"PROFIT: {(ct - bt):.2f}"
                    else:
                        status = "NO PROFIT/LOSS"
                    c.execute(
                        f"UPDATE pf SET STATUS = '{status}' WHERE BUY_PRICE={buypr} AND TICKER='{i}'"
                    )
                cu.commit()
                c.execute(f"SELECT * FROM pf WHERE TICKER = '{i}'")
                fet = c.fetchall()
                head = ["TICKER", "QUANTITY", "BUY AT", "CURRENT AT", "TOTAL", "STATUS"]
                td = []
                for xy in fet:
                    td.append(
                        [xy[0], xy[1], f"${xy[2]}", f"${xy[3]:.2f}", f"${xy[4]}", xy[5]]
                    )

                print(
                    tabulate(
                        td, head, tablefmt="grid", stralign="center", numalign="center"
                    )
                )
            else:
                print("TICKER NOT FOUND IN DATABASE!")

        elif inpu == "drop.table":
            c.execute("DROP TABLE pf")
            cu.commit()


if __name__ == "__main__":
    portfolio()
