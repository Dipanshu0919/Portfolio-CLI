from __future__ import annotations

import asyncio
import datetime
import sqlite3 as sq
import threading
from functools import wraps

import ccxt
import ccxt.async_support as ccxt_async
from tabulate import tabulate
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Button, DataTable, Footer, Header, Input, Static

exchange = ccxt.bitget()
thread_lock = threading.Lock()
price_dict: dict[str, float] = {}

async_exchange = ccxt_async.bitget()
_loop = asyncio.new_event_loop()
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()

PORTFOLIO_HEADERS = ("COIN", "QTY", "BUY AT", "BUY TOTAL", "CURRENT AT", "TOTAL", "P & L")
PAGE_NAMES = ("add", "remove", "view-one", "view-all", "clear", "live")
NAV_ITEMS = (
    ("add", "Coins add"),
    ("remove", "Coin remove"),
    ("view-one", "View particular coin"),
    ("view-all", "View all coins"),
    ("clear", "Clear all data"),
    ("exit", "Exit"),
    ("live", "Live data"),
)


def sqldb(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            db = sq.connect("portfolio.db")
            db.row_factory = sq.Row
            c = db.cursor()
            c.execute(
                "CREATE TABLE IF NOT EXISTS pf("
                "TICKER varchar(30), "
                "QUANTITY decimal(38,18), "
                "BUY_PRICE decimal(38,18), "
                "BUY_TOTAL decimal(38,18), "
                "CURRENT_PRICE decimal(38,18), "
                "TOTAL_AMOUNT decimal(38,18), "
                "STATUS varchar(50))"
            )
            call_function = function(c, *args, **kwargs)
        finally:
            db.commit()
            db.close()
        return call_function

    return wrapper


def clear_price_dict():
    price_dict.clear()


async def async_single_fetch(coin, ticker="USDT"):
    try:
        data = await async_exchange.fetch_ticker(f"{coin}/{ticker}")
        return coin, float(data["last"])
    except Exception as e:
        print(f"ERROR FOR COIN ({coin}/{ticker}): {e}")
        return coin, None


async def async_all_fetch(coins_list, ticker="USDT"):
    tasks = [async_single_fetch(coin, ticker) for coin in set(coins_list)]
    prices = await asyncio.gather(*tasks)
    return {k: v for k, v in prices if v is not None}


def run_async_fetch(coins_list, ticker="USDT"):
    future = asyncio.run_coroutine_threadsafe(async_all_fetch(coins_list, ticker), _loop)
    return future.result(timeout=10)


def get_price(coin, ticker="USDT"):
    coin = coin.upper()
    ticker = ticker.upper()
    if coin in price_dict:
        return price_dict[coin]
    future = asyncio.run_coroutine_threadsafe(async_single_fetch(coin, ticker), _loop)
    _, price = future.result(timeout=10)
    if price is not None:
        price_dict[coin] = price
    return price


@sqldb
def update_price(c, coin, quantity, buy_price, current_price):
    buy_total_amount = buy_price * quantity
    current_price = get_price(coin)
    if current_price is None:
        return

    current_total_amount = current_price * quantity
    diff = current_total_amount - buy_total_amount
    status = f"{diff:+.4f}"

    with thread_lock:
        c.execute(
            "UPDATE pf SET STATUS=?, TOTAL_AMOUNT=?, CURRENT_PRICE=? "
            "WHERE TICKER=? AND QUANTITY=? AND BUY_PRICE=?",
            (status, round(current_total_amount, 18), current_price, coin, quantity, buy_price),
        )


def _format_currency(value):
    return f"${float(value or 0):.4f}"


def _format_total(value):
    return f"{float(value or 0):+.4f}"


def _build_portfolio_snapshot(c, coin_name=None):
    if coin_name:
        c.execute("SELECT * FROM pf WHERE TICKER=?", (coin_name,))
    else:
        c.execute("SELECT * FROM pf")

    all_rows = c.fetchall()
    if not all_rows:
        return [], 0.0

    coins_list = list({row["TICKER"] for row in all_rows})
    fetched_prices = run_async_fetch(coins_list)
    price_dict.update(fetched_prices)

    threads = [
        threading.Thread(
            target=update_price,
            args=(row["TICKER"], row["QUANTITY"], row["BUY_PRICE"], row["CURRENT_PRICE"]),
        )
        for row in all_rows
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    clear_price_dict()

    if coin_name:
        c.execute("SELECT * FROM pf WHERE TICKER=?", (coin_name,))
    else:
        c.execute("SELECT * FROM pf")

    refreshed_rows = c.fetchall()
    rendered_rows = []
    total_profit_loss = 0.0

    for row in refreshed_rows:
        buy_total = float(row["BUY_TOTAL"] or 0)
        current_total = float(row["TOTAL_AMOUNT"] or 0)
        status_value = float(row["STATUS"] or 0)
        total_profit_loss += status_value

        if buy_total == 0:
            percentage = "N/A"
        else:
            percentage = f"{((current_total - buy_total) / buy_total) * 100:+.2f}%"

        rendered_rows.append(
            (
                row["TICKER"],
                str(row["QUANTITY"]),
                _format_currency(row["BUY_PRICE"]),
                _format_currency(row["BUY_TOTAL"]),
                _format_currency(row["CURRENT_PRICE"]),
                _format_currency(row["TOTAL_AMOUNT"]),
                f"{row['STATUS'] or '+0.0000'} ({percentage})",
            )
        )

    return rendered_rows, total_profit_loss


@sqldb
def show_portfolio(c, coin_name=None):
    rows, total = _build_portfolio_snapshot(c, coin_name)
    if not rows:
        return "No DATA FOUND!", (0.0,)
    return tabulate(rows, PORTFOLIO_HEADERS, tablefmt="grid", stralign="center", numalign="center"), (total,)


@sqldb
def get_portfolio_snapshot(c, coin_name=None):
    return _build_portfolio_snapshot(c, coin_name.upper() if coin_name else None)


@sqldb
def add_coin(c, ticker, quantity, buy_price):
    ticker = ticker.upper()
    total_purchase = round(buy_price * quantity, 18)
    current_price = get_price(ticker)
    if current_price is None:
        raise ValueError(f"Unable to fetch live price for {ticker}.")

    c.execute(
        "INSERT INTO pf(TICKER, QUANTITY, BUY_PRICE, BUY_TOTAL, CURRENT_PRICE, TOTAL_AMOUNT, STATUS) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, quantity, buy_price, total_purchase, current_price, total_purchase, "0.0000"),
    )
    return ticker, current_price


@sqldb
def remove_coin(c, ticker):
    ticker = ticker.upper()
    count = c.execute("SELECT COUNT(*) FROM pf WHERE TICKER = ?", (ticker,)).fetchone()[0]
    if not count:
        return False, 0
    c.execute("DELETE FROM pf WHERE TICKER = ?", (ticker,))
    return True, count


@sqldb
def clear_all_data(c):
    count = c.execute("SELECT COUNT(*) FROM pf").fetchone()[0]
    c.execute("DELETE FROM pf")
    return count


@sqldb
def drop_table(c):
    c.execute("DROP TABLE IF EXISTS pf")


def _parse_positive_number(raw_value, field_name):
    try:
        parsed = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid number.") from exc

    if parsed <= 0:
        raise ValueError(f"{field_name} must be greater than zero.")
    return parsed


class PortfolioTextualApp(App):
    CSS = """
    Screen {
        layout: vertical;
        background: #08111f;
        color: #f8fafc;
    }

    Header {
        dock: top;
    }

    #nav-bar {
        dock: top;
        height: auto;
        padding: 1 2;
        background: #0f172a;
        border-bottom: solid #1d4ed8;
    }

    #nav-bar Button {
        margin-right: 1;
        min-width: 14;
        background: #162033;
        color: #dbeafe;
        border: solid #274060;
    }

    #nav-bar Button.active {
        background: #1d4ed8;
        color: #ffffff;
        text-style: bold;
    }

    #page-stack {
        height: 1fr;
        padding: 1 2;
    }

    .page {
        height: 1fr;
    }

    .section-title {
        margin-bottom: 1;
        color: #f8fafc;
        text-style: bold;
    }

    .help-text {
        margin-bottom: 1;
        color: #94a3b8;
    }

    .summary {
        margin: 1 0;
        padding: 1 1;
        background: #0f172a;
        color: #bfdbfe;
        border: round #334155;
    }

    .form-row {
        height: auto;
        margin-bottom: 1;
    }

    .field-label {
        width: 24;
        padding-top: 1;
        color: #93c5fd;
    }

    .field-input {
        width: 1fr;
    }

    .action-row {
        height: auto;
        margin-top: 1;
    }

    .action-row Button {
        margin-right: 1;
    }

    DataTable {
        height: 1fr;
        margin-top: 1;
        background: #020617;
    }

    #status-bar {
        dock: bottom;
        height: auto;
        padding: 1 2;
        background: #0f172a;
        color: #e2e8f0;
        border-top: solid #1e293b;
    }

    Footer {
        dock: bottom;
    }
    """

    BINDINGS = [
        ("1", "show_add", "Coins add"),
        ("2", "show_remove", "Coin remove"),
        ("3", "show_view_one", "View coin"),
        ("4", "show_view_all", "View all"),
        ("5", "show_clear", "Clear all"),
        ("6", "exit_app", "Exit"),
        ("7", "show_live", "Live data"),
        ("q", "exit_app", "Exit"),
    ]

    def __init__(self):
        super().__init__()
        self.active_view = "add"
        self.live_refresh_in_progress = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="nav-bar"):
            for page_name, label in NAV_ITEMS:
                yield Button(label, id=f"nav-{page_name}")
        with Container(id="page-stack"):
            with Vertical(id="page-add", classes="page"):
                yield Static("Coins Add", classes="section-title")
                yield Static(
                    "Fill all fields, then submit to save a new holding to portfolio.db.",
                    classes="help-text",
                )
                with Horizontal(classes="form-row"):
                    yield Static("Coin ticker", classes="field-label")
                    yield Input(placeholder="BTC", id="add-ticker", classes="field-input")
                with Horizontal(classes="form-row"):
                    yield Static("Total quantity", classes="field-label")
                    yield Input(placeholder="0.50", id="add-quantity", classes="field-input")
                with Horizontal(classes="form-row"):
                    yield Static("Buy price", classes="field-label")
                    yield Input(placeholder="42000", id="add-buy-price", classes="field-input")
                with Horizontal(classes="action-row"):
                    yield Button("Add coin", id="submit-add", variant="primary")
                    yield Button("Reset", id="reset-add")

            with Vertical(id="page-remove", classes="page"):
                yield Static("Coin Remove", classes="section-title")
                yield Static(
                    "Enter the ticker and type YES to remove every row for that coin.",
                    classes="help-text",
                )
                with Horizontal(classes="form-row"):
                    yield Static("Coin ticker", classes="field-label")
                    yield Input(placeholder="BTC", id="remove-ticker", classes="field-input")
                with Horizontal(classes="form-row"):
                    yield Static("Confirmation", classes="field-label")
                    yield Input(placeholder="Type YES", id="remove-confirmation", classes="field-input")
                with Horizontal(classes="action-row"):
                    yield Button("Remove coin", id="submit-remove", variant="error")
                    yield Button("Reset", id="reset-remove")

            with Vertical(id="page-view-one", classes="page"):
                yield Static("View Particular Coin", classes="section-title")
                yield Static(
                    "Enter a ticker to refresh and display only that coin's rows.",
                    classes="help-text",
                )
                with Horizontal(classes="form-row"):
                    yield Static("Coin ticker", classes="field-label")
                    yield Input(placeholder="BTC", id="view-one-ticker", classes="field-input")
                with Horizontal(classes="action-row"):
                    yield Button("View coin", id="submit-view-one", variant="primary")
                    yield Button("Clear table", id="reset-view-one")
                yield Static("No coin loaded yet.", id="view-one-summary", classes="summary")
                yield DataTable(id="view-one-table")

            with Vertical(id="page-view-all", classes="page"):
                yield Static("View All Coins", classes="section-title")
                yield Static(
                    "Refresh the whole portfolio with the latest fetched prices.",
                    classes="help-text",
                )
                with Horizontal(classes="action-row"):
                    yield Button("Refresh portfolio", id="submit-view-all", variant="primary")
                yield Static("No portfolio data loaded yet.", id="view-all-summary", classes="summary")
                yield DataTable(id="view-all-table")

            with Vertical(id="page-clear", classes="page"):
                yield Static("Clear All Data", classes="section-title")
                yield Static(
                    "Type CLEAR to remove every portfolio row from the database.",
                    classes="help-text",
                )
                with Horizontal(classes="form-row"):
                    yield Static("Confirmation", classes="field-label")
                    yield Input(placeholder="Type CLEAR", id="clear-confirmation", classes="field-input")
                with Horizontal(classes="action-row"):
                    yield Button("Clear data", id="submit-clear", variant="error")
                    yield Button("Reset", id="reset-clear")

            with Vertical(id="page-live", classes="page"):
                yield Static("Live Data", classes="section-title")
                yield Static(
                    "This view auto-refreshes every 5 seconds while it is open.",
                    classes="help-text",
                )
                with Horizontal(classes="action-row"):
                    yield Button("Refresh now", id="submit-live-now", variant="primary")
                yield Static("Last updated: never", id="live-updated", classes="summary")
                yield Static("No live portfolio data loaded yet.", id="live-summary", classes="summary")
                yield DataTable(id="live-table")
        yield Static("Ready. Use the top navbar or keys 1-7.", id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        self._setup_tables()
        self.switch_view("add")
        self.set_interval(5, self._trigger_live_refresh)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""

        if button_id.startswith("nav-"):
            page_name = button_id.removeprefix("nav-")
            if page_name == "exit":
                self.exit()
            else:
                self.switch_view(page_name)
            return

        if button_id == "submit-add":
            await self.handle_add_coin()
        elif button_id == "reset-add":
            self.reset_add_form()
        elif button_id == "submit-remove":
            await self.handle_remove_coin()
        elif button_id == "reset-remove":
            self.reset_remove_form()
        elif button_id == "submit-view-one":
            await self.refresh_single_coin_view()
        elif button_id == "reset-view-one":
            self.reset_single_coin_view()
        elif button_id == "submit-view-all":
            await self.refresh_all_view()
        elif button_id == "submit-clear":
            await self.handle_clear_all()
        elif button_id == "reset-clear":
            self.reset_clear_form()
        elif button_id == "submit-live-now":
            await self.refresh_live_view()

    def action_show_add(self) -> None:
        self.switch_view("add")

    def action_show_remove(self) -> None:
        self.switch_view("remove")

    def action_show_view_one(self) -> None:
        self.switch_view("view-one")

    def action_show_view_all(self) -> None:
        self.switch_view("view-all")

    def action_show_clear(self) -> None:
        self.switch_view("clear")

    def action_show_live(self) -> None:
        self.switch_view("live")

    def action_exit_app(self) -> None:
        self.exit()

    def switch_view(self, page_name: str) -> None:
        if page_name not in PAGE_NAMES:
            return

        self.active_view = page_name

        for current_page in PAGE_NAMES:
            page = self.query_one(f"#page-{current_page}", Vertical)
            page.display = current_page == page_name

        for nav_name, _ in NAV_ITEMS:
            if nav_name == "exit":
                continue
            self.query_one(f"#nav-{nav_name}", Button).set_class(nav_name == page_name, "active")

        if page_name == "add":
            self.query_one("#add-ticker", Input).focus()
            self.set_status("Add a new holding by filling all visible fields.")
        elif page_name == "remove":
            self.query_one("#remove-ticker", Input).focus()
            self.set_status("Remove all rows for one ticker.")
        elif page_name == "view-one":
            self.query_one("#view-one-ticker", Input).focus()
            self.set_status("Enter a ticker to view one coin.")
        elif page_name == "view-all":
            self.set_status("Refreshing full portfolio view.")
            asyncio.create_task(self.refresh_all_view())
        elif page_name == "clear":
            self.query_one("#clear-confirmation", Input).focus()
            self.set_status("Type CLEAR before deleting every row.")
        elif page_name == "live":
            self.set_status("Live data view is active and will refresh every 5 seconds.")
            asyncio.create_task(self.refresh_live_view())

    def _setup_tables(self) -> None:
        for table_id in ("#view-one-table", "#view-all-table", "#live-table"):
            table = self.query_one(table_id, DataTable)
            table.add_columns(*PORTFOLIO_HEADERS)
            table.cursor_type = "row"

    def _render_rows(self, table_id: str, rows) -> None:
        table = self.query_one(table_id, DataTable)
        table.clear(columns=False)
        for row in rows:
            table.add_row(*row)

    def _trigger_live_refresh(self) -> None:
        if self.active_view != "live" or self.live_refresh_in_progress:
            return
        asyncio.create_task(self.refresh_live_view())

    def set_status(self, message: str) -> None:
        self.query_one("#status-bar", Static).update(message)

    def reset_add_form(self) -> None:
        self.query_one("#add-ticker", Input).value = ""
        self.query_one("#add-quantity", Input).value = ""
        self.query_one("#add-buy-price", Input).value = ""
        self.query_one("#add-ticker", Input).focus()
        self.set_status("Add form cleared.")

    def reset_remove_form(self) -> None:
        self.query_one("#remove-ticker", Input).value = ""
        self.query_one("#remove-confirmation", Input).value = ""
        self.query_one("#remove-ticker", Input).focus()
        self.set_status("Remove form cleared.")

    def reset_single_coin_view(self) -> None:
        self.query_one("#view-one-ticker", Input).value = ""
        self.query_one("#view-one-summary", Static).update("No coin loaded yet.")
        self._render_rows("#view-one-table", [])
        self.query_one("#view-one-ticker", Input).focus()
        self.set_status("Single-coin view cleared.")

    def reset_clear_form(self) -> None:
        self.query_one("#clear-confirmation", Input).value = ""
        self.query_one("#clear-confirmation", Input).focus()
        self.set_status("Clear-all confirmation reset.")

    async def handle_add_coin(self) -> None:
        ticker = self.query_one("#add-ticker", Input).value.strip().upper()
        quantity_raw = self.query_one("#add-quantity", Input).value.strip()
        buy_price_raw = self.query_one("#add-buy-price", Input).value.strip()

        if not ticker:
            self.set_status("Coin ticker is required.")
            self.query_one("#add-ticker", Input).focus()
            return

        try:
            quantity = _parse_positive_number(quantity_raw, "Quantity")
            buy_price = _parse_positive_number(buy_price_raw, "Buy price")
        except ValueError as exc:
            self.set_status(str(exc))
            return

        self.set_status(f"Adding {ticker} to the portfolio...")
        try:
            saved_ticker, current_price = await asyncio.to_thread(add_coin, ticker, quantity, buy_price)
        except Exception as exc:
            self.set_status(f"Add failed: {exc}")
            return

        self.reset_add_form()
        self.set_status(
            f"{saved_ticker} added successfully. Latest fetched price: {_format_currency(current_price)}."
        )

    async def handle_remove_coin(self) -> None:
        ticker = self.query_one("#remove-ticker", Input).value.strip().upper()
        confirmation = self.query_one("#remove-confirmation", Input).value.strip().upper()

        if not ticker:
            self.set_status("Coin ticker is required before removing.")
            self.query_one("#remove-ticker", Input).focus()
            return

        if confirmation != "YES":
            self.set_status("Type YES in the confirmation field to remove the coin.")
            self.query_one("#remove-confirmation", Input).focus()
            return

        self.set_status(f"Removing {ticker} from the portfolio...")
        try:
            removed, count = await asyncio.to_thread(remove_coin, ticker)
        except Exception as exc:
            self.set_status(f"Remove failed: {exc}")
            return

        if not removed:
            self.set_status(f"{ticker} was not found in the database.")
            return

        self.reset_remove_form()
        self.set_status(f"Removed {count} row(s) for {ticker}.")

    async def refresh_single_coin_view(self) -> None:
        ticker = self.query_one("#view-one-ticker", Input).value.strip().upper()
        if not ticker:
            self.set_status("Enter a coin ticker to view.")
            self.query_one("#view-one-ticker", Input).focus()
            return

        self.set_status(f"Refreshing rows for {ticker}...")
        try:
            rows, total = await asyncio.to_thread(get_portfolio_snapshot, ticker)
        except Exception as exc:
            self.set_status(f"View failed: {exc}")
            return

        self._render_rows("#view-one-table", rows)
        if not rows:
            self.query_one("#view-one-summary", Static).update(f"No data found for {ticker}.")
            self.set_status(f"No rows found for {ticker}.")
            return

        self.query_one("#view-one-summary", Static).update(
            f"{len(rows)} row(s) for {ticker}. Total P/L: {_format_total(total)}"
        )
        self.set_status(f"Loaded {len(rows)} row(s) for {ticker}.")

    async def refresh_all_view(self) -> None:
        self.set_status("Refreshing full portfolio...")
        try:
            rows, total = await asyncio.to_thread(get_portfolio_snapshot)
        except Exception as exc:
            self.set_status(f"Portfolio refresh failed: {exc}")
            return

        self._render_rows("#view-all-table", rows)
        if not rows:
            self.query_one("#view-all-summary", Static).update("No portfolio data found.")
            self.set_status("Portfolio is empty.")
            return

        self.query_one("#view-all-summary", Static).update(
            f"{len(rows)} row(s) loaded. Total P/L: {_format_total(total)}"
        )
        self.set_status(f"Portfolio refreshed with {len(rows)} row(s).")

    async def handle_clear_all(self) -> None:
        confirmation = self.query_one("#clear-confirmation", Input).value.strip().upper()
        if confirmation != "CLEAR":
            self.set_status("Type CLEAR to remove every portfolio row.")
            self.query_one("#clear-confirmation", Input).focus()
            return

        self.set_status("Clearing all portfolio data...")
        try:
            deleted_rows = await asyncio.to_thread(clear_all_data)
        except Exception as exc:
            self.set_status(f"Clear-all failed: {exc}")
            return

        self.reset_clear_form()
        self._render_rows("#view-all-table", [])
        self._render_rows("#view-one-table", [])
        self._render_rows("#live-table", [])
        self.query_one("#view-all-summary", Static).update("No portfolio data found.")
        self.query_one("#view-one-summary", Static).update("No coin loaded yet.")
        self.query_one("#live-summary", Static).update("No live portfolio data loaded yet.")
        self.query_one("#live-updated", Static).update("Last updated: never")
        self.set_status(f"Removed {deleted_rows} row(s) from the database.")

    async def refresh_live_view(self) -> None:
        if self.live_refresh_in_progress:
            return

        self.live_refresh_in_progress = True
        self.set_status("Refreshing live portfolio data...")

        try:
            rows, total = await asyncio.to_thread(get_portfolio_snapshot)
            self._render_rows("#live-table", rows)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.query_one("#live-updated", Static).update(f"Last updated: {timestamp}")

            if not rows:
                self.query_one("#live-summary", Static).update("No portfolio data found.")
                self.set_status("Live view refreshed. Portfolio is empty.")
                return

            self.query_one("#live-summary", Static).update(
                f"{len(rows)} row(s) loaded. Total P/L: {_format_total(total)}"
            )
            self.set_status(f"Live view refreshed at {timestamp}.")
        except Exception as exc:
            self.set_status(f"Live refresh failed: {exc}")
        finally:
            self.live_refresh_in_progress = False


def shutdown_async_runtime() -> None:
    try:
        asyncio.run_coroutine_threadsafe(async_exchange.close(), _loop).result(timeout=10)
    except Exception:
        pass

    try:
        _loop.call_soon_threadsafe(_loop.stop)
    except RuntimeError:
        pass

    _loop_thread.join(timeout=5)


def main() -> None:
    try:
        PortfolioTextualApp().run()
    finally:
        shutdown_async_runtime()


if __name__ == "__main__":
    main()
