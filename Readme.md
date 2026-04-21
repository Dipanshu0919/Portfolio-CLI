# Crypto Portfolio Tracker CLI

A lightweight, terminal-based application to track your cryptocurrency portfolio in real-time. Built with Python, it allows you to record your coin holdings, track buy prices, and view live Profit & Loss (P&L) calculated using real-time market data fetched via `ccxt`.

---

## ✨ Features

- **Interactive Terminal UI (TUI):** A rich, responsive terminal interface powered by `textual` for seamless navigation and data viewing.
- **Real-Time Data:** Fetches live cryptocurrency prices asynchronously using `ccxt` (defaults to the Bitget exchange).
- **Profit & Loss Tracking:** Automatically calculates your total current value and unrealized P&L based on your buy prices.
- **Local Storage:** All portfolio data is securely stored locally on your machine using an SQLite database (`portfolio.db`).
- **Live Auto-Refresh:** Dedicated "Live Data" view that automatically updates your portfolio's value every 5 seconds.
- **Inline Editing:** Easily update your coin quantities and exchanges directly from the "View All" screen in the Textual app.

---

## 🛠 Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

---

## 🚀 Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/dipanshu0919/portfolio-cli.git
   cd portfolio-cli
   ```

2. **(Optional but recommended) Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 💻 Usage

The project currently contains multiple iterations of the application.

### 1. The Interactive TUI (Recommended)
To run the latest, rich terminal user interface (built with Textual), run:
```bash
python main3.py
```
**Keyboard Navigation (`main3.py`):**
- `1`: Add a new coin
- `2`: Remove a coin
- `3`: View a specific coin's data
- `4`: View all holdings (Select a row with `Enter` to edit)
- `5`: Clear all data
- `6` or `q`: Exit the application
- `7`: Open the Live Data auto-refresh view

### 2. The Standard CLI
If you prefer a simpler, text-prompt-based command-line interface, you can run the original scripts:
```bash
python main.py
```
*(Simply follow the on-screen numbered prompts to manage your portfolio).*

---

## 🗂 Project Structure

- `main.py`: The original, prompt-based Command Line Interface.
- `main2.py`: An alternate iteration of the standard CLI.
- `main3.py`: The robust, advanced Terminal UI (TUI) utilizing the `textual` framework.
- `portfolio.db`: SQLite database file where your holdings are saved (generated automatically on first run).
- `requirements.txt`: Python package dependencies.
- `test.py`: A simple script to test `ccxt` synchronous and asynchronous connection times.

---

## 📦 Dependencies

This project relies on the following open-source libraries:
- `ccxt` - For fetching cryptocurrency market data.
- `tabulate` - For formatting terminal output into clean tables.
- `textual` - For building the interactive terminal user interface.

---

## 🔮 Planned Features (Roadmap)
- CSV Import/Export for bulk adding or backing up holdings.
- Complete transaction history (tracking buys and sells over time).
- Support for multiple API-connected exchanges to auto-fetch wallet balances.
- Price alerts and threshold notifications.
