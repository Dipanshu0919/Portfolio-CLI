import time
import ccxt
import ccxt.async_support as ccxt_async

print("Testing connection times for sync and async exchanges...")

print("Connecting to sync exchange...")

now = time.time()

exchange = ccxt.bitget()

print(f"connection time for sync exhange: {time.time() - now} seconds")

print("Connecting to async exchange...")

now = time.time()

async_exchange = ccxt_async.bitget()
print(f"connection time for async exhange: {time.time() - now} seconds")