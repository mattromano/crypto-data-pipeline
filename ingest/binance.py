"""Binance public market-data source for dlt.

This is the centerpiece: it demonstrates the two things you hand-rolled in
Streamline with jinja loops + recursive CTEs, done declaratively with dlt:

  1. Pagination   -> advance `startTime` past the last bar each page, stop when
                     the API returns fewer rows than the page limit.
  2. Incremental  -> `dlt.sources.incremental` tracks a watermark (open_time)
     state            across runs and we PUSH that watermark into the request
                      (startTime=...), so re-runs only fetch new candles.

No API key required. Public host, no geo block:
    https://data-api.binance.vision/api/v3/klines

Swap the source by writing a sibling module:
  - DefiLlama : https://api.llama.fi/v2/historicalChainTvl/{chain}  (date cursor)
  - Etherscan : https://api.etherscan.io/api?module=account&action=txlist
                (free key, page + startblock cursor -- most on-brand for EVM work)
"""

from typing import Iterator

import dlt
from dlt.sources.helpers import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"
PAGE_LIMIT = 1000  # Binance max rows per request

# Binance returns each kline as a positional array; name the columns.
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
]

# 2024-01-01T00:00:00Z in epoch milliseconds -- where a cold start begins.
DEFAULT_START_MS = 1_704_067_200_000


@dlt.source(name="binance")
def binance_source(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    interval: str = "1h",
):
    """One source, one resource -> one `klines` table with a `symbol` column."""
    return klines(symbols=symbols, interval=interval)


@dlt.resource(
    name="klines",
    write_disposition="merge",          # upsert; safe to re-run / overlap
    primary_key=["symbol", "open_time"],  # dedupe key for the merge
)
def klines(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    interval: str = "1h",
    open_time: dlt.sources.incremental[int] = dlt.sources.incremental(
        "open_time", initial_value=DEFAULT_START_MS
    ),
) -> Iterator[dict]:
    # `open_time.last_value` is restored from dlt state between runs: the max
    # open_time we've ever loaded. Symbols share the same time grid (same
    # interval), so a single global watermark is correct for all of them.
    watermark = open_time.last_value

    for symbol in symbols:
        cursor = watermark  # local paging cursor for this symbol
        while True:
            resp = requests.get(
                BASE_URL,
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,   # <- watermark pushed into the request
                    "limit": PAGE_LIMIT,
                },
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break

            for row in rows:
                record = dict(zip(KLINE_COLUMNS, row))
                record["symbol"] = symbol
                record.pop("ignore", None)
                yield record

            # Last page: API returned a partial batch -> we're caught up.
            if len(rows) < PAGE_LIMIT:
                break
            # Advance past the last bar's open_time (ms) to fetch the next page.
            cursor = rows[-1][0] + 1
