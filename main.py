#!/usr/bin/env python3
"""Poll Pyth Hermes for multiple USD feeds; upsert key–value rows into PostgreSQL bbb.system."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import psycopg2
from dotenv import load_dotenv

log = logging.getLogger("coin-price-updator")


# Pyth price feed IDs (Solana mainnet / global feed ids) -> human symbol
PRICE_FEEDS: Dict[str, str] = {
    "SOL/USD": "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
    "BTC/USD": "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETH/USD": "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "USDT/USD": "0x2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b",
    # Hermes: Crypto.USDC/USD (old 0x41f36… returns 404 on /v2/updates/price/latest)
    "USDC/USD": "0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
    # Hermes: Crypto.USD1/USD (World Liberty Financial USD1)
    "USD1/USD": "0x0a2425d43486780990d8b63543029e20556be51fd756cca584212f4d539611d4",
}

# Pyth symbol -> DB key (sol-price matches solana-swap-listener default)
SYMBOL_TO_KEY: Dict[str, str] = {
    "SOL/USD": "sol-price",
    "BTC/USD": "btc-price",
    "ETH/USD": "eth-price",
    "USDT/USD": "usdt-price",
    "USDC/USD": "usdc-price",
    "USD1/USD": "usd1-price",
}

# Non-Pyth tokens priced via DexScreener. Used for platform/launchpad quote
# tokens that pair with subject tokens on EVM DEXs (evm-swap-listener consults
# these keys to USD-value swaps paired with these tokens). One entry per
# (db_key, chain, token_address). DexScreener is queried for the token's most
# liquid USD-quoted pair; priceUsd is taken from there.
DEX_TOKENS: List[Tuple[str, str, str]] = [
    # (db_key, chain, token_address)
    ("zora-price", "base", "0x1111111111166b7fe7bd91427724b487980afc69"),
]


def norm_feed_id(pid: str) -> str:
    s = (pid or "").strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    return s


def feed_id_to_symbol() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for sym, fid in PRICE_FEEDS.items():
        out[norm_feed_id(fid)] = sym
    return out


def hermes_latest_url(base: str, feed_hex_ids: List[str]) -> str:
    base = base.rstrip("/")
    q = [("ids[]", fid) for fid in feed_hex_ids]
    return f"{base}/v2/updates/price/latest?{urllib.parse.urlencode(q)}"


def fetch_hermes_json(url: str, timeout: float = 30.0) -> dict:
    # Hermes/WAF returns 403 for urllib's default User-Agent (Python-urllib/*).
    ua = os.environ.get(
        "PYTH_HTTP_USER_AGENT",
        "coin-price-updator/1.0 (+https://docs.pyth.network/)",
    ).strip() or "coin-price-updator/1.0"
    headers = {
        "Accept": "application/json",
        "User-Agent": ua,
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def pyth_human_price(price_obj: dict) -> float:
    raw = price_obj.get("price")
    expo = price_obj.get("expo")
    if raw is None or expo is None:
        raise ValueError(f"missing price/expo: {price_obj!r}")
    return float(int(str(raw))) * (10.0 ** int(expo))


def parse_prices(data: dict, id_to_symbol: Dict[str, str]) -> Dict[str, float]:
    parsed = data.get("parsed")
    if not isinstance(parsed, list):
        raise ValueError("response missing parsed[]")
    out: Dict[str, float] = {}
    for item in parsed:
        pid = norm_feed_id(str(item.get("id", "")))
        sym = id_to_symbol.get(pid)
        if not sym:
            continue
        p = item.get("price")
        if not isinstance(p, dict):
            continue
        out[sym] = pyth_human_price(p)
    return out


def fetch_dexscreener_price(chain: str, token_addr: str, timeout: float = 15.0) -> float:
    """Return the most-liquid USD-quoted priceUsd for `token_addr` on `chain`,
    or 0.0 on failure. We prefer the pair with the highest liquidity.usd to
    avoid wash-traded dust pools."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_addr}"
    # DexScreener's WAF rejects urllib's default UA with 403 — same pattern
    # as the Hermes endpoint.
    ua = os.environ.get(
        "DEXSCREENER_USER_AGENT",
        "coin-price-updator/1.0 (+https://github.com/anthropics)",
    ).strip() or "coin-price-updator/1.0"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": ua},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        log.warning("dexscreener %s/%s: %s", chain, token_addr, e)
        return 0.0
    pairs = data.get("pairs") or []
    chain_lc = chain.strip().lower()
    best_liq = 0.0
    best_price = 0.0
    for p in pairs:
        if (p.get("chainId") or "").strip().lower() != chain_lc:
            continue
        try:
            price = float(p.get("priceUsd") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        try:
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
        except (TypeError, ValueError):
            liq = 0.0
        if liq > best_liq:
            best_liq = liq
            best_price = price
    return best_price


def kv_rows_from_dex_tokens(now: datetime) -> List[Tuple[str, str, datetime]]:
    rows: List[Tuple[str, str, datetime]] = []
    for db_key, chain, addr in DEX_TOKENS:
        price = fetch_dexscreener_price(chain, addr)
        if price <= 0:
            log.warning("dexscreener returned no price for %s (%s/%s)", db_key, chain, addr)
            continue
        rows.append((db_key, f"{price:.12g}", now))
    return rows


def kv_rows_from_prices(prices: Dict[str, float]) -> List[Tuple[str, str, datetime]]:
    now = datetime.now(timezone.utc)
    rows: List[Tuple[str, str, datetime]] = []
    for sym, db_key in SYMBOL_TO_KEY.items():
        if sym not in prices:
            continue
        v = prices[sym]
        rows.append((db_key, f"{v:.12g}", now))
    return rows


def pg_connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1").strip(),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "bbbadmin").strip(),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get("POSTGRES_DB", "bbb").strip(),
    )


def upsert_rows(conn, table_fqn: str, rows: List[Tuple[str, str, datetime]]) -> None:
    if not rows:
        return
    sql = (
        f"INSERT INTO {table_fqn} (key, value, updated_at) VALUES (%s, %s, %s) "
        f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    hermes_base = os.environ.get("PYTH_HERMES_BASE", "https://hermes.pyth.network").strip()
    schema = os.environ.get("POSTGRES_SCHEMA", "bbb").strip() or "bbb"
    table = os.environ.get("POSTGRES_SYSTEM_TABLE", "system").strip() or "system"
    table_fqn = f'"{schema}"."{table}"'
    poll = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
    if poll < 10:
        poll = 10

    id_to_symbol = feed_id_to_symbol()
    feed_ids = list(PRICE_FEEDS.values())
    url = hermes_latest_url(hermes_base, feed_ids)

    conn = pg_connect()

    log.info(
        "Hermes %s — %d feeds; PG %s table=%s",
        hermes_base,
        len(feed_ids),
        os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        table_fqn,
    )

    sol_key = SYMBOL_TO_KEY["SOL/USD"]

    while True:
        try:
            data = fetch_hermes_json(url)
            prices = parse_prices(data, id_to_symbol)
            missing = [s for s in PRICE_FEEDS if s not in prices]
            if missing:
                log.warning("missing feeds: %s", ", ".join(missing))
            if "SOL/USD" not in prices or prices["SOL/USD"] <= 0:
                log.error("SOL/USD missing or invalid; skip insert (swap listener needs %r)", sol_key)
            else:
                rows = kv_rows_from_prices(prices)
                # Append DexScreener-sourced non-Pyth quote tokens (ZORA, …).
                dex_rows = kv_rows_from_dex_tokens(datetime.now(timezone.utc))
                rows.extend(dex_rows)
                if not rows:
                    continue
                try:
                    upsert_rows(conn, table_fqn, rows)
                except (psycopg2.InterfaceError, psycopg2.OperationalError) as db_err:
                    log.warning("PG connection lost (%s); reconnecting", db_err)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = pg_connect()
                    upsert_rows(conn, table_fqn, rows)
                log.info(
                    "updated %d keys (pyth + %d dex) e.g. %s=%.4f",
                    len(rows),
                    len(dex_rows),
                    sol_key,
                    prices.get("SOL/USD", 0),
                )
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            log.warning("network: %s", e)
        except Exception:
            log.exception("poll/insert failed")

        time.sleep(poll)


if __name__ == "__main__":
    main()
