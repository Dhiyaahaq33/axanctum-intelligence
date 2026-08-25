"""
Loop pemantau harga live (OKX) + auto TP/SL untuk posisi terbuka di
axanctum-intelligence, dijalankan berkala lewat GitHub Actions
(.github/workflows/price-monitor.yml) - karena Vercel serverless
(server.py) tidak bisa menahan proses/background task yang nyala terus.

Postgres (Neon) jadi satu-satunya sumber kebenaran state: script ini baca
posisi terbuka, fetch harga OKX, cek TP/SL, dan tulis balik ke DB kalau ada
posisi yang harus ditutup. server.py (dashboard) cuma baca hasil akhirnya.

Loop internal tiap ~2 detik selama job masih dalam budget waktu (mirip pola
scan_once.py di axanctum-v2.test) - supaya TP/SL bereaksi cepat walau GitHub
Actions tidak bisa menahan proses selamanya.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import aiohttp
import asyncpg

TZ_WIB = timezone(timedelta(hours=7))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
OKX_BASE = "https://www.okx.com"

LOOP_BUDGET_SEC = float(os.environ.get("PRICE_MONITOR_BUDGET_SEC", 270))
LOOP_INTERVAL_SEC = float(os.environ.get("PRICE_MONITOR_INTERVAL_SEC", 2))


async def fetch_all_swap_prices(session: aiohttp.ClientSession) -> Dict[str, float]:
    """Ambil harga last price semua SWAP OKX dalam 1 request, key format
    Binance-style ('BTCUSDT') supaya cocok dengan format symbol yang dipakai
    di sim_positions."""
    try:
        async with session.get(
            f"{OKX_BASE}/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return {}
            body = await resp.json(content_type=None)
            out = {}
            for t in body.get("data", []):
                inst_id = t.get("instId", "")
                if not inst_id.endswith("-USDT-SWAP"):
                    continue
                base = inst_id[: -len("-USDT-SWAP")]
                symbol = f"{base}USDT"
                try:
                    out[symbol] = float(t.get("last", 0) or 0)
                except (TypeError, ValueError):
                    continue
            return out
    except Exception as exc:
        print(f"[price_monitor] fetch_all_swap_prices error: {exc}")
        return {}


async def load_open_positions(pool: asyncpg.Pool) -> List[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT data FROM sim_positions ORDER BY id")
        return [json.loads(r["data"]) for r in rows]


async def save_prices(pool: asyncpg.Pool, prices: Dict[str, float]) -> None:
    if not prices:
        return
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sim_prices(symbol, price, updated_at) VALUES($1,$2, now())
            ON CONFLICT(symbol) DO UPDATE SET price=EXCLUDED.price, updated_at=now()
            """,
            [(sym, price) for sym, price in prices.items()],
        )


async def close_position(pool: asyncpg.Pool, pos: dict, reason: str, exit_price: float) -> float:
    entry = float(pos["entry"])
    pct = (exit_price - entry) / entry if entry else 0.0
    pnl = (pct if pos["direction"] == "LONG" else -pct) * pos["margin"] * pos["leverage"]

    hist_entry = {
        "id":        int(time.time() * 1000),
        "time":      datetime.now(TZ_WIB).strftime("%d/%m %H:%M"),
        "symbol":    pos["symbol"],
        "direction": pos["direction"],
        "entry":     pos["entry"],
        "exit":      round(exit_price, 6),
        "pnl":       round(pnl, 4),
        "reason":    reason,
        "opened_at": pos.get("opened_at", ""),
        "tp":        pos["tp"],
        "sl":        pos["sl"],
    }

    async with pool.acquire() as conn:
        async with conn.transaction():
            bal_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='balance'")
            pnl_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='realized_pnl'")
            win_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='wins'")
            loss_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='losses'")

            balance = float(bal_row["value"]) if bal_row else 1000.0
            realized = float(pnl_row["value"]) if pnl_row else 0.0
            wins = int(win_row["value"]) if win_row else 0
            losses = int(loss_row["value"]) if loss_row else 0

            balance += pos["margin"] + pnl
            realized += pnl
            if pnl >= 0:
                wins += 1
            else:
                losses += 1

            for key, val in [("balance", balance), ("realized_pnl", realized), ("wins", wins), ("losses", losses)]:
                await conn.execute(
                    """
                    INSERT INTO sim_account(key, value) VALUES($1,$2)
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                    """,
                    key, float(val),
                )

            await conn.execute(
                """
                INSERT INTO sim_history(id, data) VALUES($1,$2)
                ON CONFLICT(id) DO NOTHING
                """,
                hist_entry["id"], json.dumps(hist_entry),
            )
            await conn.execute("DELETE FROM sim_positions WHERE id=$1", pos["id"])

    return pnl


async def check_and_close_tp_sl(pool: asyncpg.Pool, positions: List[dict], prices: Dict[str, float]) -> int:
    closed = 0
    for pos in positions:
        price = prices.get(pos["symbol"])
        if not price:
            continue
        tp = round(float(pos["tp"]), 8)
        sl = round(float(pos["sl"]), 8)
        cur = round(float(price), 8)
        reason = None
        if pos["direction"] == "LONG":
            if cur >= tp:
                reason = "TP"
            elif cur <= sl:
                reason = "SL"
        else:
            if cur <= tp:
                reason = "TP"
            elif cur >= sl:
                reason = "SL"
        if reason:
            print(f"[{reason}] {pos['symbol']} {pos['direction']} hit at {cur} (tp={tp} sl={sl})")
            await close_position(pool, pos, reason, price)
            closed += 1
    return closed


async def main_async() -> None:
    if not DATABASE_URL:
        print("[price_monitor] DATABASE_URL belum diset - exit.")
        return

    pool = await asyncpg.create_pool(DATABASE_URL, ssl="require", min_size=1, max_size=3)
    t_start = time.monotonic()
    pass_no = 0

    async with aiohttp.ClientSession() as session:
        while True:
            elapsed = time.monotonic() - t_start
            if elapsed >= LOOP_BUDGET_SEC:
                print(f"[price_monitor] budget {elapsed:.0f}s tercapai - stop loop.")
                break

            pass_no += 1
            positions = await load_open_positions(pool)
            all_prices = await fetch_all_swap_prices(session)

            relevant = {p["symbol"]: all_prices[p["symbol"]] for p in positions if p["symbol"] in all_prices}
            if relevant:
                await save_prices(pool, relevant)

            closed = 0
            if positions:
                closed = await check_and_close_tp_sl(pool, positions, relevant)

            if pass_no % 30 == 0 or closed:
                print(f"[price_monitor] pass {pass_no} elapsed={elapsed:.0f}s "
                      f"positions={len(positions)} prices_updated={len(relevant)} closed={closed}")

            await asyncio.sleep(LOOP_INTERVAL_SEC)

    await pool.close()
    print(f"[price_monitor] done - total {pass_no} pass dalam {time.monotonic()-t_start:.0f}s")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
