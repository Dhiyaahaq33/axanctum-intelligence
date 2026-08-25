"""
AXANCTUM INTELLIGENCE 911 - Simulation Server
Vercel Serverless + Neon Postgres Edition

Beda dari versi lama (Railway + PostgreSQL, proses nyala terus dengan
WebSocket + background asyncio task): Vercel serverless functions itu
stateless per-invocation (tidak ada proses yang nyala terus, tidak bisa
nahan background task, dan memori antar-request tidak dijamin sama).

Jadi arsitekturnya diubah:
- Semua state (account, positions, signals, history, prices) dibaca ulang
  dari Postgres di AWAL tiap request (bukan cuma sekali di startup), dan
  ditulis ke Postgres tiap ada perubahan - Postgres jadi single source of
  truth, bukan variabel Python di memori.
- WebSocket dihapus total (Vercel serverless tidak bisa nahan koneksi
  persisten) - diganti endpoint GET /state yang di-poll dari frontend
  tiap beberapa detik.
- Loop pemantau harga real-time + auto TP/SL (dulu binance_price_feed +
  check_tp_sl jalan sebagai background task di proses ini) dipindah total
  ke script terpisah (price_monitor_once.py), dijalankan berkala lewat
  GitHub Actions - baca posisi dari DB, cek harga live, tutup posisi yang
  kena TP/SL langsung ke DB. Server ini hanya baca hasil akhirnya.
- Sumber harga chart (/klines) pindah dari Binance ke OKX, karena Binance
  memblokir IP US (termasuk region default Vercel serverless functions).
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

TZ_WIB = timezone(timedelta(hours=7))

# --- Config ------------------------------------------------------------------
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "zariba")
PORTFOLIO_PASSWORD = os.environ.get("PORTFOLIO_PASSWORD", "TRADER123")
DATABASE_URL       = os.environ.get("DATABASE_URL", "")

_db_pool: Optional[asyncpg.Pool] = None
_db_ready = False


async def get_pool() -> asyncpg.Pool:
    """Buat/ambil connection pool. Di-cache per-instance Vercel (warm reuse
    kalau instance yang sama dipakai lagi untuk request berikutnya)."""
    global _db_pool, _db_ready
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL belum diset")
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(DATABASE_URL, ssl="require", min_size=0, max_size=3)
    if not _db_ready:
        async with _db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sim_account (
                    key TEXT PRIMARY KEY,
                    value DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS sim_positions (
                    id BIGINT PRIMARY KEY,
                    data JSONB
                );
                CREATE TABLE IF NOT EXISTS sim_history (
                    id BIGINT PRIMARY KEY,
                    data JSONB
                );
                CREATE TABLE IF NOT EXISTS sim_signals (
                    id BIGINT PRIMARY KEY,
                    data JSONB
                );
                CREATE TABLE IF NOT EXISTS sim_prices (
                    symbol TEXT PRIMARY KEY,
                    price DOUBLE PRECISION,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
        _db_ready = True
    return _db_pool


DEFAULT_ACCOUNT = {
    "balance":            1000.0,
    "realized_pnl":       0.0,
    "wins":               0,
    "losses":             0,
    "max_positions":      0,
    "default_leverage":   5,
    "default_margin_pct": 10,
    "auto_open":          False,
    "signal_id_counter":  1,
}


async def load_state() -> dict:
    """Baca seluruh state simulasi dari Postgres - dipanggil di awal tiap
    request supaya selalu dapat data terbaru (tidak ada memori antar-request
    yang bisa diandalkan di serverless)."""
    pool = await get_pool()
    state = dict(DEFAULT_ACCOUNT)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM sim_account")
        for r in rows:
            key = r["key"]
            if key in ("wins", "losses", "signal_id_counter"):
                state[key] = int(r["value"])
            elif key == "auto_open":
                state[key] = bool(r["value"])
            else:
                state[key] = r["value"]

        pos_rows = await conn.fetch("SELECT data FROM sim_positions ORDER BY id")
        state["positions"] = [json.loads(r["data"]) for r in pos_rows]

        sig_rows = await conn.fetch("SELECT data FROM sim_signals ORDER BY id")
        state["signals"] = [json.loads(r["data"]) for r in sig_rows]

        hist_rows = await conn.fetch("SELECT data FROM sim_history ORDER BY id DESC LIMIT 100")
        state["history"] = [json.loads(r["data"]) for r in hist_rows]

        price_rows = await conn.fetch("SELECT symbol, price FROM sim_prices")
        state["prices"] = {r["symbol"]: r["price"] for r in price_rows}

    return state


async def save_account(state: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        for key in (
            "balance", "realized_pnl", "wins", "losses",
            "signal_id_counter", "auto_open", "max_positions",
            "default_leverage", "default_margin_pct",
        ):
            await conn.execute(
                """
                INSERT INTO sim_account(key, value) VALUES($1,$2)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                """,
                key, float(state.get(key, 0) or 0),
            )


async def save_position(pos: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sim_positions(id, data) VALUES($1,$2)
            ON CONFLICT(id) DO UPDATE SET data=EXCLUDED.data
            """,
            pos["id"], json.dumps(pos),
        )


async def delete_position(pos_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM sim_positions WHERE id=$1", pos_id)


async def save_signal(sig: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sim_signals(id, data) VALUES($1,$2)
            ON CONFLICT(id) DO UPDATE SET data=EXCLUDED.data
            """,
            sig["id"], json.dumps(sig),
        )


async def delete_signal(sig_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM sim_signals WHERE id=$1", sig_id)


async def save_history(entry: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sim_history(id, data) VALUES($1,$2)
            ON CONFLICT(id) DO NOTHING
            """,
            entry["id"], json.dumps(entry),
        )


def _state_response(state: dict) -> dict:
    return {
        "type":               "state",
        "balance":            state["balance"],
        "realized_pnl":       state["realized_pnl"],
        "wins":               state["wins"],
        "losses":             state["losses"],
        "positions":          state["positions"],
        "signals":            state["signals"],
        "history":            state["history"][:50],
        "prices":             state["prices"],
        "max_positions":      state.get("max_positions", 0),
        "default_leverage":   state.get("default_leverage", 5),
        "default_margin_pct": state.get("default_margin_pct", 10),
        "auto_open":          state.get("auto_open", False),
    }


async def _close_position_in_state(state: dict, pos_id: int, reason: str, exit_price: Optional[float] = None):
    """
    Tutup posisi secara atomik (DELETE ... RETURNING sebagai klaim) supaya
    aman dari race condition dengan price_monitor_once.py (GitHub Actions)
    yang juga bisa menutup posisi yang sama persis di waktu bersamaan kalau
    TP/SL kena tepat saat user klik close manual.
    """
    pos = next((p for p in state["positions"] if p["id"] == pos_id), None)
    if not pos:
        return None
    price = exit_price or state["prices"].get(pos["symbol"], pos["entry"])
    pct = (price - pos["entry"]) / pos["entry"] if pos["entry"] else 0
    pnl = (pct if pos["direction"] == "LONG" else -pct) * pos["margin"] * pos["leverage"]

    hist_entry = {
        "id":        int(time.time() * 1000),
        "time":      datetime.now(TZ_WIB).strftime("%d/%m %H:%M"),
        "symbol":    pos["symbol"],
        "direction": pos["direction"],
        "entry":     pos["entry"],
        "exit":      round(price, 6),
        "pnl":       round(pnl, 4),
        "reason":    reason,
        "opened_at": pos.get("opened_at", ""),
        "tp":        pos["tp"],
        "sl":        pos["sl"],
    }

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            claimed = await conn.fetchrow("DELETE FROM sim_positions WHERE id=$1 RETURNING id", pos_id)
            if not claimed:
                # Sudah ditutup proses lain (mis. price_monitor_once.py TP/SL) - batalkan.
                return None

            bal_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='balance' FOR UPDATE")
            pnl_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='realized_pnl' FOR UPDATE")
            win_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='wins' FOR UPDATE")
            loss_row = await conn.fetchrow("SELECT value FROM sim_account WHERE key='losses' FOR UPDATE")

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

    return pnl


app = FastAPI(title="AXANCTUM INTELLIGENCE 911")

# --- Models --------------------------------------------------------------
class Signal(BaseModel):
    symbol: str; direction: str; entry: float; tp: float; sl: float
    grade: str = "B"; leverage: int = 5; source: str = "bot"

class ApproveRequest(BaseModel):
    signal_id: int; leverage: Optional[int] = None
    tp: Optional[float] = None; sl: Optional[float] = None
    margin_pct: float = 0.1

class CloseRequest(BaseModel):
    position_id: int; reason: str = "Manual"

class DepositRequest(BaseModel):
    amount: float

class LoginRequest(BaseModel):
    password: str

class PortfolioAuthRequest(BaseModel):
    password: str

class SettingsRequest(BaseModel):
    default_leverage: Optional[int] = None
    default_margin_pct: Optional[float] = None


# --- Endpoints -------------------------------------------------------------

@app.get("/state")
async def get_state():
    state = await load_state()
    return _state_response(state)


@app.post("/login")
async def login(req: LoginRequest):
    if req.password == DASHBOARD_PASSWORD:
        return {"ok": True}
    raise HTTPException(401, "Password salah")


@app.post("/verify-portfolio-password")
async def verify_portfolio_password(req: PortfolioAuthRequest):
    """Password kedua (terpisah dari login dashboard) untuk aksi kelola
    portofolio: deposit/withdraw/set-balance/settings/max-positions/auto-open/reset."""
    if req.password == PORTFOLIO_PASSWORD:
        return {"ok": True}
    raise HTTPException(401, "Password salah")


@app.post("/signal")
async def receive_signal(sig: Signal):
    state = await load_state()
    signal_id = state["signal_id_counter"]
    signal = {
        "id": signal_id, "symbol": sig.symbol.upper(),
        "direction": sig.direction.upper(), "entry": sig.entry,
        "tp": sig.tp, "sl": sig.sl, "grade": sig.grade,
        "leverage": sig.leverage, "source": sig.source,
        "time": datetime.now(TZ_WIB).strftime("%H:%M:%S"),
    }
    state["signal_id_counter"] = signal_id + 1
    state["signals"].append(signal)
    await save_signal(signal)
    await save_account(state)
    print(f"[Signal] {signal['symbol']} {signal['direction']}")

    # -- Auto-open logic --
    if state.get("auto_open", False):
        symbol = signal["symbol"]
        max_pos = state.get("max_positions", 0)
        already_open = any(p["symbol"] == symbol for p in state["positions"])

        if already_open:
            print(f"[AutoOpen] {symbol} sudah ada posisi terbuka, skip")
        elif max_pos > 0 and len(state["positions"]) >= max_pos:
            print(f"[AutoOpen] Max posisi ({max_pos}) tercapai, skip")
        else:
            margin_pct = state.get("default_margin_pct", 10) / 100
            leverage   = state.get("default_leverage", 5)
            margin     = state["balance"] * margin_pct
            if margin > 0 and state["balance"] >= margin:
                state["balance"] -= margin
                pos = {
                    "id":        int(time.time() * 1000),
                    "symbol":    symbol,
                    "direction": signal["direction"],
                    "entry":     signal["entry"],
                    "tp":        signal["tp"],
                    "sl":        signal["sl"],
                    "leverage":  leverage,
                    "margin":    round(margin, 4),
                    "opened_at": datetime.now(TZ_WIB).strftime("%d/%m %H:%M"),
                }
                state["positions"].append(pos)
                state["signals"] = [s for s in state["signals"] if s["id"] != signal["id"]]
                await save_position(pos)
                await delete_signal(signal["id"])
                await save_account(state)
                print(f"[AutoOpen] {symbol} {signal['direction']} @ {signal['entry']}")
            else:
                print(f"[AutoOpen] Saldo tidak cukup untuk {symbol}")

    return {"ok": True, "signal_id": signal["id"]}


@app.post("/set-auto-open")
async def set_auto_open(enabled: bool):
    state = await load_state()
    state["auto_open"] = enabled
    await save_account(state)
    return {"ok": True, "auto_open": enabled}


@app.post("/approve")
async def approve_signal(req: ApproveRequest):
    state = await load_state()
    sig = next((s for s in state["signals"] if s["id"] == req.signal_id), None)
    if not sig:
        raise HTTPException(404, "Sinyal tidak ditemukan")

    max_pos = state.get("max_positions", 0)
    if max_pos > 0 and len(state["positions"]) >= max_pos:
        await delete_signal(req.signal_id)
        return {"ok": False, "reason": f"Max posisi ({max_pos}) sudah tercapai"}

    symbol = sig["symbol"]
    already_open = any(p["symbol"] == symbol for p in state["positions"])
    if already_open:
        await delete_signal(req.signal_id)
        return {"ok": False, "reason": f"{symbol} sudah ada posisi terbuka"}

    margin = state["balance"] * req.margin_pct
    if margin <= 0 or state["balance"] < margin:
        raise HTTPException(400, "Saldo tidak cukup")
    state["balance"] -= margin
    pos = {
        "id": int(time.time() * 1000), "symbol": sig["symbol"],
        "direction": sig["direction"], "entry": sig["entry"],
        "tp": req.tp or sig["tp"], "sl": req.sl or sig["sl"],
        "leverage": req.leverage or sig["leverage"],
        "margin": round(margin, 4),
        "opened_at": datetime.now(TZ_WIB).strftime("%d/%m %H:%M"),
    }
    await save_position(pos)
    await delete_signal(req.signal_id)
    await save_account(state)
    return {"ok": True, "position_id": pos["id"]}


@app.post("/reject/{signal_id}")
async def reject_signal(signal_id: int):
    await delete_signal(signal_id)
    return {"ok": True}


@app.post("/close")
async def close_position(req: CloseRequest):
    state = await load_state()
    pnl = await _close_position_in_state(state, req.position_id, req.reason)
    if pnl is None:
        raise HTTPException(404, "Posisi tidak ditemukan")
    return {"ok": True, "pnl": pnl}


@app.post("/update-position/{pos_id}")
async def update_position(pos_id: int, tp: Optional[float] = None, sl: Optional[float] = None):
    state = await load_state()
    pos = next((p for p in state["positions"] if p["id"] == pos_id), None)
    if not pos:
        raise HTTPException(404, "Posisi tidak ditemukan")
    if tp:
        pos["tp"] = tp
    if sl:
        pos["sl"] = sl
    await save_position(pos)
    return {"ok": True}


@app.post("/set-max-positions")
async def set_max_positions(max_pos: int):
    state = await load_state()
    state["max_positions"] = max(0, max_pos)
    await save_account(state)
    return {"ok": True, "max_positions": state["max_positions"]}


@app.post("/save-settings")
async def save_settings(req: SettingsRequest):
    state = await load_state()
    if req.default_leverage is not None:
        state["default_leverage"] = max(1, req.default_leverage)
    if req.default_margin_pct is not None:
        state["default_margin_pct"] = max(1, min(100, req.default_margin_pct))
    await save_account(state)
    return {"ok": True}


@app.post("/deposit")
async def deposit(req: DepositRequest):
    if req.amount == 0:
        raise HTTPException(400, "Jumlah tidak boleh 0")
    state = await load_state()
    state["balance"] += req.amount
    await save_account(state)
    return {"ok": True, "balance": state["balance"]}


@app.post("/set-balance")
async def set_balance(req: DepositRequest):
    if req.amount <= 0:
        raise HTTPException(400, "Saldo harus lebih dari 0")
    state = await load_state()
    state["balance"] = req.amount
    await save_account(state)
    return {"ok": True, "balance": state["balance"]}


@app.post("/reset")
async def reset():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM sim_positions; DELETE FROM sim_history; "
            "DELETE FROM sim_signals; DELETE FROM sim_account;"
        )
    return {"ok": True}


@app.get("/klines/{symbol}")
async def get_klines(symbol: str, interval: str = "15m", limit: int = 100, endTime: Optional[int] = None):
    """
    Proxy chart candle - pakai OKX (Binance memblokir IP US/Vercel serverless).
    symbol format tetap Binance-style ('BTCUSDT') supaya frontend tidak perlu
    diubah - diterjemahkan ke instId OKX di sini.
    """
    import aiohttp

    base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
    inst_id = f"{base.upper()}-USDT-SWAP"
    bar_map = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "8h": "8H",
        "12h": "12H", "1d": "1Dutc",
    }
    bar = bar_map.get(interval, interval)
    params = f"instId={inst_id}&bar={bar}&limit={limit}"
    if endTime:
        params += f"&before={endTime}"
    url = f"https://www.okx.com/api/v5/market/candles?{params}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    rows = body.get("data", [])
                    rows = list(reversed(rows))  # OKX newest-first -> oldest-first
                    # Format ulang ke shape Binance-kline-like supaya chart lib
                    # di frontend (yang mengharap [openTime,o,h,l,c,vol,...]) tetap jalan.
                    return [
                        [int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[0]), r[7], 0, r[6], "0", "0"]
                        for r in rows
                    ]
    except Exception:
        pass
    return {"error": "Tidak bisa fetch data chart"}


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "dashboard.html"), "r", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
