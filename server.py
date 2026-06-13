"""
AXANCTUM INTELLIGENCE 911 — Simulation Server
Railway + PostgreSQL Edition
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta

TZ_WIB = timezone(timedelta(hours=7))  # UTC+7
from typing import Optional
from contextlib import asynccontextmanager

import uvicorn
import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ─── Config ──────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "zariba")
DATABASE_URL       = os.environ.get("DATABASE_URL", "")  # otomatis dari Railway
PORT               = int(os.environ.get("PORT", 8000))

# ─── In-memory state (harga & sinyal pending tidak perlu disimpan ke DB) ─────
sim_state = {
    "balance":      1000.0,
    "realized_pnl": 0.0,
    "wins":         0,
    "losses":       0,
    "positions":    [],
    "signals":      [],
    "history":      [],
    "prices":       {},
    "max_positions": 0,
    "default_leverage": 5,
    "default_margin_pct": 10,
    "auto_open": False,
}
connected_clients: list[WebSocket] = []
signal_id_counter = 1
db_pool = None
# Set symbol yang sedang dalam proses dibuka — mencegah race condition
_opening_symbols: set = set()

# ─── Database helpers ─────────────────────────────────────────────────────────

async def init_db():
    global db_pool
    if not DATABASE_URL:
        print("[DB] DATABASE_URL tidak ada, pakai in-memory saja")
        return
    db_pool = await asyncpg.create_pool(DATABASE_URL, ssl="require")
    async with db_pool.acquire() as conn:
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
        """)
    print("[DB] Database siap")

async def load_from_db():
    global signal_id_counter
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM sim_account")
        for r in rows:
            if r["key"] == "balance":           sim_state["balance"]      = r["value"]
            if r["key"] == "realized_pnl":      sim_state["realized_pnl"] = r["value"]
            if r["key"] == "wins":              sim_state["wins"]         = int(r["value"])
            if r["key"] == "losses":            sim_state["losses"]       = int(r["value"])
            if r["key"] == "signal_id_counter": signal_id_counter         = int(r["value"])
            if r["key"] == "auto_open":         sim_state["auto_open"]    = bool(r["value"])

        pos_rows = await conn.fetch("SELECT data FROM sim_positions")
        sim_state["positions"] = [json.loads(r["data"]) for r in pos_rows]

        hist_rows = await conn.fetch("SELECT data FROM sim_history ORDER BY id DESC LIMIT 100")
        sim_state["history"] = [json.loads(r["data"]) for r in hist_rows]

    print(f"[DB] Loaded: balance=${sim_state['balance']:.2f}, {len(sim_state['positions'])} posisi")

async def save_account():
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        for key, val in [
            ("balance",           sim_state["balance"]),
            ("realized_pnl",      sim_state["realized_pnl"]),
            ("wins",              sim_state["wins"]),
            ("losses",            sim_state["losses"]),
            ("signal_id_counter", float(signal_id_counter)),
            ("auto_open",         float(sim_state.get("auto_open", False))),
        ]:
            await conn.execute("""
                INSERT INTO sim_account(key, value) VALUES($1,$2)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
            """, key, float(val))

async def save_position(pos: dict):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sim_positions(id, data) VALUES($1,$2)
            ON CONFLICT(id) DO UPDATE SET data=EXCLUDED.data
        """, pos["id"], json.dumps(pos))

async def delete_position(pos_id: int):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM sim_positions WHERE id=$1", pos_id)

async def save_history(entry: dict):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sim_history(id, data) VALUES($1,$2)
            ON CONFLICT(id) DO NOTHING
        """, entry["id"], json.dumps(entry))

# ─── WebSocket broadcast ──────────────────────────────────────────────────────

async def broadcast(data: dict):
    dead = []
    for ws in connected_clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.remove(ws)

async def push_state():
    await broadcast({
        "type":         "state",
        "balance":      sim_state["balance"],
        "realized_pnl": sim_state["realized_pnl"],
        "wins":         sim_state["wins"],
        "losses":       sim_state["losses"],
        "positions":    sim_state["positions"],
        "signals":      sim_state["signals"],
        "history":      sim_state["history"][-50:],
        "prices":       sim_state["prices"],
        "max_positions":      sim_state.get("max_positions", 0),
        "default_leverage":   sim_state.get("default_leverage", 5),
        "default_margin_pct": sim_state.get("default_margin_pct", 10),
        "auto_open":          sim_state.get("auto_open", False),
    })

# ─── Binance price feed ───────────────────────────────────────────────────────

async def binance_price_feed():
    import websockets as ws_lib
    url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    while True:
        try:
            async with ws_lib.connect(url, ping_interval=20) as ws:
                print("[Binance] Terhubung — memantau SEMUA symbol")
                async for raw in ws:
                    tickers = json.loads(raw)
                    if isinstance(tickers, list):
                        for t in tickers:
                            sym = t.get("s", "")
                            price = float(t.get("c", 0))
                            if sym and price:
                                sim_state["prices"][sym] = price
                    await check_tp_sl()
        except Exception as e:
            print(f"[Binance] Putus: {e}. Reconnect 5s...")
            await asyncio.sleep(5)

async def check_tp_sl():
    to_close = []
    for pos in sim_state["positions"]:
        price = sim_state["prices"].get(pos["symbol"])
        if not price:
            # Coba fetch via REST sebelum skip
            price = await fetch_price_rest(pos["symbol"])
        if not price:
            print(f"[WARN] check_tp_sl: harga {pos['symbol']} tidak ditemukan via WS maupun REST")
            continue
        tp = round(float(pos["tp"]), 8)
        sl = round(float(pos["sl"]), 8)
        cur = round(float(price), 8)
        if pos["direction"] == "LONG":
            if cur >= tp:
                print(f"[TP] {pos['symbol']} LONG hit TP: price={cur} tp={tp}")
                to_close.append((pos["id"], "TP", price))
            elif cur <= sl:
                print(f"[SL] {pos['symbol']} LONG hit SL: price={cur} sl={sl}")
                to_close.append((pos["id"], "SL", price))
        else:
            if cur <= tp:
                print(f"[TP] {pos['symbol']} SHORT hit TP: price={cur} tp={tp}")
                to_close.append((pos["id"], "TP", price))
            elif cur >= sl:
                print(f"[SL] {pos['symbol']} SHORT hit SL: price={cur} sl={sl}")
                to_close.append((pos["id"], "SL", price))
    for pos_id, reason, exit_price in to_close:
        await _close_position(pos_id, reason, exit_price)
    if to_close:
        await push_state()

async def fetch_price_rest(symbol: str) -> float:
    """Fetch harga via REST sebagai fallback kalau tidak ada di WebSocket cache."""
    import aiohttp
    urls = [
        f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}",
        f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
    ]
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, ssl=False, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data.get("price", 0))
                        if price > 0:
                            sim_state["prices"][symbol] = price  # update cache
                            return price
            except Exception:
                continue
    return 0.0

async def price_broadcaster():
    while True:
        await asyncio.sleep(1)
        if connected_clients and sim_state["positions"]:
            pnl_list = []
            relevant_prices = {}
            for pos in sim_state["positions"]:
                sym = pos["symbol"]
                price = sim_state["prices"].get(sym, 0)
                # Kalau tidak ada di WebSocket cache, fetch via REST
                if price == 0:
                    price = await fetch_price_rest(sym)
                if price == 0:
                    price = pos["entry"]  # last resort fallback
                relevant_prices[sym] = round(price, 8)
                pct = (price - pos["entry"]) / pos["entry"] if pos["entry"] > 0 else 0
                pnl = (pct if pos["direction"] == "LONG" else -pct) * pos["margin"] * pos["leverage"]
                pnl_list.append({"id": pos["id"], "current_price": round(price, 8), "upnl": round(pnl, 4)})
            await broadcast({"type": "prices", "prices": relevant_prices, "positions_pnl": pnl_list})

async def _close_position(pos_id: int, reason: str, exit_price: Optional[float] = None):
    pos = next((p for p in sim_state["positions"] if p["id"] == pos_id), None)
    if not pos:
        return None
    price = exit_price or sim_state["prices"].get(pos["symbol"], pos["entry"])
    pct = (price - pos["entry"]) / pos["entry"]
    pnl = (pct if pos["direction"] == "LONG" else -pct) * pos["margin"] * pos["leverage"]
    sim_state["balance"] += pos["margin"] + pnl
    sim_state["realized_pnl"] += pnl
    if pnl >= 0: sim_state["wins"] += 1
    else:        sim_state["losses"] += 1
    entry = {
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
    sim_state["history"].append(entry)
    sim_state["positions"] = [p for p in sim_state["positions"] if p["id"] != pos_id]
    await delete_position(pos_id)
    await save_history(entry)
    await save_account()
    return pnl

# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await load_from_db()
    t1 = asyncio.create_task(binance_price_feed())
    t2 = asyncio.create_task(price_broadcaster())
    yield
    t1.cancel(); t2.cancel()
    if db_pool:
        await db_pool.close()

app = FastAPI(title="AXANCTUM INTELLIGENCE 911", lifespan=lifespan)

# ─── Models ───────────────────────────────────────────────────────────────────

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

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/login")
async def login(req: LoginRequest):
    if req.password == DASHBOARD_PASSWORD:
        return {"ok": True}
    raise HTTPException(401, "Password salah")

@app.post("/signal")
async def receive_signal(sig: Signal):
    global signal_id_counter
    signal = {
        "id": signal_id_counter, "symbol": sig.symbol.upper(),
        "direction": sig.direction.upper(), "entry": sig.entry,
        "tp": sig.tp, "sl": sig.sl, "grade": sig.grade,
        "leverage": sig.leverage, "source": sig.source,
        "time": datetime.now(TZ_WIB).strftime("%H:%M:%S"),
    }
    signal_id_counter += 1
    sim_state["signals"].append(signal)
    await save_account()
    await broadcast({"type": "new_signal", "signal": signal})
    await push_state()
    print(f"[Signal] {signal['symbol']} {signal['direction']}")

    # ── Auto-open logic ──────────────────────────────────────────────────
    if sim_state.get("auto_open", False):
        symbol = signal["symbol"]
        max_pos = sim_state.get("max_positions", 0)
        already_open = any(p["symbol"] == symbol for p in sim_state["positions"])

        if already_open or symbol in _opening_symbols:
            print(f"[AutoOpen] {symbol} sudah ada posisi terbuka, skip")
        elif max_pos > 0 and len(sim_state["positions"]) >= max_pos:
            print(f"[AutoOpen] Max posisi ({max_pos}) tercapai, skip")
        else:
            margin_pct = sim_state.get("default_margin_pct", 10) / 100
            leverage   = sim_state.get("default_leverage", 5)
            margin     = sim_state["balance"] * margin_pct
            if margin > 0 and sim_state["balance"] >= margin:
                _opening_symbols.add(symbol)
                try:
                    sim_state["balance"] -= margin
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
                    sim_state["positions"].append(pos)
                    sim_state["signals"] = [s for s in sim_state["signals"] if s["id"] != signal["id"]]
                    await save_position(pos)
                    await save_account()
                    await push_state()
                    print(f"[AutoOpen] ✅ {symbol} {signal['direction']} @ {signal['entry']}")
                finally:
                    _opening_symbols.discard(symbol)
            else:
                print(f"[AutoOpen] Saldo tidak cukup untuk {symbol}")
    # ────────────────────────────────────────────────────────────────────

    return {"ok": True, "signal_id": signal["id"]}


@app.post("/set-auto-open")
async def set_auto_open(enabled: bool):
    sim_state["auto_open"] = enabled
    await save_account()
    await push_state()
    return {"ok": True, "auto_open": enabled}

@app.post("/approve")
async def approve_signal(req: ApproveRequest):
    sig = next((s for s in sim_state["signals"] if s["id"] == req.signal_id), None)
    if not sig: raise HTTPException(404, "Sinyal tidak ditemukan")

    # Cek max posisi terbuka
    max_pos = sim_state.get("max_positions", 0)
    if max_pos > 0 and len(sim_state["positions"]) >= max_pos:
        sim_state["signals"] = [s for s in sim_state["signals"] if s["id"] != req.signal_id]
        return {"ok": False, "reason": f"Max posisi ({max_pos}) sudah tercapai"}

    # Cek deduplikasi — 1 posisi per symbol
    symbol = sig["symbol"]
    already_open = any(p["symbol"] == symbol for p in sim_state["positions"])
    if already_open or symbol in _opening_symbols:
        sim_state["signals"] = [s for s in sim_state["signals"] if s["id"] != req.signal_id]
        return {"ok": False, "reason": f"{symbol} sudah ada posisi terbuka"}

    # Lock symbol sementara proses dibuka
    _opening_symbols.add(symbol)
    try:
        margin = sim_state["balance"] * req.margin_pct
        if margin <= 0 or sim_state["balance"] < margin:
            raise HTTPException(400, "Saldo tidak cukup")
        sim_state["balance"] -= margin
        pos = {
        "id": int(time.time() * 1000), "symbol": sig["symbol"],
        "direction": sig["direction"], "entry": sig["entry"],
        "tp": req.tp or sig["tp"], "sl": req.sl or sig["sl"],
        "leverage": req.leverage or sig["leverage"],
        "margin": round(margin, 4),
        "opened_at": datetime.now(TZ_WIB).strftime("%d/%m %H:%M"),
    }
        sim_state["positions"].append(pos)
        sim_state["signals"] = [s for s in sim_state["signals"] if s["id"] != req.signal_id]
        await save_position(pos)
        await save_account()
        await push_state()
        return {"ok": True, "position_id": pos["id"]}
    finally:
        _opening_symbols.discard(symbol)

@app.post("/reject/{signal_id}")
async def reject_signal(signal_id: int):
    sim_state["signals"] = [s for s in sim_state["signals"] if s["id"] != signal_id]
    await push_state()
    return {"ok": True}

@app.post("/close")
async def close_position(req: CloseRequest):
    pnl = await _close_position(req.position_id, req.reason)
    if pnl is None: raise HTTPException(404, "Posisi tidak ditemukan")
    await push_state()
    return {"ok": True, "pnl": pnl}

@app.post("/update-position/{pos_id}")
async def update_position(pos_id: int, tp: Optional[float] = None, sl: Optional[float] = None):
    pos = next((p for p in sim_state["positions"] if p["id"] == pos_id), None)
    if not pos: raise HTTPException(404, "Posisi tidak ditemukan")
    if tp: pos["tp"] = tp
    if sl: pos["sl"] = sl
    await save_position(pos)
    return {"ok": True}

@app.post("/set-max-positions")
async def set_max_positions(max_pos: int):
    sim_state["max_positions"] = max(0, max_pos)
    await push_state()
    return {"ok": True, "max_positions": sim_state["max_positions"]}

class SettingsRequest(BaseModel):
    default_leverage: Optional[int] = None
    default_margin_pct: Optional[float] = None

@app.post("/save-settings")
async def save_settings(req: SettingsRequest):
    if req.default_leverage is not None:
        sim_state["default_leverage"] = max(1, req.default_leverage)
    if req.default_margin_pct is not None:
        sim_state["default_margin_pct"] = max(1, min(100, req.default_margin_pct))
    await push_state()
    return {"ok": True}

@app.post("/deposit")
async def deposit(req: DepositRequest):
    if req.amount == 0: raise HTTPException(400, "Jumlah tidak boleh 0")
    sim_state["balance"] += req.amount
    await save_account()
    await push_state()
    return {"ok": True, "balance": sim_state["balance"]}

@app.post("/set-balance")
async def set_balance(req: DepositRequest):
    if req.amount <= 0: raise HTTPException(400, "Saldo harus lebih dari 0")
    sim_state["balance"] = req.amount
    await save_account()
    await push_state()
    return {"ok": True, "balance": sim_state["balance"]}

@app.post("/reset")
async def reset():
    sim_state.update({"balance":1000.0,"realized_pnl":0.0,"wins":0,"losses":0,"positions":[],"signals":[],"history":[]})
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM sim_positions; DELETE FROM sim_history; DELETE FROM sim_account;")
    await push_state()
    return {"ok": True}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    await push_state()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)

@app.get("/klines/{symbol}")
async def get_klines(symbol: str, interval: str = "15m", limit: int = 100, endTime: Optional[int] = None):
    """
    Proxy chart — coba Futures dulu, fallback ke Spot.
    endTime: unix timestamp ms, untuk chart history yang akurat.
    """
    import aiohttp
    end_param = f"&endTime={endTime}" if endTime else ""
    urls = [
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}{end_param}",
        f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}{end_param}",
    ]
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, ssl=False, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            return data
            except Exception:
                continue
    return {"error": "Tidak bisa fetch data chart"}

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    with open("dashboard.html", "r") as f:
        return f.read()

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
