import os
import hmac
import json
import time
import math
import hashlib
import logging
import urllib.parse
from datetime import datetime, timezone

import azure.functions as func
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

# ------------------ ENV & CONFIG ------------------

def _get_env(name: str, default: str | None = None, required: bool = False) -> str | None:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try: return int(v) if v is not None else default
    except: return default

def _get_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try: return float(v) if v is not None else default
    except: return default

def _parse_pairs_models(val: str) -> list[tuple[str, str]]:
    """
    PAIRS_MODELS = 'XRPUSDT:BS_MedianScore,BTCUSDT:BS_MedianScore'
    -> [('XRPUSDT','BS_MedianScore'), ('BTCUSDT','BS_MedianScore')]
    """
    res = []
    if not val: return res
    parts = [p.strip() for p in val.split(",") if p.strip()]
    for p in parts:
        if ":" not in p: 
            continue
        pair, model = p.split(":", 1)
        res.append((pair.strip().upper(), model.strip()))
    return res

def _parse_overrides(env_val: str) -> dict[str, float]:
    """
    'XRPUSDT:4,TRXUSDT:2' -> {'XRPUSDT':4, 'TRXUSDT':2}
    """
    out = {}
    if not env_val: return out
    for piece in env_val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece: 
            continue
        k, v = piece.split(":", 1)
        k = k.strip().upper()
        try:
            out[k] = float(v.strip())
        except:
            continue
    return out

# Strategy thresholds
MIN_CYCLES_GLOBAL = _get_int("MIN_CYCLES_PER_DAY", 3)
MIN_SCORE_GLOBAL  = _get_float("MIN_SCORE", 0.001)
OVR_CYCLES = _parse_overrides(os.getenv("MIN_CYCLES_PER_DAY_OVERRIDES", ""))
OVR_SCORE  = _parse_overrides(os.getenv("MIN_SCORE_OVERRIDES", ""))

# Storage (one account, multiple containers)
WEBJOBS_CONN      = _get_env("AzureWebJobsStorage", required=True)
SIGNALS_CONTAINER = _get_env("SIGNALS_CONTAINER", "market-signals")
MASTER_CSV_NAME   = _get_env("MASTER_CSV_NAME", "bs_levels_master.csv")
TRADES_CONTAINER  = _get_env("TRADES_CONTAINER", "trade-logs")
STATE_CONTAINER   = _get_env("STATE_CONTAINER", "bot-state")

# Binance
BINANCE_BASE_URL   = _get_env("BINANCE_BASE_URL", "https://testnet.binance.vision")
BINANCE_API_KEY    = _get_env("BINANCE_API_KEY", required=True)
BINANCE_API_SECRET = _get_env("BINANCE_API_SECRET", required=True)

QUOTE_ASSET = _get_env("QUOTE_ASSET", "USDT").upper()
TRADE_USDT_PER_ORDER = _get_float("TRADE_USDT_PER_ORDER", 50.0)
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"

TIMEOUT_SEC_PER_TICK = _get_int("TIMEOUT_SEC_PER_TICK", 20)

# Pairs
PAIRS_MODELS = _parse_pairs_models(os.getenv("PAIRS_MODELS", ""))

logger = logging.getLogger("BinanceTradeBot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ------------------ AZURE STORAGE HELPERS ------------------

blob_service = BlobServiceClient.from_connection_string(WEBJOBS_CONN)
signals_cc: ContainerClient = blob_service.get_container_client(SIGNALS_CONTAINER)
trades_cc:  ContainerClient = blob_service.get_container_client(TRADES_CONTAINER)
state_cc:   ContainerClient = blob_service.get_container_client(STATE_CONTAINER)

for cc in (signals_cc, trades_cc, state_cc):
    try:
        cc.create_container()
    except ResourceExistsError:
        pass

def _state_blob_name(pair: str, model: str) -> str:
    return f"{pair}_{model}.json"

def load_state(pair: str, model: str) -> dict:
    blob = state_cc.get_blob_client(_state_blob_name(pair, model))
    try:
        data = blob.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except ResourceNotFoundError:
        return {
            "position": "flat",  # flat|long|short
            "qty": 0.0,
            "avg_price": 0.0,
            "last_order_id": None,
            "last_fill_time": None,
            "B": None, "S": None,
            "session_tag": None
        }

def save_state(pair: str, model: str, state: dict):
    blob = state_cc.get_blob_client(_state_blob_name(pair, model))
    data = json.dumps(state, separators=(",", ":")).encode("utf-8")
    blob.upload_blob(data, overwrite=True)

def ensure_trades_csv(pair: str):
    blob = trades_cc.get_blob_client(f"{pair}_trades.csv")
    append_client = blob.as_append_blob_client()
    try:
        append_client.create_blob()
        hdr = ("time_utc,model,pair,side,qty,avg_price,quote_usdt,fee,fee_asset,"
               "order_id,b_level,s_level,pnl_pct_since_open\n")
        append_client.append_block(hdr.encode("utf-8"))
    except ResourceExistsError:
        # už existuje
        pass

def append_trade(pair: str, row: dict):
    ensure_trades_csv(pair)
    blob = trades_cc.get_blob_client(f"{pair}_trades.csv")
    append_client = blob.as_append_blob_client()
    line = ",".join([
        row.get("time_utc",""),
        row.get("model",""),
        row.get("pair",""),
        row.get("side",""),
        f"{row.get('qty',0):.8f}",
        f"{row.get('avg_price',0):.8f}",
        f"{row.get('quote_usdt',0):.8f}",
        f"{row.get('fee',0):.8f}",
        row.get("fee_asset",""),
        str(row.get("order_id","")),
        f"{row.get('b_level',0):.8f}",
        f"{row.get('s_level',0):.8f}",
        f"{row.get('pnl_pct_since_open',0):.6f}"
    ]) + "\n"
    append_client.append_block(line.encode("utf-8"))

# ------------------ SIGNALS (B/S) ------------------

def _min_cycles_for(pair: str) -> int:
    return int(OVR_CYCLES.get(pair.upper(), MIN_CYCLES_GLOBAL))

def _min_score_for(pair: str) -> float:
    return float(OVR_SCORE.get(pair.upper(), MIN_SCORE_GLOBAL))

def load_active_signal(pair: str, model: str) -> dict | None:
    """Vrátí dict s B,S pro nejnovější aktivní řádek splňující prahy nebo None."""
    blob = signals_cc.get_blob_client(MASTER_CSV_NAME)
    try:
        data = blob.download_blob().readall()
    except ResourceNotFoundError:
        logger.warning(f"Master CSV '{MASTER_CSV_NAME}' not found in {SIGNALS_CONTAINER}.")
        return None

    df = pd.read_csv(pd.io.common.BytesIO(data))
    need_cols = {"pair","model","is_active","B","S","total_cycles","score","date","load_time_utc"}
    if not need_cols.issubset(df.columns):
        logger.warning(f"Master CSV missing required columns. Have: {df.columns.tolist()}")
        return None

    f = df[
        (df["pair"].str.upper() == pair.upper()) &
        (df["model"] == model) &
        (df["is_active"] == True) &
        (df["total_cycles"] >= _min_cycles_for(pair)) &
        (df["score"] >= _min_score_for(pair))
    ].copy()

    if f.empty:
        return None

    # seřaď podle date, pak load_time_utc
    f["date"] = pd.to_datetime(f["date"], errors="coerce")
    f["load_time_utc"] = pd.to_datetime(f["load_time_utc"], errors="coerce", utc=True)
    f = f.sort_values(["date","load_time_utc"], ascending=[True, True])

    row = f.iloc[-1]
    return {
        "B": float(row["B"]),
        "S": float(row["S"]),
        "date": str(row["date"].date()) if pd.notna(row["date"]) else None,
        "load_time_utc": row["load_time_utc"].strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(row["load_time_utc"]) else None
    }

# ------------------ BINANCE REST ------------------

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(params: dict, secret: str) -> str:
    qs = urllib.parse.urlencode(params, doseq=True)
    return hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()

def _headers() -> dict:
    return {"X-MBX-APIKEY": BINANCE_API_KEY, "Content-Type": "application/x-www-form-urlencoded"}

def binance_get(path: str, params: dict | None = None, signed: bool = False):
    url = f"{BINANCE_BASE_URL}{path}"
    params = params or {}
    if signed:
        params["timestamp"] = _ts_ms()
        params["recvWindow"] = 5000
        params["signature"] = _sign(params, BINANCE_API_SECRET)
    r = requests.get(url, params=params, headers=_headers(), timeout=20)
    if r.status_code == 200:
        return r.json()
    raise RuntimeError(f"GET {path} {r.status_code}: {r.text[:200]}")

def binance_post(path: str, params: dict, signed: bool = True):
    url = f"{BINANCE_BASE_URL}{path}"
    if signed:
        params["timestamp"] = _ts_ms()
        params["recvWindow"] = 5000
        params["signature"] = _sign(params, BINANCE_API_SECRET)
    r = requests.post(url, data=params, headers=_headers(), timeout=20)
    if r.status_code in (200, 201):
        return r.json()
    raise RuntimeError(f"POST {path} {r.status_code}: {r.text[:200]}")

def get_price(pair: str) -> float:
    data = binance_get("/api/v3/ticker/price", {"symbol": pair})
    return float(data["price"])

def get_exchange_info(pair: str) -> dict:
    data = binance_get("/api/v3/exchangeInfo", {"symbol": pair})
    return data

def round_step(value: float, step: float) -> float:
    if step <= 0: 
        return value
    return math.floor(value / step) * step

def symbol_filters(info: dict) -> dict:
    filters = {f["filterType"]: f for f in info["symbols"][0]["filters"]}
    lot = filters.get("LOT_SIZE", {})
    price = filters.get("PRICE_FILTER", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    return {
        "stepSize": float(lot.get("stepSize", "0.00000001")),
        "tickSize": float(price.get("tickSize", "0.00000001")),
        "minNotional": float(min_notional.get("minNotional", "0.0"))
    }

# ------------------ TRADING LOGIC (long-only default) ------------------

def trade_tick(pair: str, model: str):
    start = time.time()

    sig = load_active_signal(pair, model)
    if not sig:
        logger.info(f"[{pair}] No active signal passing thresholds — skipping.")
        return

    B = sig["B"]; S = sig["S"]

    st = load_state(pair, model)
    # Update state with today's B/S (pro jistotu)
    st["B"] = B; st["S"] = S
    st["session_tag"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    save_state(pair, model, st)

    # Exchange constraints
    info = get_exchange_info(pair)
    filt = symbol_filters(info)

    # Last price
    price = get_price(pair)

    # Decision
    if st["position"] == "flat":
        # long-only open rule
        if price <= B:
            # Market BUY using quoteOrderQty in USDT
            params = {
                "symbol": pair,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{TRADE_USDT_PER_ORDER:.2f}"
            }
            resp = binance_post("/api/v3/order", params)
            qty = sum(float(f["qty"]) for f in resp.get("fills", [])) or float(resp.get("executedQty", 0))
            # average fill price
            fills = resp.get("fills", [])
            if fills:
                total_quote = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                total_qty = sum(float(f["qty"]) for f in fills) or 1e-12
                avg_price = total_quote / total_qty
                fee = sum(float(f.get("commission", 0)) for f in fills)
                fee_asset = fills[0].get("commissionAsset", QUOTE_ASSET)
            else:
                avg_price = float(resp.get("cummulativeQuoteQty", 0)) / max(float(resp.get("executedQty", 0)) or 1e-12, 1e-12)
                fee = 0.0; fee_asset = QUOTE_ASSET

            st["position"] = "long"
            st["qty"] = qty
            st["avg_price"] = avg_price
            st["last_order_id"] = resp.get("orderId")
            st["last_fill_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            save_state(pair, model, st)

            append_trade(pair, {
                "time_utc": st["last_fill_time"],
                "model": model,
                "pair": pair,
                "side": "BUY",
                "qty": qty,
                "avg_price": avg_price,
                "quote_usdt": TRADE_USDT_PER_ORDER,
                "fee": fee,
                "fee_asset": fee_asset,
                "order_id": resp.get("orderId"),
                "b_level": B,
                "s_level": S,
                "pnl_pct_since_open": 0.0
            })
            logger.info(f"[{pair}] BUY filled qty={qty:.8f} @ {avg_price:.8f} USDT={TRADE_USDT_PER_ORDER:.2f}")

    elif st["position"] == "long":
        if price >= S:
            # Market SELL full position
            qty_to_sell = round_step(float(st.get("qty", 0.0)), filt["stepSize"])
            if qty_to_sell <= 0:
                logger.warning(f"[{pair}] qty_to_sell <= 0, resetting to flat.")
                st["position"] = "flat"; st["qty"] = 0.0; save_state(pair, model, st); return

            params = {
                "symbol": pair,
                "side": "SELL",
                "type": "MARKET",
                "quantity": f"{qty_to_sell:.8f}"
            }
            resp = binance_post("/api/v3/order", params)
            # compute avg price & fee
            fills = resp.get("fills", [])
            if fills:
                total_quote = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                total_qty = sum(float(f["qty"]) for f in fills) or 1e-12
                avg_price = total_quote / total_qty
                fee = sum(float(f.get("commission", 0)) for f in fills)
                fee_asset = fills[0].get("commissionAsset", QUOTE_ASSET)
            else:
                avg_price = float(resp.get("cummulativeQuoteQty", 0)) / max(float(resp.get("executedQty", 0)) or 1e-12, 1e-12)
                fee = 0.0; fee_asset = QUOTE_ASSET

            # pnl %
            buy_px = float(st.get("avg_price", avg_price))
            pnl_pct = ((avg_price - buy_px) / buy_px) * 100.0

            st["position"] = "flat"
            st["qty"] = 0.0
            st["avg_price"] = 0.0
            st["last_order_id"] = resp.get("orderId")
            st["last_fill_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            save_state(pair, model, st)

            append_trade(pair, {
                "time_utc": st["last_fill_time"],
                "model": model,
                "pair": pair,
                "side": "SELL",
                "qty": float(resp.get("executedQty", qty_to_sell)),
                "avg_price": avg_price,
                "quote_usdt": float(resp.get("cummulativeQuoteQty", 0.0)),
                "fee": fee,
                "fee_asset": fee_asset,
                "order_id": resp.get("orderId"),
                "b_level": B,
                "s_level": S,
                "pnl_pct_since_open": pnl_pct
            })
            logger.info(f"[{pair}] SELL filled qty={qty_to_sell:.8f} @ {avg_price:.8f} pnl={pnl_pct:.3f}%")

    # (short režim by byl analogický, ale je defaultně vypnutý)
    dt = time.time() - start
    logger.info(f"[{pair}] tick finished in {dt:.2f}s @ price={price:.8f} B={B:.8f} S={S:.8f}")

# ------------------ ENTRYPOINT ------------------

def main(mytimer: func.TimerRequest) -> None:
    try:
        start = time.time()
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"[BinanceTradeBot] Tick {now_utc} — pairs={len(PAIRS_MODELS)}")

        if not PAIRS_MODELS:
            logger.error("PAIRS_MODELS is empty. Set e.g. 'XRPUSDT:BS_MedianScore,BTCUSDT:BS_MedianScore'.")
            return

        for (pair, model) in PAIRS_MODELS:
            try:
                trade_tick(pair, model)
            except Exception:
                logger.exception(f"[{pair}] tick error")
            if time.time() - start > TIMEOUT_SEC_PER_TICK:
                logger.warning("TIMEOUT_SEC_PER_TICK reached; stopping this tick early.")
                break
    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
