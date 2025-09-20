import os
import hmac
import json
import time
import math
import hashlib
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict

import azure.functions as func  # lehký import

# ---- Azure Blob SDK (jen univerzální klienti) ----
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# ------------------ ENV & CONFIG ------------------

def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except:
        return default

def _get_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except:
        return default

def _parse_pairs_models(val: str) -> List[Tuple[str, str]]:
    """
    PAIRS_MODELS='XRPUSDT:BS_MedianScore,BTCUSDT:BS_MedianScore'
    -> [('XRPUSDT','BS_MedianScore'), ('BTCUSDT','BS_MedianScore')]
    """
    res: List[Tuple[str, str]] = []
    if not val:
        return res
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece:
            continue
        pair, model = piece.split(":", 1)
        res.append((pair.strip().upper(), model.strip()))
    return res

def _parse_overrides(env_val: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not env_val:
        return out
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

# Binance (jen env)
BINANCE_BASE_URL   = _get_env("BINANCE_BASE_URL", "https://testnet.binance.vision")
BINANCE_API_KEY    = _get_env("BINANCE_API_KEY", "")
BINANCE_API_SECRET = _get_env("BINANCE_API_SECRET", "")

QUOTE_ASSET = (_get_env("QUOTE_ASSET", "USDT") or "USDT").upper()
TRADE_USDT_PER_ORDER = _get_float("TRADE_USDT_PER_ORDER", 50.0)
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
TIMEOUT_SEC_PER_TICK = _get_int("TIMEOUT_SEC_PER_TICK", 20)

# Pairs
PAIRS_MODELS = _parse_pairs_models(os.getenv("PAIRS_MODELS", ""))

# Storage
WEBJOBS_CONN      = _get_env("AzureWebJobsStorage", None)
SIGNALS_CONTAINER = _get_env("SIGNALS_CONTAINER", "market-signals")
MASTER_CSV_NAME   = _get_env("MASTER_CSV_NAME", "bs_levels_master.csv")
TRADES_CONTAINER  = _get_env("TRADES_CONTAINER", "trade-logs")
STATE_CONTAINER   = _get_env("STATE_CONTAINER", "bot-state")

logger = logging.getLogger("BinanceTradeBot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ------------------ BINANCE REST ------------------

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(params: Dict, secret: str) -> str:
    qs = urllib.parse.urlencode(params, doseq=True)
    return hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()

def _headers() -> Dict[str, str]:
    return {"X-MBX-APIKEY": BINANCE_API_KEY or "", "Content-Type": "application/x-www-form-urlencoded"}

# ---------- Block Blob helpers (bez AppendBlob závislosti) ----------

def _put_full_body_as_single_block(blob: BlobClient, data_bytes: bytes):
    """
    Přepíše celé tělo blobu jediným blokem (kompatibilní se všemi verzemi SDK).
    """
    import base64, secrets
    block_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    # stage + commit se seznamem ID jako list[str]
    blob.stage_block(block_id=block_id, data=data_bytes)
    blob.commit_block_list([block_id])

def _ensure_blob_with_header(blob: BlobClient, header_line: str):
    """
    Pokud blob neexistuje → vytvoř s hlavičkou.
    Pokud existuje a nemá hlavičku → doplň ji před obsah.
    """
    try:
        props = blob.get_blob_properties()
        size = props.size or 0
        if size == 0:
            _put_full_body_as_single_block(blob, (header_line + "\n").encode("utf-8"))
            return
        head = blob.download_blob(offset=0, length=256).readall().decode("utf-8", "ignore")
        if not head.startswith(header_line):
            existing = blob.download_blob().readall()
            new_body = (header_line + "\n").encode("utf-8") + existing
            _put_full_body_as_single_block(blob, new_body)
    except ResourceNotFoundError:
        _put_full_body_as_single_block(blob, (header_line + "\n").encode("utf-8"))

def _append_lines_block_blob(blob: BlobClient, new_text: str):
    """
    Připojí text (řádky) na konec block blobu: stáhne existující obsah, přidá nové řádky,
    a celé tělo uloží jedním blokem. Jednoduché a kompatibilní.
    """
    try:
        existing = b""
        try:
            props = blob.get_blob_properties()
            if (props.size or 0) > 0:
                existing = blob.download_blob().readall()
        except ResourceNotFoundError:
            existing = b""
        combined = existing + new_text.encode("utf-8")
        _put_full_body_as_single_block(blob, combined)
    except Exception as e:
        logger.error("[blob-append] failed for %s: %s", blob.blob_name, e, exc_info=True)
        raise

# ---------- API CALL FILE LOGGING (to trade-logs, Block Blob) ----------

_API_LOG_CACHE = {
    "svc": None,   # BlobServiceClient
    "cc": None,    # ContainerClient (trade-logs)
}

def _api_log_blob_name() -> str:
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"api_calls_{d}.csv"

def _get_api_log_blob(trades_cc: ContainerClient) -> BlobClient:
    name = _api_log_blob_name()
    blob = trades_cc.get_blob_client(name)
    _ensure_blob_with_header(blob, "time_utc,method,path,params_json,status,duration_ms,error")
    return blob

def _redact_params(p: Dict) -> Dict:
    if not p:
        return {}
    red = dict(p)
    if "signature" in red:
        red["signature"] = "<redacted>"
    return red

def _append_api_call(trades_cc: ContainerClient, method: str, path: str, params: Dict, status: int, duration_ms: int, error: Optional[str] = None):
    try:
        blob = _get_api_log_blob(trades_cc)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        params_json = json.dumps(_redact_params(params or {}), separators=(",", ":"), ensure_ascii=False)
        line = f"{now},{method},{path},{params_json},{status},{duration_ms},{'' if not error else error.replace(',', ';')}\n"
        _append_lines_block_blob(blob, line)
    except Exception:
        logger.error("[api-log] failed to append api call", exc_info=True)

# ---------- REST GET/POST s file-loggingem ----------

def binance_get(path: str, params: Optional[Dict] = None, signed: bool = False, trades_cc: ContainerClient = None):
    import requests
    url = f"{BINANCE_BASE_URL}{path}"
    params = params or {}
    if signed:
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            raise RuntimeError("BINANCE_API_KEY/SECRET not set")
        params["timestamp"] = _ts_ms()
        params["recvWindow"] = 5000
        params["signature"] = _sign(params, BINANCE_API_SECRET)

    t0 = time.time()
    try:
        r = requests.get(url, params=params, headers=_headers(), timeout=20)
        dur = int((time.time() - t0) * 1000)
        if trades_cc is not None:
            _append_api_call(trades_cc, "GET", path, params, r.status_code, dur, None if r.status_code == 200 else r.text[:120])
        if r.status_code == 200:
            return r.json()
        raise RuntimeError("GET {0} {1}: {2}".format(path, r.status_code, r.text[:200]))
    except Exception as e:
        dur = int((time.time() - t0) * 1000)
        if trades_cc is not None:
            _append_api_call(trades_cc, "GET", path, params, -1, dur, str(e)[:120])
        raise

def binance_post(path: str, params: Dict, signed: bool = True, trades_cc: ContainerClient = None):
    import requests
    url = f"{BINANCE_BASE_URL}{path}"
    if signed:
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            raise RuntimeError("BINANCE_API_KEY/SECRET not set")
        params = dict(params or {})
        params["timestamp"] = _ts_ms()
        params["recvWindow"] = 5000
        params["signature"] = _sign(params, BINANCE_API_SECRET)

    t0 = time.time()
    try:
        r = requests.post(url, data=params, headers=_headers(), timeout=20)
        dur = int((time.time() - t0) * 1000)
        if trades_cc is not None:
            _append_api_call(trades_cc, "POST", path, params, r.status_code, dur, None if r.status_code in (200, 201) else r.text[:120])
        if r.status_code in (200, 201):
            return r.json()
        raise RuntimeError("POST {0} {1}: {2}".format(path, r.status_code, r.text[:200]))
    except Exception as e:
        dur = int((time.time() - t0) * 1000)
        if trades_cc is not None:
            _append_api_call(trades_cc, "POST", path, params, -1, dur, str(e)[:120])
        raise

def get_price(pair: str, trades_cc: ContainerClient) -> float:
    data = binance_get("/api/v3/ticker/price", {"symbol": pair}, trades_cc=trades_cc)
    return float(data["price"])

def get_exchange_info(pair: str, trades_cc: ContainerClient) -> Dict:
    data = binance_get("/api/v3/exchangeInfo", {"symbol": pair}, trades_cc=trades_cc)
    return data

def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step

def symbol_filters(info: Dict) -> Dict[str, float]:
    filters = {f["filterType"]: f for f in info["symbols"][0]["filters"]}
    lot = filters.get("LOT_SIZE", {})
    price = filters.get("PRICE_FILTER", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    return {
        "stepSize": float(lot.get("stepSize", "0.00000001")),
        "tickSize": float(price.get("tickSize", "0.00000001")),
        "minNotional": float(min_notional.get("minNotional", "0.0"))
    }

# ------------------ CTX ------------------

class Ctx:
    def __init__(self, blob_service, signals_cc, trades_cc, state_cc):
        self.blob_service = blob_service
        self.signals_cc = signals_cc
        self.trades_cc = trades_cc
        self.state_cc = state_cc

# ------------------ STORAGE HELPERS ------------------

def _state_blob_name(pair: str, model: str) -> str:
    return f"{pair}_{model}.json"

def load_state(ctx: Ctx, pair: str, model: str) -> Dict:
    blob = ctx.state_cc.get_blob_client(_state_blob_name(pair, model))
    try:
        data = blob.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except ResourceNotFoundError:
        return {
            "position": "flat",
            "qty": 0.0,
            "avg_price": 0.0,
            "last_order_id": None,
            "last_fill_time": None,
            "B": None, "S": None,
            "session_tag": None
        }

def save_state(ctx: Ctx, pair: str, model: str, state: Dict):
    blob = ctx.state_cc.get_blob_client(_state_blob_name(pair, model))
    data = json.dumps(state, separators=(",", ":")).encode("utf-8")
    blob.upload_blob(data, overwrite=True)

TRADES_HEADER = ("time_utc,model,pair,side,qty,avg_price,quote_usdt,fee,fee_asset,"
                 "order_id,b_level,s_level,pnl_pct_since_open")

def ensure_trades_csv(ctx: Ctx, pair: str):
    name = f"{pair}_trades.csv"
    blob = ctx.trades_cc.get_blob_client(name)
    _ensure_blob_with_header(blob, TRADES_HEADER)

def append_trade(ctx: Ctx, pair: str, row: Dict):
    ensure_trades_csv(ctx, pair)
    blob = ctx.trades_cc.get_blob_client(f"{pair}_trades.csv")
    line = ",".join([
        row.get("time_utc",""),
        row.get("model",""),
        row.get("pair",""),
        row.get("side",""),
        "{0:.8f}".format(row.get("qty",0.0)),
        "{0:.8f}".format(row.get("avg_price",0.0)),
        "{0:.8f}".format(row.get("quote_usdt",0.0)),
        "{0:.8f}".format(row.get("fee",0.0)),
        row.get("fee_asset",""),
        str(row.get("order_id","")),
        "{0:.8f}".format(row.get("b_level",0.0)),
        "{0:.8f}".format(row.get("s_level",0.0)),
        "{0:.6f}".format(row.get("pnl_pct_since_open",0.0))
    ]) + "\n"
    _append_lines_block_blob(blob, line)

# ------------------ SIGNALS ------------------

def _min_cycles_for(pair: str) -> int:
    return int(OVR_CYCLES.get(pair.upper(), float(MIN_CYCLES_GLOBAL)))

def _min_score_for(pair: str) -> float:
    return float(OVR_SCORE.get(pair.upper(), float(MIN_SCORE_GLOBAL)))

def load_active_signal(ctx: Ctx, pair: str, model: str) -> Optional[Dict]:
    try:
        import pandas as pd
    except Exception as ie:
        logger.error("[signals] pandas import failed: %s", ie)
        return None

    blob = ctx.signals_cc.get_blob_client(MASTER_CSV_NAME)
    try:
        data = blob.download_blob().readall()
    except ResourceNotFoundError:
        return None

    try:
        df = pd.read_csv(pd.io.common.BytesIO(data))
    except Exception:
        return None

    need_cols = {"pair","model","is_active","B","S","total_cycles","score","date","load_time_utc"}
    if not need_cols.issubset(df.columns):
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

# ------------------ TRADING LOGIC ------------------

def trade_tick(ctx: Ctx, pair: str, model: str):
    sig = load_active_signal(ctx, pair, model)
    if not sig:
        return

    B = sig["B"]; S = sig["S"]
    st = load_state(ctx, pair, model)
    st["B"] = B; st["S"] = S
    st["session_tag"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    save_state(ctx, pair, model, st)

    info = get_exchange_info(pair, ctx.trades_cc)
    filt = symbol_filters(info)
    price = get_price(pair, ctx.trades_cc)

    if st["position"] == "flat":
        if price <= B:
            params = {
                "symbol": pair,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": "{0:.2f}".format(TRADE_USDT_PER_ORDER)
            }
            resp = binance_post("/api/v3/order", params, trades_cc=ctx.trades_cc)
            fills = resp.get("fills", [])
            if fills:
                total_quote = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                total_qty = sum(float(f["qty"]) for f in fills) or 1e-12
                avg_price = total_quote / total_qty
                fee = sum(float(f.get("commission", 0)) for f in fills)
                fee_asset = fills[0].get("commissionAsset", QUOTE_ASSET)
                qty = total_qty
            else:
                qty = float(resp.get("executedQty", 0))
                cq = float(resp.get("cummulativeQuoteQty", 0.0))
                avg_price = cq / (qty or 1e-12)
                fee = 0.0; fee_asset = QUOTE_ASSET

            st["position"] = "long"
            st["qty"] = qty
            st["avg_price"] = avg_price
            st["last_order_id"] = resp.get("orderId")
            st["last_fill_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            save_state(ctx, pair, model, st)

            append_trade(ctx, pair, {
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

    elif st["position"] == "long":
        if price >= S:
            qty_to_sell = round_step(float(st.get("qty", 0.0)), filt["stepSize"])
            if qty_to_sell <= 0:
                st["position"] = "flat"; st["qty"] = 0.0; save_state(ctx, pair, model, st); return

            params = {
                "symbol": pair,
                "side": "SELL",
                "type": "MARKET",
                "quantity": "{0:.8f}".format(qty_to_sell)
            }
            resp = binance_post("/api/v3/order", params, trades_cc=ctx.trades_cc)
            fills = resp.get("fills", [])
            if fills:
                total_quote = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                total_qty = sum(float(f["qty"]) for f in fills) or 1e-12
                avg_price = total_quote / total_qty
                fee = sum(float(f.get("commission", 0)) for f in fills)
                fee_asset = fills[0].get("commissionAsset", QUOTE_ASSET)
            else:
                total_qty = float(resp.get("executedQty", qty_to_sell))
                cq = float(resp.get("cummulativeQuoteQty", 0.0))
                avg_price = cq / (total_qty or 1e-12)
                fee = 0.0; fee_asset = QUOTE_ASSET

            buy_px = float(st.get("avg_price", avg_price))
            pnl_pct = ((avg_price - buy_px) / (buy_px or 1e-12)) * 100.0

            st["position"] = "flat"
            st["qty"] = 0.0
            st["avg_price"] = 0.0
            st["last_order_id"] = resp.get("orderId")
            st["last_fill_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            save_state(ctx, pair, model, st)

            append_trade(ctx, pair, {
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

# ------------------ ENTRYPOINT ------------------

def main(mytimer: func.TimerRequest) -> None:
    try:
        start = time.time()

        if not PAIRS_MODELS:
            logger.error("PAIRS_MODELS is empty. Set e.g. 'XRPUSDT:BS_MedianScore,BTCUSDT:BS_MedianScore'.")
            return

        if not WEBJOBS_CONN:
            logger.error("AzureWebJobsStorage is not set.")
            return

        blob_service = BlobServiceClient.from_connection_string(WEBJOBS_CONN)
        signals_cc = blob_service.get_container_client(SIGNALS_CONTAINER)
        trades_cc  = blob_service.get_container_client(TRADES_CONTAINER)
        state_cc   = blob_service.get_container_client(STATE_CONTAINER)

        for cc in (signals_cc, trades_cc, state_cc):
            try:
                cc.create_container()
            except ResourceExistsError:
                pass

        # API log cache (pro předání trades_cc do REST helperů)
        _API_LOG_CACHE["svc"] = blob_service
        _API_LOG_CACHE["cc"]  = trades_cc

        ctx = Ctx(blob_service, signals_cc, trades_cc, state_cc)

        # volitelně založ trade CSV i bez obchodů
        for (pair, model) in PAIRS_MODELS:
            ensure_trades_csv(ctx, pair)

        for (pair, model) in PAIRS_MODELS:
            try:
                trade_tick(ctx, pair, model)
            except Exception:
                logger.exception("[%s] tick error", pair)
            if time.time() - start > TIMEOUT_SEC_PER_TICK:
                break

    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
