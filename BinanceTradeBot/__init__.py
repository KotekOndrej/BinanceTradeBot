import os
import io
import hmac
import json
import time
import math
import hashlib
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List

import azure.functions as func
import requests

# ---------------------- ENV ----------------------

def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing env var: {name}")
    return v

def _get_float(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except: return default

def _get_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except: return default

# Azure Storage
WEBJOBS_CONN           = _get_env("AzureWebJobsStorage", required=True)
MODELS_CONTAINER       = _get_env("MODELS_CONTAINER", "models-recalc")
MASTER_CSV_NAME        = _get_env("MASTER_CSV_NAME", "bs_levels_master.csv")
STATE_CONTAINER        = _get_env("STATE_CONTAINER", "bot-state")
TRADE_LOGS_CONTAINER   = _get_env("TRADE_LOGS_CONTAINER", "trade-logs")

# Trading – výběr signálů
TRADE_PAIRS_MODELS     = _get_env("TRADE_PAIRS_MODELS", "XRPUSDT:BS_MedianScore")
MIN_CYCLES_PER_DAY     = _get_int("MIN_CYCLES_PER_DAY", 1)
MIN_SCORE              = _get_float("MIN_SCORE", 0.0)

# Objednávky
ORDER_USDT             = _get_float("ORDER_USDT", 10.0)   # Market BUY utratí přesně tuto částku (quoteOrderQty)
TAKER_FEE_PCT          = _get_float("COSTS_PCT", 0.001)   # pouze informativně do logu

# Binance API
BINANCE_API_KEY        = _get_env("BINANCE_API_KEY", required=True)
BINANCE_API_SECRET     = _get_env("BINANCE_API_SECRET", required=True)
USE_TESTNET            = (_get_env("BINANCE_TESTNET", "true").lower() in ("1","true","yes"))
RECV_WINDOW            = _get_int("BINANCE_RECV_WINDOW", 5000)
TIMEOUT_S              = _get_int("BINANCE_HTTP_TIMEOUT", 15)

BASE_URL = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"

logger = logging.getLogger("BinanceTradeBot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------------------- Azure Blob helpers ----------------------

def _make_blob_clients():
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import ResourceExistsError
    bs = BlobServiceClient.from_connection_string(WEBJOBS_CONN)
    models_cc = bs.get_container_client(MODELS_CONTAINER)
    state_cc  = bs.get_container_client(STATE_CONTAINER)
    logs_cc   = bs.get_container_client(TRADE_LOGS_CONTAINER)
    for cc in (models_cc, state_cc, logs_cc):
        try: cc.create_container()
        except ResourceExistsError: pass
    return bs, models_cc, state_cc, logs_cc

def _read_blob_json(cc, name: str) -> Optional[Dict[str, Any]]:
    from azure.core.exceptions import ResourceNotFoundError
    try:
        data = cc.get_blob_client(name).download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except ResourceNotFoundError:
        return None
    except Exception:
        logger.exception("[blob] failed to read JSON: %s", name)
        return None

def _write_blob_json(cc, name: str, obj: Dict[str, Any]) -> None:
    cc.get_blob_client(name).upload_blob(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        overwrite=True
    )

def _append_trade_line(logs_cc, blob_name: str, line: str):
    from azure.core.exceptions import ResourceNotFoundError
    bc = logs_cc.get_blob_client(blob_name)
    try:
        old = bc.download_blob().readall().decode("utf-8")
    except ResourceNotFoundError:
        old = "time_utc,pair,model,side,executedQty,avgFillPrice,cummulativeQuoteQty,fee_total,fee_asset,orderId,b_level,s_level,b_signal_date\n"
    bc.upload_blob((old + line), overwrite=True)

# ---------------------- Master CSV loader ----------------------

def _load_active_signal_for(models_cc, pair: str, model: str) -> Optional[Dict[str, Any]]:
    """
    Z master CSV vybere poslední řádek pro pair+model s is_active=True
    a prahy (min cycles/score). Akceptuje jak 'cycles', tak 'total_cycles'.
    """
    import pandas as pd
    from azure.core.exceptions import ResourceNotFoundError

    blob = models_cc.get_blob_client(MASTER_CSV_NAME)
    try:
        raw = blob.download_blob().readall()
    except ResourceNotFoundError:
        logger.error("Master CSV not found.")
        return None

    df = pd.read_csv(io.BytesIO(raw))
    required = {"pair","model","B","S","date","load_time_utc","is_active","score"}
    if not required.issubset(df.columns):
        logger.error("Master CSV missing columns, have: %s", df.columns.tolist())
        return None

    # zjisti název sloupce s počtem cyklů
    cycles_col = None
    if "cycles" in df.columns:
        cycles_col = "cycles"
    elif "total_cycles" in df.columns:
        cycles_col = "total_cycles"
    else:
        logger.error("Master CSV must contain 'cycles' or 'total_cycles'. Columns: %s", df.columns.tolist())
        return None

    # normalizace a filtry
    df["pair"] = df["pair"].astype(str).str.upper()
    df["model"] = df["model"].astype(str)
    df = df[(df["pair"]==pair.upper()) & (df["model"]==model)]
    if df.empty:
        return None

    df = df[(df["is_active"]==True) &
            (df[cycles_col] >= MIN_CYCLES_PER_DAY) &
            (df["score"]  >= MIN_SCORE)].copy()
    if df.empty:
        return None

    df["load_time_utc"] = pd.to_datetime(df["load_time_utc"], errors="coerce", utc=True)
    df = df.sort_values(["date","load_time_utc"])
    r = df.iloc[-1]

    try:
        return {
            "pair": pair.upper(),
            "model": model,
            "B": float(r["B"]),
            "S": float(r["S"]),
            "date": str(r["date"]),
            "load_time_utc": (r["load_time_utc"].strftime("%Y-%m-%dT%H:%M:%SZ") if not pd.isna(r["load_time_utc"]) else None),
            "cycles": float(r[cycles_col]),
            "score": float(r["score"])
        }
    except Exception:
        logger.exception("Failed to parse master row for %s/%s", pair, model)
        return None

# ---------------------- Binance REST helpers ----------------------

def _sign(query: str) -> str:
    return hmac.new(BINANCE_API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)

def _headers() -> Dict[str,str]:
    return {"X-MBX-APIKEY": BINANCE_API_KEY}

def api_get(path: str, params: Dict[str, Any]) -> Any:
    url = f"{BASE_URL}{path}"
    r = requests.get(url, params=params, headers=_headers(), timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()

def api_signed_post(path: str, params: Dict[str, Any]) -> Any:
    p = dict(params)
    p["timestamp"] = _ts()
    p["recvWindow"] = RECV_WINDOW
    qs = urllib.parse.urlencode(p, doseq=True)
    sig = _sign(qs)
    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    r = requests.post(url, headers=_headers(), timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()

def get_exchange_info(symbol: str) -> Dict[str, Any]:
    return api_get("/api/v3/exchangeInfo", {"symbol": symbol})

def get_symbol_filters(symbol: str) -> Tuple[float, float]:
    """
    Vrátí (stepSize, minNotional) pro symbol.
    """
    info = get_exchange_info(symbol)
    sym = info["symbols"][0]
    step = 0.0
    min_notional = 0.0
    for f in sym["filters"]:
        if f["filterType"] == "LOT_SIZE":
            step = float(f["stepSize"])
        if f["filterType"] == "MIN_NOTIONAL":
            min_notional = float(f.get("minNotional", "0") or 0)
    return (step or 0.0), (min_notional or 0.0)

def round_down_qty(qty: float, step: float) -> float:
    if step <= 0: return qty
    return math.floor(qty / step) * step

def get_price(symbol: str) -> float:
    j = api_get("/api/v3/ticker/price", {"symbol": symbol})
    return float(j["price"])

def place_market_buy_quote(symbol: str, quote_usdt: float) -> Dict[str, Any]:
    """
    MARKET BUY s přesnou útratou v USDT: quoteOrderQty.
    Vrací JSON orderu (obsahuje cummulativeQuoteQty, executedQty, fills[]).
    """
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": f"{quote_usdt:.8f}",
        "newOrderRespType": "FULL"
    }
    return api_signed_post("/api/v3/order", params)

def place_market_sell_qty(symbol: str, qty: float) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": f"{qty:.8f}",
        "newOrderRespType": "FULL"
    }
    return api_signed_post("/api/v3/order", params)

# ---------------------- Bot logic ----------------------

def _parse_pairs_models(val: str) -> List[Tuple[str,str]]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece: continue
        pair, model = piece.split(":",1)
        out.append((pair.strip().upper(), model.strip()))
    return out

def _state_blob_name(pair: str, model: str) -> str:
    return f"{pair}_{model}.json"

def _trade_log_blob_name(pair: str) -> str:
    return f"{pair}.csv"

def _avg_fill_price(fills: List[Dict[str,Any]]) -> float:
    if not fills: return 0.0
    total_qty = 0.0
    total_quote = 0.0
    for f in fills:
        p = float(f["price"]); q = float(f["qty"])
        total_qty += q
        total_quote += p*q
    return (total_quote / total_qty) if total_qty>0 else 0.0

def _sum_fee(fills: List[Dict[str,Any]]) -> Tuple[float,str]:
    total = 0.0
    asset = None
    for f in fills:
        total += float(f["commission"])
        asset = f.get("commissionAsset", asset)
    return total, (asset or "USDT")

def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def run_tick_for_pair(models_cc, state_cc, logs_cc, pair: str, model: str):
    # 1) Signál
    sig = _load_active_signal_for(models_cc, pair, model)
    if not sig:
        logger.info("[%s/%s] No active signal passing thresholds — skipping.", pair, model)
        return

    B = float(sig["B"]); S = float(sig["S"])

    # 2) Stav
    s_name = _state_blob_name(pair, model)
    st = _read_blob_json(state_cc, s_name) or {
        "position": "flat",
        "qty": 0.0,
        "entry_price": None,
        "entry_quote": 0.0,
        "b_level": None, "s_level": None,
        "signal_date": None
    }

    # 3) aktuální cena
    try:
        px = get_price(pair)
    except Exception as e:
        logger.warning("[%s] price failed: %s", pair, e)
        return

    # 4) Filtry symbolu
    try:
        step, min_notional = get_symbol_filters(pair)
    except Exception as e:
        logger.warning("[%s] exchangeInfo failed: %s", pair, e)
        step, min_notional = (0.0, 0.0)

    # 5) SELL podmínka
    if st["position"] == "long" and px >= (st.get("s_level") or S):
        qty_sell = round_down_qty(float(st["qty"]), step)
        if qty_sell <= 0:
            st = { "position":"flat", "qty":0.0, "entry_price":None, "entry_quote":0.0,
                   "b_level":None, "s_level":None, "signal_date":None }
            _write_blob_json(state_cc, s_name, st)
            return

        if min_notional and qty_sell * px < min_notional:
            logger.info("[%s/%s] SELL under minNotional (qty=%s px=%s) → skip.",
                        pair, model, qty_sell, px)
            return

        try:
            odr = place_market_sell_qty(pair, qty_sell)
        except Exception as e:
            logger.warning("[%s/%s] SELL order failed: %s", pair, model, e)
            return

        executed_qty = float(odr.get("executedQty", "0"))
        cq = float(odr.get("cummulativeQuoteQty", "0"))
        fills = odr.get("fills", []) or []
        avgp = _avg_fill_price(fills)
        fee_total, fee_asset = _sum_fee(fills)

        line = ",".join([
            _timestamp(), pair, model, "SELL",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cq:.8f}",
            f"{fee_total:.8f}",
            fee_asset,
            str(odr.get("orderId","")),
            f"{st.get('b_level',0.0):.8f}",
            f"{st.get('s_level',0.0):.8f}",
            str(st.get("signal_date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)

        st = { "position":"flat", "qty":0.0, "entry_price":None, "entry_quote":0.0,
               "b_level":None, "s_level":None, "signal_date":None }
        _write_blob_json(state_cc, s_name, st)
        logger.info("[%s/%s] SELL filled qty=%s avg=%.6f cq=%.6f", pair, model, executed_qty, avgp, cq)
        return

    # 6) BUY podmínka
    if st["position"] == "flat" and px <= B:
        if min_notional and ORDER_USDT < min_notional:
            logger.info("[%s/%s] BUY under minNotional (%.4f < %.4f) → skip",
                        pair, model, ORDER_USDT, min_notional)
            return
        try:
            odr = place_market_buy_quote(pair, ORDER_USDT)
        except Exception as e:
            logger.warning("[%s/%s] BUY order failed: %s", pair, model, e)
            return

        executed_qty = float(odr.get("executedQty","0"))
        cq = float(odr.get("cummulativeQuoteQty","0"))
        fills = odr.get("fills", []) or []
        avgp = _avg_fill_price(fills)
        fee_total, fee_asset = _sum_fee(fills)

        line = ",".join([
            _timestamp(), pair, model, "BUY",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cq:.8f}",
            f"{fee_total:.8f}",
            fee_asset,
            str(odr.get("orderId","")),
            f"{B:.8f}",
            f"{S:.8f}",
            str(sig.get("date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)

        st = {
            "position": "long",
            "qty": executed_qty,
            "entry_price": avgp,
            "entry_quote": cq,
            "b_level": B,
            "s_level": S,
            "signal_date": sig.get("date")
        }
        _write_blob_json(state_cc, s_name, st)
        logger.info("[%s/%s] BUY filled qty=%s avg=%.6f cq=%.6f", pair, model, executed_qty, avgp, cq)
        return

    logger.info("[%s/%s] No action. px=%.6f B=%.6f S=%.6f pos=%s", pair, model, px, B, S, st["position"])

# ---------------------- Azure Function entry ----------------------

def main(mytimer: func.TimerRequest) -> None:
    try:
        _, models_cc, state_cc, logs_cc = _make_blob_clients()
        pairs_models = _parse_pairs_models(TRADE_PAIRS_MODELS)
        if not pairs_models:
            logger.error("TRADE_PAIRS_MODELS is empty"); return

        for pair, model in pairs_models:
            run_tick_for_pair(models_cc, state_cc, logs_cc, pair, model)

    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
