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

# ---------------------- API call CSV logging ----------------------

def _api_csv_blob_name() -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"api_calls_{today}.csv"

def _append_api_csv(logs_cc, row: Dict[str, Any]):
    """
    Zapíše jeden řádek do denního CSV s API voláními.
    Sloupce: ts,method,path,params,status,error,resp_sample
    """
    from azure.core.exceptions import ResourceNotFoundError
    bc = logs_cc.get_blob_client(_api_csv_blob_name())
    header = "ts,method,path,params,status,error,resp_sample\n"
    try:
        old = bc.download_blob().readall().decode("utf-8")
        if not old.startswith("ts,"):
            old = header
    except ResourceNotFoundError:
        old = header
    # serializace hodnot (params jako JSON bez citlivých klíčů)
    params_ser = json.dumps(row.get("params", {}), ensure_ascii=False, separators=(",", ":"))
    resp_sample = row.get("resp_sample", "")
    # omez výstup
    if isinstance(resp_sample, str) and len(resp_sample) > 1000:
        resp_sample = resp_sample[:1000] + "..."
    line = ",".join([
        row.get("ts",""),
        row.get("method",""),
        row.get("path",""),
        params_ser.replace("\n"," ").replace("\r"," "),
        str(row.get("status","")),
        (row.get("error","") or "").replace("\n"," ").replace("\r"," "),
        (resp_sample or "").replace("\n"," ").replace("\r"," ")
    ]) + "\n"
    bc.upload_blob(old + line, overwrite=True)

def _sanitize_params(p: Dict[str, Any]) -> Dict[str, Any]:
    # odstraň citlivé/směšně dlouhé věci
    if not isinstance(p, dict):
        return {}
    cleaned = dict(p)
    cleaned.pop("signature", None)
    return cleaned

def _log_api_call_csv(logs_cc, *, method: str, path: str, params: Dict[str, Any], status: int, error: Optional[str], resp_text: Optional[str]):
    try:
        _append_api_csv(logs_cc, {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "method": method,
            "path": path,
            "params": _sanitize_params(params or {}),
            "status": status,
            "error": error or "",
            "resp_sample": resp_text or ""
        })
    except Exception:
        # Logging CSV selhat nesmí zlomit trading
        logger.warning("[api-csv] failed to append log", exc_info=True)

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

    cycles_col = "cycles" if "cycles" in df.columns else ("total_cycles" if "total_cycles" in df.columns else None)
    if cycles_col is None:
        logger.error("Master CSV must contain 'cycles' or 'total_cycles'. Columns: %s", df.columns.tolist())
        return None

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

# ---------------------- Binance REST helpers (s CSV logging) ----------------------

def _sign(query: str) -> str:
    return hmac.new(BINANCE_API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)

def _headers() -> Dict[str,str]:
    return {"X-MBX-APIKEY": BINANCE_API_KEY}

def api_get(path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    url = f"{BASE_URL}{path}"
    try:
        r = requests.get(url, params=params, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        text_sample = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        if logs_cc: _log_api_call_csv(logs_cc, method="GET", path=path, params=params, status=status, error=None, resp_text=text_sample)
        return j
    except Exception as e:
        if logs_cc:
            # pokud máme response, pokusíme se zalogovat status/text
            status = getattr(e.response, "status_code", "") if hasattr(e, "response") else ""
            text = ""
            try:
                text = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                text = ""
            _log_api_call_csv(logs_cc, method="GET", path=path, params=params, status=status, error=str(e), resp_text=text)
        raise

def api_signed_post(path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    p = dict(params)
    p["timestamp"] = _ts()
    p["recvWindow"] = RECV_WINDOW
    qs = urllib.parse.urlencode(p, doseq=True)
    sig = _sign(qs)
    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    try:
        r = requests.post(url, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        text_sample = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        if logs_cc: _log_api_call_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=None, resp_text=text_sample)
        return j
    except Exception as e:
        if logs_cc:
            status = getattr(e.response, "status_code", "") if hasattr(e, "response") else ""
            text = ""
            try:
                text = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                text = ""
            _log_api_call_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=str(e), resp_text=text)
        raise

def get_price(symbol: str, *, logs_cc=None) -> float:
    j = api_get("/api/v3/ticker/price", {"symbol": symbol}, logs_cc=logs_cc)
    return float(j["price"])

# ---------------------- exchangeInfo cache (1× denně) ----------------------

def _pairs_from_env(val: str) -> List[str]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece:
            continue
        pair, _model = piece.split(":",1)
        out.append(pair.strip().upper())
    seen = set(); res = []
    for p in out:
        if p not in seen:
            seen.add(p); res.append(p)
    return res

def _exchangeinfo_blob_name_for_today() -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"exchangeinfo_{today}.json"

def _ensure_exchangeinfo_json_for_today(logs_cc, pairs: List[str]) -> Dict[str, Any]:
    """
    Zkus načíst dnešní exchangeinfo JSON z TRADE_LOGS_CONTAINER.
    Pokud neexistuje, zavolá 1× Binance /api/v3/exchangeInfo se seznamem symbolů,
    uloží minimalizovaný JSON a vrátí mapu filtrů.
    """
    blob_name = _exchangeinfo_blob_name_for_today()
    cached = _read_blob_json(logs_cc, blob_name)
    if cached and isinstance(cached, dict) and cached.get("asof_date"):
        logger.info("[exchangeInfo] using cached %s", blob_name)
        return cached

    symbols_param = json.dumps(pairs, separators=(",", ":"))
    info = api_get("/api/v3/exchangeInfo", {"symbols": symbols_param}, logs_cc=logs_cc)

    filt_map: Dict[str, Any] = {}
    for sym in info.get("symbols", []):
        sname = sym.get("symbol")
        status = sym.get("status")
        tick_size = None
        step_size = None
        min_notional = None
        for f in sym.get("filters", []):
            ftype = f.get("filterType")
            if ftype == "PRICE_FILTER":
                try: tick_size = float(f.get("tickSize"))
                except: tick_size = None
            elif ftype == "LOT_SIZE":
                try: step_size = float(f.get("stepSize"))
                except: step_size = None
            elif ftype == "MIN_NOTIONAL":
                try: min_notional = float(f.get("minNotional", "0") or 0)
                except: min_notional = None
        if sname:
            filt_map[sname.upper()] = {
                "status": status,
                "tickSize": tick_size or 0.0,
                "stepSize": step_size or 0.0,
                "minNotional": min_notional or 0.0
            }

    payload = {
        "asof_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "filters": filt_map
    }
    _write_blob_json(logs_cc, blob_name, payload)
    logger.info("[exchangeInfo] cached to %s (%d symbols)", blob_name, len(filt_map))
    return payload

def get_symbol_filters_cached(filters_json: Dict[str, Any], symbol: str) -> Tuple[float, float, float, str]:
    """
    Vrátí (stepSize, minNotional, tickSize, status) z denního JSONu.
    Pokud symbol nenajdeme, vrací (0,0,0,"UNKNOWN").
    """
    entry = (filters_json.get("filters") or {}).get(symbol.upper())
    if not entry:
        return (0.0, 0.0, 0.0, "UNKNOWN")
    return (
        float(entry.get("stepSize", 0.0)),
        float(entry.get("minNotional", 0.0)),
        float(entry.get("tickSize", 0.0)),
        str(entry.get("status", "TRADING") or "TRADING")
    )

def round_down_qty(qty: float, step: float) -> float:
    if step <= 0: return qty
    return math.floor(qty / step) * step

# ---------------------- Orders ----------------------

def place_market_buy_quote(symbol: str, quote_usdt: float, *, logs_cc=None) -> Dict[str, Any]:
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
    return api_signed_post("/api/v3/order", params, logs_cc=logs_cc)

def place_market_sell_qty(symbol: str, qty: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": f"{qty:.8f}",
        "newOrderRespType": "FULL"
    }
    return api_signed_post("/api/v3/order", params, logs_cc=logs_cc)

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

def run_tick_for_pair(models_cc, state_cc, logs_cc, pair: str, model: str, exch_cache: Dict[str,Any]):
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
        px = get_price(pair, logs_cc=logs_cc)
    except Exception as e:
        logger.warning("[%s] price failed: %s", pair, e)
        return

    # 4) Filtry symbolu z denního JSONu
    step, min_notional, tick, status = get_symbol_filters_cached(exch_cache, pair)
    if status and status != "TRADING":
        logger.info("[%s/%s] status=%s → skipping trading", pair, model, status)
        return

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
            odr = place_market_sell_qty(pair, qty_sell, logs_cc=logs_cc)
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
            odr = place_market_buy_quote(pair, ORDER_USDT, logs_cc=logs_cc)
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

        # 0) Připrav páry a denní exchangeInfo cache (1× denně JSON v TRADE_LOGS)
        pairs_models = _parse_pairs_models(TRADE_PAIRS_MODELS)
        if not pairs_models:
            logger.error("TRADE_PAIRS_MODELS is empty"); return
        pairs = sorted({p for (p, _m) in pairs_models})
        exchangeinfo_today = _ensure_exchangeinfo_json_for_today(logs_cc, pairs)

        # 1) Tik pro každý (pair, model)
        for pair, model in pairs_models:
            run_tick_for_pair(models_cc, state_cc, logs_cc, pair, model, exchangeinfo_today)

    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
