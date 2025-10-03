import os
import io
import hmac
import json
import time
import math
import hashlib
import logging
import urllib.parse
import csv
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, getcontext
from typing import Dict, Any, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, BlobType
from azure.core.exceptions import ResourceNotFoundError

# ========= LOGGING =========
logger = logging.getLogger("BinanceTradeBot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ========= ENV =========
def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing env var: {name}")
    return v

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except:
        return default

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except:
        return default

def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    return str(raw).lower() in ("1", "true", "yes", "y", "on")

# Azure Storage
WEBJOBS_CONN         = _get_env("AzureWebJobsStorage", required=True)
MODELS_CONTAINER     = _get_env("MODELS_CONTAINER", "models-recalc")
MASTER_CSV_NAME      = _get_env("MASTER_CSV_NAME", "bs_levels_master.csv")
STATE_CONTAINER      = _get_env("STATE_CONTAINER", "bot-state")
TRADE_LOGS_CONTAINER = _get_env("TRADE_LOGS_CONTAINER", "trade-logs")

# Trading – výběr signálů
TRADE_PAIRS_MODELS   = _get_env("TRADE_PAIRS_MODELS", "AVAXUSDC:BS_WeightedUtility")
MIN_CYCLES_PER_DAY   = _get_int("MIN_CYCLES_PER_DAY", 1)
MIN_SCORE            = _get_float("MIN_SCORE", 0.0)

# Objednávky
ORDER_USDT           = _get_float("ORDER_USDT", 10.0)

# Binance API
BINANCE_API_KEY      = _get_env("BINANCE_API_KEY", required=True)
BINANCE_API_SECRET   = _get_env("BINANCE_API_SECRET", required=True)
USE_TESTNET          = (_get_env("BINANCE_TESTNET", "false").lower() in ("1", "true", "yes"))
RECV_WINDOW          = _get_int("BINANCE_RECV_WINDOW", 5000)
TIMEOUT_S            = _get_int("BINANCE_HTTP_TIMEOUT", 15)
BASE_URL             = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"

# výkon
PRICE_FETCH_WORKERS  = _get_int("PRICE_FETCH_WORKERS", 10)
API_CSV_LOGGING      = _get_bool("API_CSV_LOGGING", True)

# odchozí IP cache
OUTBOUND_IP_CACHE_TTL_S = _get_int("OUTBOUND_IP_CACHE_TTL_S", 300)

# ========= DECIMAL PRECISION =========
getcontext().prec = 28

# ========= HELPERS =========
def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _headers() -> Dict[str, str]:
    return {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Accept": "application/json",
        "User-Agent": "functions-binance-bot/1.0"
    }

def _sign(query: str) -> str:
    return hmac.new(BINANCE_API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)

def _sanitize_params(p: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(p, dict):
        return {}
    cleaned = dict(p)
    cleaned.pop("signature", None)
    return cleaned

# ========= Outbound IP (cached) =========
_IP_CACHE = {"ip": None, "ts": 0.0}

def _get_outbound_ip() -> Optional[str]:
    now = time.time()
    if _IP_CACHE["ip"] and (now - _IP_CACHE["ts"] < OUTBOUND_IP_CACHE_TTL_S):
        return _IP_CACHE["ip"]
    ip = None
    try:
        ip = requests.get("https://api.ipify.org?format=json", timeout=5).json().get("ip")
    except Exception:
        pass
    if not ip:
        try:
            ip = requests.get("https://ifconfig.me/ip", timeout=5).text.strip()
        except Exception:
            pass
    _IP_CACHE["ip"] = ip
    _IP_CACHE["ts"] = now
    return ip

# ========= Azure Blob =========
def _make_blob_clients():
    bs = BlobServiceClient.from_connection_string(WEBJOBS_CONN)
    models_cc = bs.get_container_client(MODELS_CONTAINER)
    state_cc  = bs.get_container_client(STATE_CONTAINER)
    logs_cc   = bs.get_container_client(TRADE_LOGS_CONTAINER)
    for cc in (models_cc, state_cc, logs_cc):
        try:
            cc.create_container()
        except Exception:
            pass
    return bs, models_cc, state_cc, logs_cc

def _read_blob_json(cc, name: str) -> Optional[Dict[str, Any]]:
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

# ========= Trade CSV (AppendBlob) =========
def _trade_log_blob_name(pair: str) -> str:
    return f"trades/{pair}.csv"

_TRADE_HEADER = "ts,pair,model,side,qty,avg_price,quote_qty,fee,fee_asset,order_id,B,S,signal_date\n"

def _ensure_trade_log_append_blob_with_header(logs_cc, blob_name: str):
    bc = logs_cc.get_blob_client(blob_name)
    header_b = _TRADE_HEADER.encode("utf-8")

    def _to_chunks(b: bytes, chunk: int = 4 * 1024 * 1024):
        for i in range(0, len(b), chunk):
            yield b[i:i+chunk]

    try:
        props = bc.get_blob_properties()
        blob_type = getattr(props, "blob_type", None)
        size = int(getattr(props, "size", 0) or 0)

        if blob_type == BlobType.AppendBlob:
            if size == 0:
                bc.append_block(header_b); return
            try:
                head = bc.download_blob(offset=0, length=max(len(header_b), 1024)).readall()
            except Exception:
                head = b""
            if head.decode("utf-8", errors="ignore").startswith(_TRADE_HEADER):
                return
            content = bc.download_blob().readall()
            bc.delete_blob(); bc.create_append_blob(); bc.append_block(header_b)
            for chunk in _to_chunks(content):
                if chunk: bc.append_block(chunk)
            return

        content = b""
        if size > 0:
            try: content = bc.download_blob().readall()
            except Exception: content = b""
        has_header = content.decode("utf-8", errors="ignore").startswith(_TRADE_HEADER)
        bc.delete_blob(); bc.create_append_blob()
        if not has_header: bc.append_block(header_b)
        for chunk in _to_chunks(content):
            if chunk: bc.append_block(chunk)

    except ResourceNotFoundError:
        bc.create_append_blob()
        bc.append_block(header_b)

def _append_trade_line(logs_cc, blob_name: str, line: str):
    try:
        _ensure_trade_log_append_blob_with_header(logs_cc, blob_name)
        logs_cc.get_blob_client(blob_name).append_block(line.encode("utf-8"))
    except Exception as e:
        logger.warning("Trade CSV append failed for %s: %s", blob_name, e)

# ========= API CSVs (per-hour) =========
API_LOG_COLUMNS = ["ts","method","path","params","status","error","resp_sample","outbound_ip"]

def _date_parts():
    datehour = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H")
    date = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    return date, datehour

def _api_log_blob_name_prices() -> str:
    d, dh = _date_parts()
    return f"logs/{d}/prices_{dh}.csv"

def _api_log_blob_name_general() -> str:
    d, dh = _date_parts()
    return f"logs/{d}/api_calls_{dh}.csv"

def _csv_line(values: List[Any]) -> str:
    sio = io.StringIO()
    csv.writer(sio, lineterminator="\n").writerow(values)
    return sio.getvalue()

def _ensure_api_log_append_blob_with_header(logs_cc, *, is_price: bool):
    blob_name = _api_log_blob_name_prices() if is_price else _api_log_blob_name_general()
    bc = logs_cc.get_blob_client(blob_name)
    header = _csv_line(API_LOG_COLUMNS).encode("utf-8")

    def _to_chunks(b: bytes, chunk: int = 4 * 1024 * 1024):
        for i in range(0, len(b), chunk):
            yield b[i:i+chunk]

    try:
        props = bc.get_blob_properties()
        if props.blob_type == BlobType.AppendBlob:
            if int(props.size or 0) == 0:
                bc.append_block(header); return
            first = b""
            try:
                first = bc.download_blob(offset=0, length=max(len(header), 1024)).readall()
            except Exception:
                pass
            if first.decode("utf-8", errors="ignore").startswith(_csv_line(API_LOG_COLUMNS).strip()):
                return
            content = bc.download_blob().readall()
            bc.delete_blob(); bc.create_append_blob(); bc.append_block(header)
            for chunk in _to_chunks(content):
                if chunk: bc.append_block(chunk)
            return
        content = b""
        try:
            if int(props.size or 0) > 0:
                content = bc.download_blob().readall()
        except Exception:
            pass
        has_header = content.decode("utf-8", errors="ignore").startswith(_csv_line(API_LOG_COLUMNS).strip())
        bc.delete_blob(); bc.create_append_blob()
        if not has_header: bc.append_block(header)
        for chunk in _to_chunks(content):
            if chunk: bc.append_block(chunk)
    except ResourceNotFoundError:
        bc.create_append_blob(); bc.append_block(header)

def _append_api_csv_row(logs_cc, row_values: List[Any], *, is_price: bool):
    _ensure_api_log_append_blob_with_header(logs_cc, is_price=is_price)
    line = _csv_line(row_values).encode("utf-8")
    blob_name = _api_log_blob_name_prices() if is_price else _api_log_blob_name_general()
    logs_cc.get_blob_client(blob_name).append_block(line)

def _append_api_csv(logs_cc, *, method: str, path: str, params: Dict[str, Any],
                    status: Any, error: Optional[str], resp_text: Optional[str],
                    outbound_ip: Optional[str]):
    if not API_CSV_LOGGING:
        return
    params_ser = json.dumps(_sanitize_params(params or {}), ensure_ascii=False, separators=(",", ":"))
    sample = (resp_text or "")
    if isinstance(sample, str) and len(sample) > 1000:
        sample = sample[:1000] + "..."
    sample = sample.replace("\r", " ").replace("\n", " ")
    row = [_timestamp(), method, path, params_ser, str(status or ""), (error or ""), sample, (outbound_ip or "")]
    is_price = (path == "/api/v3/ticker/price")
    _append_api_csv_row(logs_cc, row, is_price=is_price)

# ========= HTTP helpers =========
def api_get(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    hosts = [
        BASE_URL,
        BASE_URL.replace("api.", "api1."),
        BASE_URL.replace("api.", "api2."),
        BASE_URL.replace("api.", "api3."),
    ]
    last_err = None
    for base in hosts:
        url = f"{base}{path}"
        try:
            r = session.get(url, params=params, headers=_headers(), timeout=TIMEOUT_S)
            status = r.status_code
            txt = r.text[:800] if isinstance(r.text, str) else ""
            r.raise_for_status()
            j = r.json()
            _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=None, resp_text=txt, outbound_ip=None)
            return j
        except Exception as e:
            last_err = e
            status = getattr(getattr(e, "response", None), "status_code", "")
            txt = ""
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                pass
            _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=str(e), resp_text=txt, outbound_ip=None)
            if status not in (451, 418, 429, 500, 503):
                break
            continue
    raise last_err if last_err else RuntimeError("api_get failed")

def api_signed_get(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    p = dict(params or {})
    p["timestamp"] = _ts()
    p["recvWindow"] = RECV_WINDOW
    qs = urllib.parse.urlencode(p, doseq=True)
    sig = _sign(qs)
    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    try:
        r = session.get(url, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        txt = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        _append_api_csv(logs_cc, method="GET", path=path, params=_sanitize_params(p), status=status, error=None, resp_text=txt, outbound_ip=None)
        return j
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", "")
        txt = ""
        try:
            ej = e.response.json()
            txt = json.dumps(ej, ensure_ascii=False)
        except Exception:
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                txt = ""
        _append_api_csv(logs_cc, method="GET", path=path, params=_sanitize_params(p), status=status, error=str(e), resp_text=txt, outbound_ip=None)
        raise

def api_signed_post(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    p = dict(params)
    p["timestamp"] = _ts()
    p["recvWindow"] = RECV_WINDOW
    qs = urllib.parse.urlencode(p, doseq=True)
    sig = _sign(qs)
    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    ip = _get_outbound_ip() if path == "/api/v3/order" else None
    try:
        r = session.post(url, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        txt = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        _append_api_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=None, resp_text=txt, outbound_ip=ip)
        return j
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", "")
        txt = ""
        try:
            ej = e.response.json()
            txt = json.dumps(ej, ensure_ascii=False)
        except Exception:
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                txt = ""
        _append_api_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=str(e), resp_text=txt, outbound_ip=ip)
        raise

def get_price(session: requests.Session, symbol: str, *, logs_cc=None) -> float:
    try:
        j = api_get(session, "/api/v3/ticker/price", {"symbol": symbol}, logs_cc=logs_cc)
        return float(j["price"])
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status in (451, 418, 429, 500, 503):
            data = api_get(session, "/api/v3/ticker/price", {}, logs_cc=logs_cc)
            mp = {str(d["symbol"]).upper(): float(d["price"]) for d in data}
            if symbol.upper() in mp:
                return mp[symbol.upper()]
        raise

# ========= Signals =========
def _parse_pairs_models(val: str) -> List[Tuple[str, str]]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece:
            continue
        pair, model = piece.split(":", 1)
        out.append((pair.strip().upper(), model.strip()))
    return out

def _load_master_signals_map(models_cc) -> Dict[Tuple[str, str], Dict[str, Any]]:
    import pandas as pd
    blob = models_cc.get_blob_client(MASTER_CSV_NAME)
    try:
        raw = blob.download_blob().readall()
    except ResourceNotFoundError:
        logger.error("Master CSV not found.")
        return {}

    df = pd.read_csv(io.BytesIO(raw))
    required = {"pair", "model", "B", "S", "date", "load_time_utc", "is_active", "score"}
    if not required.issubset(df.columns):
        logger.error("Master CSV missing columns, have: %s", df.columns.tolist())
        return {}

    cycles_col = "cycles" if "cycles" in df.columns else ("total_cycles" if "total_cycles" in df.columns else None)
    if cycles_col is None:
        logger.error("Master CSV musí obsahovat 'cycles' nebo 'total_cycles'.")
        return {}

    df["pair"] = df["pair"].astype(str).str.upper()
    df["model"] = df["model"].astype(str)
    df = df[(df["is_active"] == True) &
            (df[cycles_col] >= MIN_CYCLES_PER_DAY) &
            (df["score"] >= MIN_SCORE)].copy()
    if df.empty:
        return {}
    df["load_time_utc"] = pd.to_datetime(df["load_time_utc"], errors="coerce", utc=True)
    df = df.sort_values(["pair", "model", "date", "load_time_utc"])
    latest = df.groupby(["pair", "model"], as_index=False).tail(1)

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, r in latest.iterrows():
        key = (str(r["pair"]).upper(), str(r["model"]))
        out[key] = {
            "pair": key[0],
            "model": key[1],
            "B": float(r["B"]),
            "S": float(r["S"]),
            "date": str(r["date"]),
            "load_time_utc": (r["load_time_utc"].strftime("%Y-%m-%dT%H:%M:%SZ") if not pd.isna(r["load_time_utc"]) else None),
            "cycles": float(r[cycles_col]),
            "score": float(r["score"]),
        }
    return out

# ========= ExchangeInfo (cache) =========
def _pairs_only(val: str) -> List[str]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece:
            continue
        pair, _ = piece.split(":", 1)
        out.append(pair.strip().upper())
    seen = set(); res=[]
    for p in out:
        if p not in seen: seen.add(p); res.append(p)
    return res

def _exchangeinfo_blob_name_for_today() -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"exchangeinfo_{today}.json"

def _ensure_exchangeinfo_json_for_today(session: requests.Session, logs_cc, pairs: List[str]) -> Dict[str, Any]:
    blob_name = _exchangeinfo_blob_name_for_today()
    cached = _read_blob_json(logs_cc, blob_name)
    if cached and isinstance(cached, dict) and cached.get("asof_date"):
        return cached
    info = api_get(session, "/api/v3/exchangeInfo", {"symbols": json.dumps(pairs, separators=(',',':'))}, logs_cc=logs_cc)
    filt_map: Dict[str, Any] = {}
    for sym in info.get("symbols", []):
        sname = sym.get("symbol")
        status = sym.get("status")
        tick_size = step_size = min_notional = 0.0
        for f in sym.get("filters", []):
            ft = f.get("filterType")
            if ft == "PRICE_FILTER":
                tick_size = float(f.get("tickSize") or 0.0)
            elif ft == "LOT_SIZE":
                step_size = float(f.get("stepSize") or 0.0)
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                min_notional = float(f.get("minNotional") or 0.0)
        if sname:
            filt_map[sname.upper()] = {"status": status, "tickSize": tick_size, "stepSize": step_size, "minNotional": min_notional}
    payload = {"asof_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "filters": filt_map}
    _write_blob_json(logs_cc, blob_name, payload)
    return payload

def get_symbol_filters_cached(filters_json: Dict[str, Any], symbol: str) -> Tuple[float, float, float, str]:
    entry = (filters_json.get("filters") or {}).get(symbol.upper())
    if not entry:
        return (0.0, 0.0, 0.0, "UNKNOWN")
    return (float(entry.get("stepSize", 0.0)),
            float(entry.get("minNotional", 0.0)),
            float(entry.get("tickSize", 0.0)),
            str(entry.get("status", "TRADING") or "TRADING"))

# ========= Amount helpers =========
def floor_step(qty: float, step: float) -> float:
    if step <= 0:
        return float(qty)
    q = Decimal(str(qty))
    s = Decimal(str(step))
    steps = (q / s).to_integral_value(rounding=ROUND_FLOOR)
    return float(steps * s)

# ========= Account =========
def _get_free_asset(session: requests.Session, logs_cc, asset: str) -> float:
    # SIGNED: /api/v3/account
    j = api_signed_get(session, "/api/v3/account", {}, logs_cc=logs_cc)
    for b in j.get("balances", []):
        if b.get("asset") == asset:
            try: return float(b.get("free", "0"))
            except: return 0.0
    return 0.0

# ========= Orders =========
def place_market_buy_quote(session: requests.Session, symbol: str, quote_usdt: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol, "side": "BUY", "type": "MARKET",
        "quoteOrderQty": f"{quote_usdt:.8f}", "newOrderRespType": "FULL"
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

def place_market_sell_qty(session: requests.Session, symbol: str, qty: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol, "side": "SELL", "type": "MARKET",
        "quantity": f"{qty:.8f}", "newOrderRespType": "FULL"
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

# ========= Price parallel =========
def _fetch_prices_parallel(session: requests.Session, logs_cc, pairs: List[str]) -> Dict[str, float]:
    prices: Dict[str,float] = {}
    def task(pair: str):
        return pair, get_price(session, pair, logs_cc=logs_cc)

    with ThreadPoolExecutor(max_workers=max(1, PRICE_FETCH_WORKERS)) as ex:
        futs = { ex.submit(task, p): p for p in pairs }
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                sym, price = fut.result()
                prices[sym] = price
            except Exception as e:
                logger.warning("[%s] price fetch failed: %s", p, e)
    return prices

# ========= Core =========
def _state_blob_name(pair: str, model: str) -> str:
    return f"{pair}_{model}.json"

def _avg_fill_price(fills: List[Dict[str,Any]]) -> float:
    if not fills: return 0.0
    total_qty = 0.0; total_quote = 0.0
    for f in fills:
        p = float(f["price"]); q = float(f["qty"])
        total_qty += q; total_quote += p*q
    return (total_quote / total_qty) if total_qty>0 else 0.0

def _sum_fee(fills: List[Dict[str,Any]]) -> Tuple[float,str]:
    total = 0.0; asset = None
    for f in fills:
        total += float(f.get("commission", 0) or 0)
        asset = f.get("commissionAsset", asset)
    return total, (asset or "")

def run_decision_for_pair(session: requests.Session,
                          models_cc, state_cc, logs_cc,
                          pair: str, model: str,
                          sig_row: Optional[Dict[str,Any]],
                          exch_cache: Dict[str,Any],
                          current_price: Optional[float]) -> None:
    if not sig_row:
        logger.info("[%s/%s] No active signal passing thresholds — skipping.", pair, model)
        return

    B = float(sig_row["B"]); S = float(sig_row["S"])
    s_name = _state_blob_name(pair, model)
    st = _read_blob_json(state_cc, s_name) or {"position":"flat","qty":0.0,"entry_price":None,"entry_quote":0.0,"b_level":None,"s_level":None,"signal_date":None}

    if current_price is None:
        try:
            current_price = get_price(session, pair, logs_cc=logs_cc)
        except Exception as e:
            logger.warning("[%s/%s] Missing current price → skip (%s)", pair, model, e)
            return
    px = float(current_price)

    step, min_notional, _tick, status = get_symbol_filters_cached(exch_cache, pair)
    if status and status != "TRADING":
        logger.info("[%s/%s] status=%s → skip", pair, model, status); return

    base = pair.replace("USDT","").replace("USDC","").replace("BUSD","")
    quote = "USDT" if pair.endswith("USDT") else ("USDC" if pair.endswith("USDC") else "USD")

    # ===== SELL =====
    target_S = st.get("s_level") if st.get("s_level") is not None else S
    if st.get("position") == "long" and px >= float(target_S):
        free_base = _get_free_asset(session, logs_cc, base)
        raw = min(float(st.get("qty", 0.0)), float(free_base))
        raw = max(0.0, raw - (step * 1e-3))
        qty_sell = floor_step(raw, step)

        if qty_sell <= 0:
            logger.info("[%s/%s] SELL qty=0 after floor (free_base=%.8f, step=%.8f) → skip", pair, model, free_base, step)
            return

        if min_notional and (qty_sell * px) < min_notional:
            logger.info("[%s/%s] SELL under minNotional (%.8f < %.8f) → skip", pair, model, qty_sell*px, min_notional)
            return

        try:
            odr = place_market_sell_qty(session, pair, qty_sell, logs_cc=logs_cc)
        except Exception as e:
            logger.warning("[%s/%s] SELL order failed: %s", pair, model, e)
            return

        executed_qty = float(odr.get("executedQty", "0") or 0)
        cq = float(odr.get("cummulativeQuoteQty", "0") or 0)
        fills = odr.get("fills", []) or []
        avgp = _avg_fill_price(fills)
        fee_total, fee_asset = _sum_fee(fills)

        free_after = _get_free_asset(session, logs_cc, base)
        rem = floor_step(float(free_after), step)

        if rem > 0.0:
            new_state = {
                "position": "long",
                "qty": rem,
                "entry_price": st.get("entry_price"),
                "entry_quote": st.get("entry_quote"),
                "b_level": st.get("b_level"),
                "s_level": st.get("s_level"),
                "signal_date": st.get("signal_date")
            }
        else:
            new_state = {"position":"flat","qty":0.0,"entry_price":None,"entry_quote":0.0,"b_level":None,"s_level":None,"signal_date":None}

        _write_blob_json(state_cc, s_name, new_state)

        line = ",".join([
            _timestamp(), pair, model, "SELL",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cq:.8f}",
            f"{fee_total:.8f}",
            fee_asset or "",
            str(odr.get("orderId","")),
            f"{float(st.get('b_level') if st.get('b_level') is not None else B):.8f}",
            f"{float(target_S):.8f}",
            str(st.get("signal_date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)
        logger.info("[%s/%s] SELL filled qty=%.8f @ %.8f, rem=%.8f", pair, model, executed_qty, avgp, rem)
        return

    # ===== BUY =====
    target_B = st.get("b_level") if st.get("b_level") is not None else B
    if st.get("position") == "flat" and px <= float(target_B):
        if min_notional and ORDER_USDT < min_notional:
            logger.info("[%s/%s] BUY under minNotional (%.4f < %.4f) → skip", pair, model, ORDER_USDT, min_notional)
            return

        try:
            odr = place_market_buy_quote(session, pair, ORDER_USDT, logs_cc=logs_cc)
        except Exception as e:
            logger.warning("[%s/%s] BUY order failed: %s", pair, model, e)
            return

        executed_qty = float(odr.get("executedQty","0") or 0)
        cq = float(odr.get("cummulativeQuoteQty","0") or 0)
        fills = odr.get("fills", []) or []
        avgp = _avg_fill_price(fills)
        fee_total, fee_asset = _sum_fee(fills)

        # --- preferovaný způsob: skutečný zůstatek po BUY (SIGNED /account)
        qty_hold = None
        try:
            free_after_buy = _get_free_asset(session, logs_cc, base)
            qty_hold = floor_step(float(free_after_buy), step)
        except Exception as e:
            logger.warning("[%s/%s] BUY: failed to read account balance (fallback to fills): %s", pair, model, e)

        # --- fallback: dopočítej z fillů (odečti fee v BASE)
        if qty_hold is None:
            hold = float(executed_qty)
            if (fee_asset or "").upper() == base.upper():
                hold = max(0.0, hold - float(fee_total))
            qty_hold = floor_step(hold, step)

        new_state = {
            "position": "long",
            "qty": qty_hold,
            "entry_price": avgp,
            "entry_quote": cq,
            "b_level": float(B),
            "s_level": float(S),
            "signal_date": sig_row.get("date")
        }
        _write_blob_json(state_cc, s_name, new_state)

        line = ",".join([
            _timestamp(), pair, model, "BUY",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cq:.8f}",
            f"{fee_total:.8f}",
            fee_asset or "",
            str(odr.get("orderId","")),
            f"{float(B):.8f}",
            f"{float(S):.8f}",
            str(sig_row.get("date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)
        logger.info("[%s/%s] BUY filled qty=%.8f @ %.8f (hold=%.8f)", pair, model, executed_qty, avgp, qty_hold)
        return

    logger.info("[%s/%s] No action (pos=%s px=%.8f B=%.8f S=%.8f)", pair, model, st.get("position"), px, B, S)

# ========= Egress IP (jednorázové info) =========
def _egress_ip_info_once():
    ip = _get_outbound_ip()
    logger.info("Outbound IP=%s BASE_URL=%s TESTNET=%s", ip, BASE_URL, USE_TESTNET)

# ========= Orchestration =========
def main(mytimer: func.TimerRequest) -> None:
    try:
        _, models_cc, state_cc, logs_cc = _make_blob_clients()

        pairs_models = _parse_pairs_models(TRADE_PAIRS_MODELS)
        if not pairs_models:
            logger.error("TRADE_PAIRS_MODELS is empty"); return
        pairs = []
        seen=set()
        for piece in TRADE_PAIRS_MODELS.split(","):
            piece=piece.strip()
            if not piece or ":" not in piece: continue
            p,_=piece.split(":",1); p=p.strip().upper()
            if p not in seen: seen.add(p); pairs.append(p)

        session = requests.Session()
        _egress_ip_info_once()

        exch = _ensure_exchangeinfo_json_for_today(session, logs_cc, pairs)
        sig_map = _load_master_signals_map(models_cc)

        # ceny paralelně
        prices: Dict[str, float] = {}
        def task(pair: str):
            return pair, get_price(session, pair, logs_cc=logs_cc)
        with ThreadPoolExecutor(max_workers=max(1, PRICE_FETCH_WORKERS)) as ex:
            futs = { ex.submit(task, p): p for p in pairs }
            for fut in as_completed(futs):
                p = futs[fut]
                try:
                    sym, price = fut.result()
                    prices[sym] = price
                except Exception as e:
                    logger.warning("[%s] price fetch failed: %s", p, e)

        for pair, model in pairs_models:
            sig = sig_map.get((pair, model))
            px = prices.get(pair)
            run_decision_for_pair(session, models_cc, state_cc, logs_cc, pair, model, sig, exch, px)

    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
