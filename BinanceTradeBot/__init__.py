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
from typing import Dict, Any, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func
import requests

# ====================== ENV ======================

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

def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "": return default
    return str(raw).lower() in ("1","true","yes","y","on")

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

# Binance API
BINANCE_API_KEY        = _get_env("BINANCE_API_KEY", required=True)
BINANCE_API_SECRET     = _get_env("BINANCE_API_SECRET", required=True)
USE_TESTNET            = (_get_env("BINANCE_TESTNET", "true").lower() in ("1","true","yes"))
RECV_WINDOW            = _get_int("BINANCE_RECV_WINDOW", 5000)
TIMEOUT_S              = _get_int("BINANCE_HTTP_TIMEOUT", 15)

# výkon
PRICE_FETCH_WORKERS    = _get_int("PRICE_FETCH_WORKERS", 10)  # paralelní vlákna pro ticker/price
API_CSV_LOGGING        = _get_bool("API_CSV_LOGGING", True)   # vypnout na produkci lze nastavením 0/false

# API log – jeden společný soubor pro všechny běhy
API_LOG_FILE_NAME      = os.getenv("API_LOG_FILE_NAME", "api_calls.csv")

BASE_URL = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"

logger = logging.getLogger("BinanceTradeBot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ====================== Azure Blob helpers ======================

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

# --- Append řádek do textového blobu přes block list (bez download) ---
def _append_text_blob_blocklist(container_client, blob_name: str, text_to_append: str):
    """
    Bezpečný append pro text: stáhne jen seznam commited block IDs, přidá nový block a commitne.
    Funguje na BlockBlob (nenutí AppendBlob).
    """
    from azure.storage.blob import BlobBlock
    from azure.core.exceptions import ResourceNotFoundError
    import base64, secrets

    bc = container_client.get_blob_client(blob_name)

    # pokud blob neexistuje, vytvoř prázdný
    try:
        bc.get_blob_properties()
    except ResourceNotFoundError:
        bc.upload_blob(b"", overwrite=True)

    # získej existující block list
    bl = bc.get_block_list(block_list_type="committed")
    committed = []
    if hasattr(bl, "committed_blocks") and bl.committed_blocks:
        committed = [b.id for b in bl.committed_blocks if getattr(b, "id", None)]
    elif isinstance(bl, list):
        committed = [getattr(b, "id", None) for b in bl if getattr(b, "id", None)]

    # nový block
    block_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    bc.stage_block(block_id=block_id, data=text_to_append.encode("utf-8"))

    # commit combined
    new_list = [BlobBlock(bid) for bid in committed] + [BlobBlock(block_id)]
    bc.commit_block_list(new_list)

# ====================== TRADE CSV (hlavička + append) ======================

def _ensure_trade_header(logs_cc, blob_name: str, header: str):
    """
    Zajistí, že trade CSV má vždy hlavičku jako první řádek.
    - pokud blob neexistuje → vytvoří s hlavičkou
    - pokud je prázdný → zapíše hlavičku
    - pokud existuje a hlavička chybí → stáhne celé tělo a přepíše na 'header + původní obsah'
    """
    from azure.core.exceptions import ResourceNotFoundError

    bc = logs_cc.get_blob_client(blob_name)
    try:
        props = bc.get_blob_properties()
        size = int(getattr(props, "size", 0) or 0)
        if size == 0:
            bc.upload_blob(header.encode("utf-8"), overwrite=True)
            return

        # přečti prvních pár bajtů a ověř hlavičku
        head = b""
        try:
            head = bc.download_blob(offset=0, length=max(256, len(header))).readall()
        except Exception:
            head = b""
        head_txt = head.decode("utf-8", errors="ignore")
        if not head_txt.startswith(header):
            full = bc.download_blob().readall()
            new_body = header.encode("utf-8") + full
            bc.upload_blob(new_body, overwrite=True)
    except ResourceNotFoundError:
        bc.upload_blob(header.encode("utf-8"), overwrite=True)

def _append_trade_line(logs_cc, blob_name: str, line: str):
    header = "time_utc,pair,model,side,executedQty,avgFillPrice,cummulativeQuoteQty,fee_total,fee_asset,orderId,b_level,s_level,b_signal_date\n"
    _ensure_trade_header(logs_cc, blob_name, header)
    _append_text_blob_blocklist(logs_cc, blob_name, line)

# ====================== API log (jeden soubor, hlavička + append přes block-list) ======================

API_LOG_COLUMNS = ["ts","method","path","params","status","error","resp_sample"]

def _api_log_blob_name() -> str:
    return API_LOG_FILE_NAME  # jeden soubor pro všechny běhy

def _csv_line(values: List[Any]) -> str:
    sio = io.StringIO()
    w = csv.writer(sio, lineterminator="\n")
    w.writerow(values)
    return sio.getvalue()

def _ensure_api_log_header(logs_cc):
    """
    Zajistí, že API log CSV má vždy hlavičku jako první řádek (BlockBlob varianta).
    - pokud blob neexistuje → vytvoří s hlavičkou
    - pokud je prázdný → zapíše hlavičku
    - pokud existuje a hlavička chybí → stáhne celé tělo a přepíše na 'header + původní obsah'
    """
    from azure.core.exceptions import ResourceNotFoundError
    blob_name = _api_log_blob_name()
    bc = logs_cc.get_blob_client(blob_name)
    header = _csv_line(API_LOG_COLUMNS)

    try:
        props = bc.get_blob_properties()
        size = int(getattr(props, "size", 0) or 0)
        if size == 0:
            bc.upload_blob(header.encode("utf-8"), overwrite=True)
            return

        # ověř prefix
        head = b""
        try:
            head = bc.download_blob(offset=0, length=max(len(header), 1024)).readall()
        except Exception:
            head = b""
        head_txt = head.decode("utf-8", errors="ignore")
        if not head_txt.startswith(header):
            full = bc.download_blob().readall()
            bc.upload_blob(header.encode("utf-8") + full, overwrite=True)
    except ResourceNotFoundError:
        bc.upload_blob(header.encode("utf-8"), overwrite=True)

def _append_api_csv_row(logs_cc, row_values: List[Any]):
    _ensure_api_log_header(logs_cc)
    line = _csv_line(row_values)
    _append_text_blob_blocklist(logs_cc, _api_log_blob_name(), line)

def _sanitize_params(p: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(p, dict):
        return {}
    cleaned = dict(p)
    cleaned.pop("signature", None)
    return cleaned

def _append_api_csv(logs_cc, *, method: str, path: str, params: Dict[str, Any], status: Any, error: Optional[str], resp_text: Optional[str]):
    if not API_CSV_LOGGING:
        return
    # bezpečná serializace
    par = _sanitize_params(params or {})
    params_ser = json.dumps(par, ensure_ascii=False, separators=(",", ":"))
    sample = (resp_text or "")
    if isinstance(sample, str) and len(sample) > 1000:
        sample = sample[:1000] + "..."

    row = [
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        method,
        path,
        params_ser,
        str(status or ""),
        (error or ""),
        sample.replace("\r"," ").replace("\n"," "),
    ]
    _append_api_csv_row(logs_cc, row)

# ====================== Master CSV loader (1× za tik) ======================

def _parse_pairs_models(val: str) -> List[Tuple[str,str]]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece: continue
        pair, model = piece.split(":",1)
        out.append((pair.strip().upper(), model.strip()))
    return out

def _load_master_signals_map(models_cc) -> Dict[Tuple[str,str], Dict[str,Any]]:
    """
    Načte master CSV 1×, vyfiltruje is_active a prahy (MIN_CYCLES_PER_DAY, MIN_SCORE),
    a pro každé (pair, model) vrátí poslední (podle date, load_time_utc) řádek s B/S.
    """
    import pandas as pd
    from azure.core.exceptions import ResourceNotFoundError

    blob = models_cc.get_blob_client(MASTER_CSV_NAME)
    try:
        raw = blob.download_blob().readall()
    except ResourceNotFoundError:
        logger.error("Master CSV not found.")
        return {}

    df = pd.read_csv(io.BytesIO(raw))
    required = {"pair","model","B","S","date","load_time_utc","is_active","score"}
    if not required.issubset(df.columns):
        logger.error("Master CSV missing columns, have: %s", df.columns.tolist()); return {}

    # alias pro počet cyklů
    cycles_col = "cycles" if "cycles" in df.columns else ("total_cycles" if "total_cycles" in df.columns else None)
    if cycles_col is None:
        logger.error("Master CSV musí obsahovat 'cycles' nebo 'total_cycles'."); return {}

    # normalizace
    df["pair"] = df["pair"].astype(str).str.upper()
    df["model"] = df["model"].astype(str)

    # filtry
    df = df[(df["is_active"]==True) &
            (df[cycles_col] >= MIN_CYCLES_PER_DAY) &
            (df["score"]  >= MIN_SCORE)].copy()
    if df.empty:
        return {}

    # parse čas
    df["load_time_utc"] = pd.to_datetime(df["load_time_utc"], errors="coerce", utc=True)

    # vezmi poslední per (pair, model)
    df = df.sort_values(["pair","model","date","load_time_utc"])
    latest = df.groupby(["pair","model"], as_index=False).tail(1)

    # výstupní mapa
    out: Dict[Tuple[str,str], Dict[str,Any]] = {}
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
            "score": float(r["score"])
        }
    return out

# ====================== Binance REST helpers (Session + CSV log) ======================

def _sign(query: str) -> str:
    return hmac.new(BINANCE_API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)

def _headers() -> Dict[str,str]:
    return {"X-MBX-APIKEY": BINANCE_API_KEY}

def api_get(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    url = f"{BASE_URL}{path}"
    try:
        r = session.get(url, params=params, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        text_sample = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=None, resp_text=text_sample)
        return j
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", "")
        text = ""
        try:
            text = e.response.text[:800] if getattr(e, "response", None) is not None else ""
        except Exception:
            text = ""
        _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=str(e), resp_text=text)
        raise

def api_signed_post(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    p = dict(params)
    p["timestamp"] = _ts()
    p["recvWindow"] = RECV_WINDOW
    qs = urllib.parse.urlencode(p, doseq=True)
    sig = _sign(qs)
    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    try:
        r = session.post(url, headers=_headers(), timeout=TIMEOUT_S)
        status = r.status_code
        text_sample = r.text[:800] if isinstance(r.text, str) else ""
        r.raise_for_status()
        j = r.json()
        _append_api_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=None, resp_text=text_sample)
        return j
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", "")
        text = ""
        try:
            text = e.response.text[:800] if getattr(e, "response", None) is not None else ""
        except Exception:
            text = ""
        _append_api_csv(logs_cc, method="POST", path=path, params=_sanitize_params(p), status=status, error=str(e), resp_text=text)
        raise

def get_price(session: requests.Session, symbol: str, *, logs_cc=None) -> float:
    j = api_get(session, "/api/v3/ticker/price", {"symbol": symbol}, logs_cc=logs_cc)
    return float(j["price"])

# ====================== exchangeInfo cache (1× denně) ======================

def _pairs_only(val: str) -> List[str]:
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece or ":" not in piece: continue
        pair, _ = piece.split(":",1)
        out.append(pair.strip().upper())
    # dedup
    seen=set(); res=[]
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
        logger.info("[exchangeInfo] using cached %s", blob_name)
        return cached

    symbols_param = json.dumps(pairs, separators=(",", ":"))
    info = api_get(session, "/api/v3/exchangeInfo", {"symbols": symbols_param}, logs_cc=logs_cc)

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

# ====================== Orders ======================

def place_market_buy_quote(session: requests.Session, symbol: str, quote_usdt: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": f"{quote_usdt:.8f}",
        "newOrderRespType": "FULL"
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

def place_market_sell_qty(session: requests.Session, symbol: str, qty: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": f"{qty:.8f}",
        "newOrderRespType": "FULL"
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

# ====================== Bot core ======================

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

def _fetch_prices_parallel(session: requests.Session, logs_cc, pairs: List[str]) -> Dict[str, float]:
    """
    Stáhne /ticker/price pro všechny páry paralelně, vrátí mapu {pair: price}.
    """
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

def run_decision_for_pair(session: requests.Session,
                          models_cc, state_cc, logs_cc,
                          pair: str, model: str,
                          sig_row: Optional[Dict[str,Any]],
                          exch_cache: Dict[str,Any],
                          current_price: Optional[float]) -> None:
    # pokud nemáme platný signál, přeskoč
    if not sig_row:
        logger.info("[%s/%s] No active signal passing thresholds — skipping.", pair, model)
        return

    B = float(sig_row["B"]); S = float(sig_row["S"])

    # načti stav
    s_name = _state_blob_name(pair, model)
    st = _read_blob_json(state_cc, s_name) or {
        "position": "flat",
        "qty": 0.0,
        "entry_price": None,
        "entry_quote": 0.0,
        "b_level": None, "s_level": None,
        "signal_date": None
    }

    # cena
    if current_price is None:
        logger.warning("[%s/%s] Missing current price → skip.", pair, model)
        return
    px = float(current_price)

    # filtry
    step, min_notional, _tick, status = get_symbol_filters_cached(exch_cache, pair)
    if status and status != "TRADING":
        logger.info("[%s/%s] status=%s → skipping trading", pair, model, status)
        return

    # SELL
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
            odr = place_market_sell_qty(session, pair, qty_sell, logs_cc=logs_cc)
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

    # BUY
    if st["position"] == "flat" and px <= B:
        if min_notional and ORDER_USDT < min_notional:
            logger.info("[%s/%s] BUY under minNotional (%.4f < %.4f) → skip",
                        pair, model, ORDER_USDT, min_notional)
            return
        try:
            odr = place_market_buy_quote(session, pair, ORDER_USDT, logs_cc=logs_cc)
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
            str(sig_row.get("date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)

        st = {
            "position": "long",
            "qty": executed_qty,
            "entry_price": avgp,
            "entry_quote": cq,
            "b_level": B,
            "s_level": S,
            "signal_date": sig_row.get("date")
        }
        _write_blob_json(state_cc, s_name, st)
        logger.info("[%s/%s] BUY filled qty=%s avg=%.6f cq=%.6f", pair, model, executed_qty, avgp, cq)
        return

    logger.info("[%s/%s] No action. px=%.6f B=%.6f S=%.6f pos=%s", pair, model, px, B, S, st["position"])

# ====================== Azure Function entry ======================

def main(mytimer: func.TimerRequest) -> None:
    try:
        _, models_cc, state_cc, logs_cc = _make_blob_clients()

        # 0) Parsuj páry/modely + výpis pro diagnózu
        pairs_models = _parse_pairs_models(TRADE_PAIRS_MODELS)
        if not pairs_models:
            logger.error("TRADE_PAIRS_MODELS is empty"); return
        pairs = sorted({p for (p, _m) in pairs_models})
        logger.info("Parsed %d pairs, %d pair-model combos", len(pairs), len(pairs_models))

        # 1) Session reuse
        session = requests.Session()

        # 2) exchangeInfo cache 1× denně
        exchangeinfo_today = _ensure_exchangeinfo_json_for_today(session, logs_cc, pairs)

        # 3) master CSV load 1× za tik
        sig_map = _load_master_signals_map(models_cc)

        # 4) ceny paralelně
        prices = _fetch_prices_parallel(session, logs_cc, pairs)

        # 5) rozhodnutí per pair-model (sekvenčně; ordery řešíme jednotlivě)
        start = time.time()
        for pair, model in pairs_models:
            sig_row = sig_map.get((pair, model))
            price = prices.get(pair)
            run_decision_for_pair(session, models_cc, state_cc, logs_cc, pair, model, sig_row, exchangeinfo_today, price)
        dur = time.time() - start
        logger.info("Tick finished in %.2fs for %d pairs", dur, len(pairs))

    except Exception:
        logger.exception("[BinanceTradeBot] Unhandled exception in main()")
        raise
