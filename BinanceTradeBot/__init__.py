import os
import io
import csv
import json
import hmac
import time
import hashlib
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobType

# -----------------------
# Logování
# -----------------------
logger = logging.getLogger("trade-func")
logger.setLevel(logging.INFO)

# -----------------------
# ENV
# -----------------------
USE_TESTNET = os.getenv("BINANCE_TESTNET", "false").strip().lower() in ("1", "true", "yes")
BASE_URL = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"
API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip().encode("utf-8")
TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "20"))
RECV_WINDOW = int(os.getenv("RECV_WINDOW_MS", "5000"))

TRADE_PAIRS_MODELS = os.getenv("TRADE_PAIRS_MODELS", "").strip()  # např. "AVAXUSDC:BS_WeightedUtility"
ORDER_QUOTE = float(os.getenv("ORDER_QUOTE", "10"))               # kolik quote (USDC/USDT) utratit při BUY
MIN_SCORE = float(os.getenv("MIN_SCORE", "0"))
MIN_CYCLES_PER_DAY = float(os.getenv("MIN_CYCLES_PER_DAY", "1"))
SAFE_MODE = os.getenv("SAFE_MODE", "false").strip().lower() in ("1", "true", "yes")

AZ_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
STATE_CONTAINER = os.getenv("STATE_CONTAINER", "bot-state")
LOGS_CONTAINER = os.getenv("LOGS_CONTAINER", "trade-logs")
SIGNALS_CONTAINER = os.getenv("SIGNALS_CONTAINER", "signals")
SIGNALS_BLOB = os.getenv("SIGNALS_BLOB", "bs_levels_master.csv")

API_CSV_LOGGING = os.getenv("API_CSV_LOGGING", "false").strip().lower() in ("1", "true", "yes")

# -----------------------
# Globální stav
# -----------------------
_SERVER_TIME_OFFSET_MS = 0

# -----------------------
# Pomocné funkce
# -----------------------
def _headers() -> Dict[str, str]:
    return {
        "X-MBX-APIKEY": API_KEY,
        "Accept": "application/json",
        "User-Agent": "functions-trader/1.0",
    }

def _sign(qs: str) -> str:
    return hmac.new(API_SECRET, qs.encode("utf-8"), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000 + _SERVER_TIME_OFFSET_MS)

def _timestamp() -> str:
    return dt.datetime.utcnow().isoformat() + "Z"

def _parse_pairs_models(spec: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not spec:
        return out
    for part in spec.split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        sym, model = part.split(":", 1)
        out.append((sym.strip().upper(), model.strip()))
    return out

def _egress_ip_info():
    import urllib.request
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as r:
            ip = r.read().decode()
            logger.info("Egress public IP detected as %s", ip)
    except Exception as e:
        logger.warning("Failed to detect egress IP: %s", e)

def _append_api_csv(logs_cc, *, method: str, path: str, params: Dict[str, Any], status: Any, error: Optional[str], resp_text: str):
    if not API_CSV_LOGGING:
        return
    try:
        name = dt.datetime.utcnow().strftime("logs/%Y_%m_%d/api.csv")
        bc = logs_cc.get_blob_client(name)
        header = "ts,method,path,params,status,error,resp_text\n"
        line = f'{_timestamp()},{method},{path},"{json.dumps(params, ensure_ascii=False)}",{status or ""},"{(error or "").replace(",", " ").replace("\n"," ")}","{(resp_text or "").replace(",", " ").replace("\n", " ")[:1000]}"\n'
        try:
            bc.get_blob_properties()
        except Exception:
            bc.upload_blob(header.encode("utf-8"), overwrite=True, content_settings=ContentSettings(content_type="text/csv"))
        bc.append_block(line.encode("utf-8"))
    except Exception as e:
        logger.debug("API csv log failed: %s", e)

def api_get(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    url = f"{BASE_URL}{path}"
    attempts, backoff = 4, 0.5
    for i in range(attempts):
        try:
            r = session.get(url, params=params, headers=_headers(), timeout=TIMEOUT_S)
            status = r.status_code
            txt = r.text[:800] if isinstance(r.text, str) else ""
            r.raise_for_status()
            j = r.json()
            _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=None, resp_text=txt)
            return j
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", "")
            if status in (418, 429, 500, 503) and i < attempts - 1:
                time.sleep(backoff); backoff *= 2
                continue
            txt = ""
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                pass
            _append_api_csv(logs_cc, method="GET", path=path, params=params, status=status, error=str(e), resp_text=txt)
            raise

def _signed_url(path: str, params: Dict[str, Any]) -> str:
    q = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())]) if params else ""
    if q:
        q += "&"
    q += f"timestamp={_ts()}&recvWindow={RECV_WINDOW}"
    sig = _sign(q)
    return f"{BASE_URL}{path}?{q}&signature={sig}"

def api_signed_get(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    url = _signed_url(path, params)
    attempts, backoff = 4, 0.5
    for i in range(attempts):
        try:
            r = session.get(url, headers=_headers(), timeout=TIMEOUT_S)
            status = r.status_code
            txt = r.text[:800] if isinstance(r.text, str) else ""
            r.raise_for_status()
            j = r.json()
            _append_api_csv(logs_cc, method="SIGNED_GET", path=path, params=params, status=status, error=None, resp_text=txt)
            return j
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", "")
            if status in (418, 429, 500, 503) and i < attempts - 1:
                time.sleep(backoff); backoff *= 2
                continue
            txt = ""
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                pass
            _append_api_csv(logs_cc, method="SIGNED_GET", path=path, params=params, status=status, error=str(e), resp_text=txt)
            raise

def api_signed_post(session: requests.Session, path: str, params: Dict[str, Any], *, logs_cc=None) -> Any:
    url = _signed_url(path, params)
    attempts, backoff = 4, 0.5
    for i in range(attempts):
        try:
            r = session.post(url, headers=_headers(), timeout=TIMEOUT_S)
            status = r.status_code
            txt = r.text[:800] if isinstance(r.text, str) else ""
            r.raise_for_status()
            j = r.json()
            _append_api_csv(logs_cc, method="SIGNED_POST", path=path, params=params, status=status, error=None, resp_text=txt)
            return j
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", "")
            if status in (418, 429, 500, 503) and i < attempts - 1:
                time.sleep(backoff); backoff *= 2
                continue
            txt = ""
            try:
                txt = e.response.text[:800] if getattr(e, "response", None) is not None else ""
            except Exception:
                pass
            _append_api_csv(logs_cc, method="SIGNED_POST", path=path, params=params, status=status, error=str(e), resp_text=txt)
            raise

def _sync_server_time(session: requests.Session, logs_cc=None):
    global _SERVER_TIME_OFFSET_MS
    try:
        j = api_get(session, "/api/v3/time", {}, logs_cc=logs_cc)
        srv = int(j.get("serverTime", 0))
        loc = int(time.time() * 1000)
        _SERVER_TIME_OFFSET_MS = srv - loc
        logger.info("Server time offset set to %d ms", _SERVER_TIME_OFFSET_MS)
    except Exception as e:
        logger.warning("Failed to sync server time: %s", e)

# -----------------------
# Exchange info
# -----------------------
def _fetch_exchange_info(session: requests.Session, logs_cc, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    j = api_get(session, "/api/v3/exchangeInfo", {"symbols": json.dumps(symbols)}, logs_cc=logs_cc)
    out = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        sym = s["symbol"]
        tick_size = step_size = min_notional = min_qty = None
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick_size = float(f.get("tickSize"))
            elif t == "LOT_SIZE":
                step_size = float(f.get("stepSize"))
                min_qty = float(f.get("minQty"))
            elif t in ("MIN_NOTIONAL", "NOTIONAL"):
                min_notional = float(f.get("minNotional"))
        out[sym] = {
            "base": s.get("baseAsset"),
            "quote": s.get("quoteAsset"),
            "tickSize": tick_size or 0.0,
            "stepSize": step_size or 0.0,
            "minQty": min_qty or 0.0,
            "minNotional": min_notional or 0.0,
        }
    return out

# -----------------------
# Prices
# -----------------------
def _fetch_prices_bulk(session: requests.Session, logs_cc, pairs: List[str]) -> Dict[str, float]:
    try:
        data = api_get(session, "/api/v3/ticker/price", {}, logs_cc=logs_cc)
        allp = {str(d["symbol"]).upper(): float(d["price"]) for d in data}
        return {p: allp[p] for p in pairs if p in allp}
    except Exception as e:
        logger.warning("Bulk price fetch failed: %s", e)
        out = {}
        for p in pairs:
            j = api_get(session, "/api/v3/ticker/price", {"symbol": p}, logs_cc=logs_cc)
            out[p] = float(j["price"])
        return out

# -----------------------
# Storage
# -----------------------
def _blob_clients():
    bsc = BlobServiceClient.from_connection_string(AZ_CONN_STR)
    return (
        bsc.get_container_client(STATE_CONTAINER),
        bsc.get_container_client(LOGS_CONTAINER),
        bsc.get_container_client(SIGNALS_CONTAINER),
    )

def _ensure_container(cc):
    try:
        cc.create_container()
    except Exception:
        pass

def _trade_log_blob_name(pair: str) -> str:
    # sjednocený path pro trade CSV
    return f"trades/{pair}.csv"

_TRADE_HEADER = "ts,pair,model,side,qty,avg_price,quote_qty,fee,fee_asset,order_id,B,S,signal_date\n"

def _ensure_trade_log_append_blob_with_header(logs_cc, blob_name: str):
    bc = logs_cc.get_blob_client(blob_name)
    header_b = _TRADE_HEADER.encode("utf-8")
    try:
        props = bc.get_blob_properties()
        if props.blob_type != BlobType.AppendBlob:
            data = bc.download_blob().readall()
            bc.delete_blob()
            bc.create_append_blob()
            # vložit hlavičku, pokud v původním souboru chyběla
            if not data or not data.startswith(header_b):
                bc.append_block(header_b)
            if data:
                bc.append_block(data)
        else:
            # ověř hlavičku
            try:
                first = bc.download_blob(offset=0, length=len(header_b)).readall()
            except Exception:
                first = b""
            if not first or not first.decode("utf-8", errors="ignore").startswith("ts,"):
                tmp = bc.download_blob().readall()
                bc.delete_blob()
                bc.create_append_blob()
                bc.append_block(header_b)
                if tmp:
                    bc.append_block(tmp)
    except ResourceNotFoundError:
        bc.create_append_blob()
        bc.append_block(header_b)

def _append_trade_line(logs_cc, blob_name: str, line: str):
    try:
        _ensure_trade_log_append_blob_with_header(logs_cc, blob_name)
        bc = logs_cc.get_blob_client(blob_name)
        bc.append_block(line.encode("utf-8"))
    except Exception as e:
        logger.warning("Trade CSV append failed for %s: %s", blob_name, e)

def _write_blob_json(state_cc, name: str, obj: Dict[str, Any]):
    bc = state_cc.get_blob_client(name)
    data = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    bc.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type="application/json"))

def _read_blob_text(cc, name: str) -> Optional[str]:
    bc = cc.get_blob_client(name)
    try:
        return bc.download_blob().readall().decode("utf-8")
    except Exception:
        return None

# -----------------------
# Signals
# -----------------------
def _load_signals(signals_cc) -> List[Dict[str, Any]]:
    txt = _read_blob_text(signals_cc, SIGNALS_BLOB)
    if not txt:
        logger.warning("Signals CSV not found: %s/%s", SIGNALS_CONTAINER, SIGNALS_BLOB)
        return []
    out: List[Dict[str, Any]] = []
    rdr = csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            row["pair"] = row.get("pair", "").upper()
            row["model"] = row.get("model", "")
            row["is_active"] = str(row.get("is_active", "true")).lower() in ("1", "true", "yes")
            row["score"] = float(row.get("score", "0") or 0)
            row["cycles"] = float(row.get("cycles", row.get("total_cycles", "0")) or 0)
            row["B"] = float(row.get("B") or row.get("b") or 0)
            row["S"] = float(row.get("S") or row.get("s") or 0)
            out.append(row)
        except Exception as e:
            logger.warning("Bad signals row skipped: %s (%s)", row, e)
    return out

def _select_signal(sig_rows: List[Dict[str, Any]], pair: str, model: str) -> Optional[Dict[str, Any]]:
    cand = [r for r in sig_rows if r.get("pair") == pair and r.get("model") == model and r.get("is_active")]
    cand = [r for r in cand if r.get("score", 0) >= MIN_SCORE and r.get("cycles", 0) >= MIN_CYCLES_PER_DAY]
    if not cand:
        return None
    try:
        cand.sort(key=lambda r: r.get("date", ""))
    except Exception:
        pass
    return cand[-1]

# -----------------------
# Account helpers
# -----------------------
def _get_free_asset(session: requests.Session, logs_cc, asset: str) -> float:
    acct = api_signed_get(session, "/api/v3/account", {}, logs_cc=logs_cc)
    for b in acct.get("balances", []):
        if b.get("asset") == asset:
            try:
                return float(b.get("free", "0"))
            except Exception:
                return 0.0
    return 0.0

# -----------------------
# Objednávky (MARKET)
# -----------------------
def place_market_buy_quote(session: requests.Session, symbol: str, quote_qty: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": f"{quote_qty:.8f}",
        "newOrderRespType": "FULL",
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

def place_market_sell_base(session: requests.Session, symbol: str, qty: float, *, logs_cc=None) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": f"{qty:.8f}",
        "newOrderRespType": "FULL",
    }
    return api_signed_post(session, "/api/v3/order", params, logs_cc=logs_cc)

# -----------------------
# Rozhodování
# -----------------------
def run_decision_for_pair(session: requests.Session, state_cc, logs_cc,
                          pair: str, model: str, exf: Dict[str, Any],
                          price: float, sig_row: Dict[str, Any]) -> None:

    s_name = f"{pair}_{model}.json"
    st_txt = _read_blob_text(state_cc, s_name)
    st = json.loads(st_txt) if st_txt else {"position": "flat"}

    B = float(sig_row.get("B", 0.0))
    S = float(sig_row.get("S", 0.0))
    base = exf.get("base", "BASE")
    quote = exf.get("quote", "USDT")
    step = float(exf.get("stepSize", 0.0))
    min_qty = float(exf.get("minQty", 0.0))
    min_not = float(exf.get("minNotional", 0.0))

    logger.info("[%s/%s] price=%.8f B=%.8f pos=%s", pair, model, price, B, st.get("position"))

    # --- SELL (placeholder: podmínku doplň podle modelu)
    sell_condition = False  # ponech dle své strategie
    if st.get("position") == "long" and sell_condition:
        try:
            odr = place_market_sell_base(session, pair, float(st.get("qty", 0.0)), logs_cc=logs_cc)
        except Exception as e:
            logger.warning("[%s/%s] SELL order failed: %s", pair, model, e)
            return

        # parse výsledků
        fills = odr.get("fills", []) or []
        executed_qty = sum(float(f.get("qty", 0)) for f in fills) if fills else float(odr.get("executedQty", 0))
        cqq = sum(float(f.get("price", 0)) * float(f.get("qty", 0)) for f in fills) if fills else float(odr.get("cummulativeQuoteQty", 0))
        avgp = (cqq / executed_qty) if executed_qty > 0 else price
        fee_total = sum(float(f.get("commission", 0) or 0) for f in fills) if fills else 0.0
        fee_asset = (fills[0].get("commissionAsset") if fills else "")

        # 1) nejdřív stav
        st = {"position": "flat"}
        _write_blob_json(state_cc, s_name, st)

        # 2) pak CSV
        line = ",".join([
            _timestamp(), pair, model, "SELL",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cqq:.8f}",
            f"{fee_total:.8f}",
            fee_asset or "",
            str(odr.get("orderId", "")),
            f"{B:.8f}",
            f"{S:.8f}",
            str(sig_row.get("date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)

        logger.info("[%s/%s] SELL filled qty=%.8f @ %.8f (quote=%.8f %s)", pair, model, executed_qty, avgp, cqq, quote)
        return

    # --- BUY: pouze pokud nejsme v pozici a cena <= B
    if st.get("position") == "flat" and price <= B:
        # minNotional guard (pokud burza vyžaduje)
        if min_not and ORDER_QUOTE < min_not:
            logger.info("[%s/%s] BUY under minNotional (%.4f < %.4f) → skip", pair, model, ORDER_QUOTE, min_not)
            return

        # dostupný quote zůstatek
        free_q = _get_free_asset(session, logs_cc, quote)
        spend = min(ORDER_QUOTE, max(0.0, free_q - 0.01))
        if spend <= 0:
            logger.info("[%s/%s] No BUY: free %s = %.8f", pair, model, quote, free_q)
            return

        if SAFE_MODE:
            logger.info("[%s/%s] SAFE_MODE ON → would BUY for %.8f %s", pair, model, spend, quote)
            return

        try:
            odr = place_market_buy_quote(session, pair, spend, logs_cc=logs_cc)
        except Exception as e:
            logger.warning("[%s/%s] BUY order failed: %s", pair, model, e)
            return

        fills = odr.get("fills", []) or []
        executed_qty = sum(float(f.get("qty", 0)) for f in fills) if fills else float(odr.get("executedQty", 0))
        cqq = sum(float(f.get("price", 0)) * float(f.get("qty", 0)) for f in fills) if fills else float(odr.get("cummulativeQuoteQty", 0))
        avgp = (cqq / executed_qty) if executed_qty > 0 else price
        fee_total = sum(float(f.get("commission", 0) or 0) for f in fills) if fills else 0.0
        fee_asset = (fills[0].get("commissionAsset") if fills else "")

        # množství zkontroluj vůči minQty (hlavně u stepSize)
        est_qty = executed_qty
        if step > 0:
            est_qty = (int(est_qty / step)) * step
        if min_qty and est_qty < min_qty:
            logger.info("[%s/%s] BUY filled qty(%.8f) < minQty(%.8f) — check filters", pair, model, executed_qty, min_qty)

        # 1) nejdřív uložit stav (aby případný problém s CSV nesmetl state)
        st = {
            "position": "long",
            "qty": executed_qty,
            "entry_price": avgp,
            "entry_quote": cqq,
            "b_level": B,
            "s_level": S,
            "signal_date": sig_row.get("date"),
            "last_update": _timestamp(),
        }
        _write_blob_json(state_cc, s_name, st)

        # 2) pak log do CSV
        line = ",".join([
            _timestamp(), pair, model, "BUY",
            f"{executed_qty:.8f}",
            f"{avgp:.8f}",
            f"{cqq:.8f}",
            f"{fee_total:.8f}",
            fee_asset or "",
            str(odr.get("orderId", "")),
            f"{B:.8f}",
            f"{S:.8f}",
            str(sig_row.get("date") or "")
        ]) + "\n"
        _append_trade_line(logs_cc, _trade_log_blob_name(pair), line)

        logger.info("[%s/%s] BUY filled qty=%.8f @ %.8f (quote=%.8f %s)", pair, model, executed_qty, avgp, cqq, quote)
        return

    # žádná akce
    logger.info("[%s/%s] No action (pos=%s, px=%.8f, B=%.8f)", pair, model, st.get("position"), price, B)

# -----------------------
# Hlavní funkce (Timer)
# -----------------------
def main(mytimer: func.TimerRequest) -> None:
    if not API_KEY or not API_SECRET:
        logger.error("Missing API credentials (BINANCE_API_KEY/BINANCE_API_SECRET).")
        return

    state_cc, logs_cc, signals_cc = _blob_clients()
    for cc in (state_cc, logs_cc, signals_cc):
        _ensure_container(cc)

    session = requests.Session()
    _egress_ip_info()
    _sync_server_time(session, logs_cc=logs_cc)

    pairs_models = _parse_pairs_models(TRADE_PAIRS_MODELS)
    if not pairs_models:
        logger.warning("No pairs to process (TRADE_PAIRS_MODELS is empty).")
        return

    symbols = [p for p, _ in pairs_models]
    exinfo = _fetch_exchange_info(session, logs_cc, symbols)
    prices = _fetch_prices_bulk(session, logs_cc, symbols)
    sig_rows = _load_signals(signals_cc)

    for pair, model in pairs_models:
        if pair not in exinfo:
            logger.info("[%s/%s] Missing exchangeInfo or not TRADING → skip", pair, model)
            continue
        if pair not in prices:
            logger.info("[%s/%s] Missing price → skip", pair, model)
            continue
        sig = _select_signal(sig_rows, pair, model)
        if not sig:
            logger.info("[%s/%s] No active signal passing thresholds — skip", pair, model)
            continue
        run_decision_for_pair(session, state_cc, logs_cc, pair, model, exinfo[pair], prices[pair], sig)
