"""TradingView -> OKX target-position bridge, single symbol / isolated / net.

Read README_FA.md BEFORE deployment. This service submits real orders when
OKX_FLAG=0 (same default as the original). Never run two copies for one account.
"""
import asyncio
import base64
import contextlib
import fcntl
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import tempfile
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

log = logging.getLogger("okx_bridge")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
D = Decimal


def number(value):
    try:
        text = str(value)
        if len(text) > 80:
            raise ValueError()
        result = D(text)
        if (not result.is_finite() or result < 0 or result > D("1e12")
                or result.as_tuple().exponent < -18):
            raise ValueError()
        return result
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Expected a finite, non-negative quantity") from None


def floor_size(value, step):
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def direction(value):
    return (value > 0) - (value < 0)


def valid_symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", value):
        raise ValueError("Invalid instId")
    return value


def normalize(data, symbol):
    """Require target semantics; never fall back to amount=1 or amount=0.01."""
    if not isinstance(data, dict):
        raise ValueError("JSON object required")
    required = ("id", "action", "marketPosition", "prevMarketPosition", "amount",
                "targetPositionSize", "prevPositionSize", "instId", "time")
    if any(key not in data for key in required):
        raise ValueError("Missing fields: use the supplied alert.json")
    valid_symbol(data["instId"])
    if symbol and data["instId"] != symbol:
        raise ValueError("instId does not match OKX_INST_ID")
    sides = {"long": 1, "short": -1, "flat": 0}
    side, prev = data["marketPosition"], data["prevMarketPosition"]
    if side not in sides or prev not in sides or data["action"] not in ("buy", "sell"):
        raise ValueError("Invalid direction/action")
    size, previous = number(data["targetPositionSize"]), number(data["prevPositionSize"])
    amount = number(data["amount"])
    if (side == "flat") != (size == 0) or (prev == "flat") != (previous == 0):
        raise ValueError("Position direction and size disagree")
    if amount <= 0 or not isinstance(data["id"], str) or not data["id"]:
        raise ValueError("Invalid order id/amount")
    stamp = datetime.fromisoformat(str(data["time"]).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Timestamp must include timezone")
    ts = stamp.timestamp()
    if ts > time.time() + 10:
        raise ValueError("Signal timestamp is in the future; check clocks")
    target = size * sides[side]
    before = previous * sides[prev]
    delta = target - before
    if not delta or (delta > 0) != (data["action"] == "buy"):
        raise ValueError("Action contradicts the target position transition")
    # amount is audit information, NOT the requested new position size.
    result = {key: data[key] for key in required}
    for key, value in (("amount", amount), ("targetPositionSize", size),
                       ("prevPositionSize", previous)):
        result[key] = format(value, "f")
    result["ts"] = ts
    result["target"] = format(target, "f")
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":"))
    result["event"] = hashlib.sha256(canonical.encode()).hexdigest()
    return result


class Store:
    """All access occurs in one event loop, transactions contain no awaits."""
    def __init__(self, path, identity):
        self.db = sqlite3.connect(path, timeout=0.2)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, value TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, ts REAL, payload TEXT)")
        row = self.db.execute("SELECT value FROM state WHERE id=1").fetchone()
        self.state = json.loads(row[0]) if row else {
            "identity": identity, "version": 0, "done": 0, "signal": None,
            "close_required": False, "active": None, "status": "idle", "error": None,
        }
        if self.state["identity"] != identity:
            raise RuntimeError("Database belongs to different credentials/mode/symbol; do not reuse it")
        self.save()

    def save(self):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES (1, ?)",
                            (json.dumps(self.state),))

    def accept(self, signal):
        if self.db.execute("SELECT 1 FROM events WHERE id=?", (signal["event"],)).fetchone():
            return "duplicate"
        old = self.state["signal"]
        if old and old["instId"] != signal["instId"]:
            raise ValueError("This service handles one symbol; another symbol is already selected")
        if old and signal["ts"] < old["ts"]:
            return "ignored_older_signal"
        # Latest target wins, but an already requested full close is never erased.
        new = dict(self.state)
        new["version"] += 1
        new["signal"] = signal
        new["status"] = "queued"
        side, prev = signal["marketPosition"], signal["prevMarketPosition"]
        if side == "flat" or (prev != "flat" and side != prev):
            new["close_required"] = True
        with self.db:
            self.db.execute("INSERT INTO events VALUES (?, ?, ?)",
                            (signal["event"], signal["ts"], json.dumps(signal)))
            self.db.execute("INSERT OR REPLACE INTO state VALUES (1, ?)", (json.dumps(new),))
        self.state.clear()
        self.state.update(new)
        return "queued"


class ExchangeError(Exception):
    pass


class OKX:
    def __init__(self, client, key, secret, passphrase, flag, base, symbol, inst_type):
        self.client, self.key, self.secret = client, key, secret
        self.passphrase, self.flag, self.base = passphrase, flag, base
        self.symbol, self.inst_type = symbol, inst_type
        self.step = self.minimum = self.maximum = None

    async def call(self, method, path, payload=None, params=None):
        if params:
            path += "?" + urlencode(params)
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        signature = base64.b64encode(hmac.new(
            self.secret.encode(), (stamp + method + path + body).encode(), hashlib.sha256
        ).digest()).decode()
        headers = {"OK-ACCESS-KEY": self.key, "OK-ACCESS-SIGN": signature,
                   "OK-ACCESS-TIMESTAMP": stamp, "OK-ACCESS-PASSPHRASE": self.passphrase,
                   "Content-Type": "application/json"}
        if self.flag == "1":
            headers["x-simulated-trading"] = "1"
        if method == "POST" and path == "/api/v5/trade/order":
            headers["expTime"] = str(int((time.time() + 5) * 1000))
        start = time.monotonic()
        # No automatic POST retries: a lost response does not mean a lost order.
        response = await self.client.request(method, self.base + path, headers=headers, content=body)
        log.info("OKX_HTTP method=%s endpoint=%s status=%s elapsed_ms=%.0f", method,
                 path.split("?")[0], response.status_code, (time.monotonic() - start) * 1000)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ExchangeError("Invalid OKX response")
        return data

    async def get(self, path, **params):
        data = await self.call("GET", path, params=params)
        if data.get("code") != "0" or not isinstance(data.get("data"), list):
            raise ExchangeError(f"OKX read rejected: {data.get('code')} {data.get('msg')}")
        return data["data"]

    async def initialize(self):
        async def find_instruments():
            kinds = [self.inst_type] if self.inst_type else ["FUTURES", "SWAP"]
            async def read_kind(kind):
                data = await self.call("GET", "/api/v5/public/instruments",
                                       params={"instType": kind})
                if data.get("code") == "51001":
                    return []  # Exact symbol absent in this product category.
                if data.get("code") != "0" or not isinstance(data.get("data"), list):
                    raise ExchangeError(f"Instrument lookup failed: {data.get('code')} {data.get('msg')}")
                return [dict(row, instType=kind) for row in data["data"]
                        if row.get("instId") == self.symbol]
            batches = await asyncio.gather(*(read_kind(kind) for kind in kinds))
            return [row for batch in batches for row in batch]
        config, instruments = await asyncio.gather(
            self.get("/api/v5/account/config"), find_instruments(),
        )
        if not config or config[0].get("posMode") != "net_mode":
            raise ExchangeError("Only One-way/net_mode is supported; no account settings were changed")
        rows = [x for x in instruments if x.get("instId") == self.symbol]
        if len(rows) != 1:
            raise ExchangeError("Instrument was not found; check OKX_INST_ID and OKX_INST_TYPE")
        instrument = rows[0]
        self.inst_type = instrument["instType"]
        self.step, self.minimum = number(instrument["lotSz"]), number(instrument["minSz"])
        self.maximum = number(instrument["maxMktSz"])
        if min(self.step, self.minimum, self.maximum) <= 0:
            raise ExchangeError("Invalid instrument sizing rules")
        log.info("CONFIG_VERIFIED symbol=%s lotSz=%s minSz=%s maxMktSz=%s",
                 self.symbol, self.step, self.minimum, self.maximum)

    async def position(self):
        rows = await self.get("/api/v5/account/positions", instId=self.symbol)
        total = D("0")
        for row in rows:
            if row.get("instId") != self.symbol:
                continue
            size = D(row["pos"])
            if not size.is_finite():
                raise ExchangeError("Non-finite position")
            if size and (row.get("posSide") != "net" or row.get("mgnMode") != "isolated"):
                raise ExchangeError("Unexpected position mode: requires isolated/net")
            total += size
        return total

    async def pending(self):
        return await self.get("/api/v5/trade/orders-pending", instId=self.symbol)

    async def order(self, cid):
        rows = await self.get("/api/v5/trade/order", instId=self.symbol, clOrdId=cid)
        if len(rows) != 1:
            raise ExchangeError("Order not found; acceptance remains unknown")
        return rows[0]

    async def cancel(self, cid):
        data = await self.call("POST", "/api/v5/trade/cancel-order",
                               {"instId": self.symbol, "clOrdId": cid})
        log.warning("CANCEL_REQUEST clOrdId=%s code=%s", cid, data.get("code"))
        # Cancellation acknowledgment is not proof of cancellation. Poll the order.

    async def submit(self, active):
        return await self.call("POST", "/api/v5/trade/order", {
            "instId": self.symbol, "tdMode": "isolated", "posSide": "net",
            "side": active["side"], "ordType": "market", "sz": active["size"],
            "reduceOnly": active["reduce"], "clOrdId": active["cid"],
        })


class Engine:
    def __init__(self, store, exchange, max_entry_age=60):
        self.store, self.exchange = store, exchange
        self.max_entry_age = max_entry_age
        self.wake = asyncio.Event()
        self.ready = False
        self.last_tick = time.time()
        self.initialized_at = 0.0

    def status(self, name, error=None):
        state = self.store.state
        if state["status"] != name or state["error"] != error:
            state.update(status=name, error=error)
            self.store.save()
            log.log(logging.ERROR if error else logging.INFO, "%s %s", name.upper(), error or "")

    async def settle(self):
        state = self.store.state
        active = state["active"]
        order = await self.exchange.order(active["cid"])
        status = order.get("state")
        if status not in ("filled", "canceled", "mmp_canceled"):
            if status not in ("live", "partially_filled"):
                raise ExchangeError("Unknown order state; reconciliation required")
            # A market order should complete promptly. Cancel lingering orders and
            # obsolete entries, then verify terminal status before any new order.
            obsolete = active["version"] != state["version"] and not active["reduce"]
            if obsolete or time.time() - active["created"] > 5:
                if time.time() - active.get("cancel_at", 0) >= 1:
                    active["cancel_at"] = time.time()
                    self.store.save()
                    await self.exchange.cancel(active["cid"])
            self.status("waiting_order_terminal")
            return
        fill = number(order.get("accFillSz"))
        expected = D(active["before"]) + fill * (1 if active["side"] == "buy" else -1)
        actual = await self.exchange.position()
        if actual != expected:
            # Never blindly resend an entry on a stale position snapshot.
            self.status("waiting_position_confirmation",
                        f"Expected {expected}; observed {actual}; verify account if this persists")
            return
        log.info("ORDER_CONFIRMED clOrdId=%s state=%s filled=%s position=%s",
                 active["cid"], status, fill, actual)
        state["active"] = None
        state["error"] = None
        self.store.save()
        self.status("order_confirmed")

    async def submit(self, actual, size, side, reduce):
        state = self.store.state
        active = {"cid": "tv" + uuid.uuid4().hex[:30], "version": state["version"],
                  "before": str(actual), "size": str(size), "side": side,
                  "reduce": reduce, "created": time.time()}
        # Record BEFORE network I/O. Restart recovery requires optional persistent storage.
        state["active"] = active
        state["status"] = "submitting_close" if reduce else "submitting_entry"
        state["error"] = None
        self.store.save()
        log.info("ORDER_SUBMIT version=%s reduceOnly=%s side=%s size=%s clOrdId=%s",
                 active["version"], reduce, side, size, active["cid"])
        result = await self.exchange.submit(active)
        rows = result.get("data")
        row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
        if result.get("code") == "0" and row.get("sCode") == "0" and row.get("ordId"):
            self.status("order_accepted")
            return
        # Only explicit validation / trading-rule rejection is treated as definite.
        # Timeout/server errors and malformed replies stay UNKNOWN, never resubmitted.
        code = str(row.get("sCode") or result.get("code") or "")
        if len(code) == 5 and code.startswith(("510", "511")):
            state["active"] = None
            if not reduce:
                # Do not hammer insufficient funds / permissions failures.
                state["done"] = active["version"]
            self.initialized_at = 0.0
            self.store.save()
            raise ExchangeError(f"Order rejected {code}: {row.get('sMsg') or result.get('msg')}")
        raise ExchangeError(f"Order acceptance unknown (code={code}); checking clOrdId, NOT resending")

    async def tick(self):
        self.last_tick = time.time()
        state = self.store.state
        if not self.exchange.symbol and state["signal"]:
            self.exchange.symbol = state["signal"]["instId"]
        if not self.exchange.symbol:
            return  # No new environment variable required: symbol comes from JSON.
        # Resolve in-flight requests BEFORE refreshing metadata.
        if state["active"]:
            await self.settle()
            return
        if not self.ready or time.monotonic() - self.initialized_at > 600:
            await self.exchange.initialize()
            self.ready = True
            self.initialized_at = time.monotonic()
        if state["version"] == state["done"] and not state["close_required"]:
            return
        actual, pending = await asyncio.gather(self.exchange.position(), self.exchange.pending())
        if pending:
            raise ExchangeError("Untracked pending order exists on this symbol; resolve it first")
        # Reload after awaits: a new signal may have arrived while reading OKX.
        state = self.store.state
        signal = state["signal"]
        if signal is None:
            return
        raw = D(signal["target"])
        target = floor_size(abs(raw), self.exchange.step) * direction(raw)
        if actual and (raw == 0 or direction(actual) != direction(raw)):
            state["close_required"] = True
            self.store.save()
        if state["close_required"]:
            if actual:
                chunk = min(abs(actual), self.exchange.maximum)
                chunk = floor_size(chunk, self.exchange.step)
                if chunk <= 0:
                    raise ExchangeError("Close remainder below lotSz; requires exchange investigation")
                await self.submit(actual, chunk, "sell" if actual > 0 else "buy", True)
                return
            state["close_required"] = False
            self.store.save()
            log.info("FLAT_CONFIRMED version=%s", state["version"])
        if raw and (abs(target) < self.exchange.minimum):
            state["done"] = state["version"]
            self.status("entry_blocked_below_minimum", "Target is below minSz; no minimum-size substitution")
            self.store.save()
            return
        delta = target - actual
        if not delta:
            state["done"] = state["version"]
            self.status("target_reached")
            self.store.save()
            return
        reduce = bool(actual and direction(delta) != direction(actual))
        if not reduce and time.time() - signal["ts"] > self.max_entry_age:
            state["done"] = state["version"]
            self.status("entry_expired", "Exit was preserved; new exposure is too old")
            self.store.save()
            return
        size = floor_size(min(abs(delta), self.exchange.maximum), self.exchange.step)
        if size <= 0 or (not reduce and size < self.exchange.minimum):
            state["done"] = state["version"]
            self.status("untradeable_difference", "Difference below lotSz/minSz; no oversizing")
            self.store.save()
            return
        await self.submit(actual, size, "buy" if delta > 0 else "sell", reduce)

    async def run(self):
        failures = 0
        while True:
            self.wake.clear()
            try:
                await self.tick()
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                # No credentials/payload are logged. Keep close intention and order ID.
                self.status("retrying_or_reconciling", f"{type(exc).__name__}: {exc}")
            state = self.store.state
            busy = state["active"] or state["close_required"] or state["version"] != state["done"]
            fast_next = busy and state["status"] in ("order_accepted", "order_confirmed")
            delay = (min(4, 0.5 * 2 ** min(failures - 1, 3)) if failures
                     else (0 if fast_next else (0.25 if busy else 1)))
            # Error backoff is not bypassed by repeated incoming webhooks.
            if failures:
                await asyncio.sleep(delay)
            else:
                try:
                    await asyncio.wait_for(self.wake.wait(), delay)
                except asyncio.TimeoutError:
                    pass


@asynccontextmanager
async def lifespan(app):
    required = ("OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE")
    missing = [x for x in required if not os.getenv(x)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    webhook_secret = os.getenv("WEBHOOK_SECRET", "")
    if webhook_secret and len(webhook_secret) < 32:
        raise RuntimeError("Use a separate random WEBHOOK_SECRET of at least 32 characters")
    flag = os.getenv("OKX_FLAG", "0")
    kind = os.getenv("OKX_INST_TYPE", "")
    if flag not in ("0", "1") or kind not in ("", "FUTURES", "SWAP"):
        raise RuntimeError("Invalid OKX_FLAG / OKX_INST_TYPE")
    max_age = float(os.getenv("MAX_ENTRY_AGE_SECONDS", "60"))
    if not 1 <= max_age <= 3600:
        raise RuntimeError("MAX_ENTRY_AGE_SECONDS must be between 1 and 3600")
    base = os.getenv("OKX_BASE_URL", "https://my.okx.com").rstrip("/")
    if base not in ("https://my.okx.com", "https://www.okx.com", "https://app.okx.com"):
        raise RuntimeError("Unapproved OKX_BASE_URL")
    symbol = os.getenv("OKX_INST_ID", "")
    if symbol:
        valid_symbol(symbol)
    state_path = os.getenv("STATE_DB", "").strip()
    persistent = bool(state_path and state_path != ":memory:")
    if persistent:
        path = Path(state_path)
        if not path.is_absolute() or not path.parent.is_dir():
            raise RuntimeError("Optional STATE_DB is invalid: correct it or remove it for memory mode")
    identity = hashlib.sha256((os.environ["OKX_API_KEY"] + flag + base + symbol).encode()).hexdigest()
    # The default is RAM only. This lock file is NOT a database or a paid disk.
    lock_path = (state_path + ".lock" if persistent else
                 str(Path(tempfile.gettempdir()) / ("okx_bridge_" + identity[:24] + ".lock")))
    lock = open(lock_path, "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise RuntimeError("Another worker is running; use exactly one process") from None
    store = Store(state_path if persistent else ":memory:", identity)
    app.state.secret = webhook_secret
    app.state.symbol = symbol
    app.state.storage = "file" if persistent else "memory"
    if not persistent:
        log.warning("MEMORY_MODE: restart clears queued signals and order tracking")
    if not webhook_secret:
        log.warning("WEBHOOK_AUTH_DISABLED: original behavior retained; keep webhook URL private")
    # Reuse TCP/TLS connections. Separate connect/read timeouts from webhook ACK.
    async with httpx.AsyncClient(timeout=httpx.Timeout(4, connect=2, pool=1),
                                 limits=httpx.Limits(max_connections=4, max_keepalive_connections=4,
                                                    keepalive_expiry=60)) as client:
        app.state.client = client
        exchange = OKX(client, os.environ["OKX_API_KEY"], os.environ["OKX_SECRET_KEY"],
                       os.environ["OKX_PASSPHRASE"], flag, base, app.state.symbol, kind)
        engine = Engine(store, exchange, max_age)
        app.state.engine = engine
        task = asyncio.create_task(engine.run())
        app.state.task = task
        log.info("BRIDGE_STARTED symbol=%s flag=%s max_entry_age=%s", app.state.symbol, flag, max_age)
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            store.db.close()
            lock.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def home():
    # Liveness only: an OKX outage must not cause a restart loop.
    task = app.state.task
    if task.done():
        raise HTTPException(503, "Worker stopped")
    return {"status": "running", "worker": "alive", "storage": app.state.storage}


@app.get("/status")
async def status(x_webhook_secret: str = Header(default="")):
    if not app.state.secret or not hmac.compare_digest(x_webhook_secret.encode(), app.state.secret.encode()):
        raise HTTPException(401, "Unauthorized")
    engine = app.state.engine
    return {"ready": engine.ready, "last_tick": engine.last_tick, **engine.store.state}


@app.post("/webhook", status_code=202, openapi_extra={
    "requestBody": {"required": True, "content": {"application/json": {
        "schema": {"type": "object"}, "example": {"test": True}
    }}}
})
async def webhook(request: Request):
    # No exchange I/O: acknowledge after recording (RAM by default, disk optional).
    start = time.monotonic()
    body = await request.body()
    if len(body) > 16384:
        raise HTTPException(413, "Payload too large")
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(400, "Invalid JSON") from None
    if not isinstance(data, dict):
        raise HTTPException(400, "JSON object required")
    if app.state.secret:
        supplied = data.get("secret", "")
        if not isinstance(supplied, str) or not hmac.compare_digest(
                supplied.encode(), app.state.secret.encode()):
            raise HTTPException(401, "Invalid webhook secret")
    if data.get("test") is True:
        log.info("TEST_SIGNAL_RECEIVED: no order was queued or submitted")
        return {"status": "test_received", "order_sent": False}
    try:
        signal = normalize(data, app.state.symbol)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from None
    engine = app.state.engine
    if app.state.task.done():
        raise HTTPException(503, "Worker stopped")
    try:
        result = engine.store.accept(signal)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    except sqlite3.Error:
        raise HTTPException(503, "Signal was NOT recorded; state storage unavailable") from None
    engine.wake.set()
    log.info("SIGNAL_%s event=%s target=%s ack_ms=%.1f signal_age_ms=%.0f",
             result.upper(), signal["event"][:12], signal["target"],
             (time.monotonic() - start) * 1000, (time.time() - signal["ts"]) * 1000)
    return {"status": result, "event": signal["event"], "execution": "asynchronous"}


@app.get("/check")
async def check(instId: str = "", x_webhook_secret: str = Header(default="")):
    """Read-only connection check. NEVER submits or queues an order."""
    if app.state.secret and not hmac.compare_digest(
            x_webhook_secret.encode(), app.state.secret.encode()):
        raise HTTPException(401, "Invalid webhook secret")
    try:
        symbol = valid_symbol(instId or app.state.symbol)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    source = app.state.engine.exchange
    probe = OKX(app.state.client, source.key, source.secret, source.passphrase,
                source.flag, source.base, symbol, os.getenv("OKX_INST_TYPE", ""))
    try:
        await probe.initialize()
        # Verify private position-read permission too; never expose account sizes.
        await probe.position()
    except (ExchangeError, httpx.HTTPError, ValueError, KeyError) as exc:
        log.error("CONNECTION_CHECK_FAILED %s", exc)
        raise HTTPException(502, "Read-only OKX check failed; inspect Render logs") from None
    return {"status": "ok", "order_sent": False, "instId": symbol,
            "instType": probe.inst_type, "positionMode": "net_mode",
            "lotSz": str(probe.step), "minSz": str(probe.minimum)}
