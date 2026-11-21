            yield
        finally:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            for engine in engines.values():
                if engine.store is not store:
                    engine.store.db.close()
            store.db.close()
            lock.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def home():
    # Liveness only: an OKX outage must not cause a restart loop.
    if any(task.done() for task in app.state.tasks.values()):
        raise HTTPException(503, "Worker stopped")
    return {"status": "running", "worker": "alive", "mode": "multi_symbol", "storage": app.state.storage}


@app.get("/status")
async def status(x_webhook_secret: str = Header(default=""), instId: str = ""):
    if not app.state.secret or not hmac.compare_digest(x_webhook_secret.encode(), app.state.secret.encode()):
        raise HTTPException(401, "Unauthorized")
    engine = app.state.engines.get(instId) if instId else app.state.engine
    if engine is None:
        raise HTTPException(422, "Unknown instId")
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
        if isinstance(data, dict) and isinstance(data.get("qtyMultiplier"), float):
            # Preserve the exact JSON decimal without changing legacy event hashes.
            data["qtyMultiplier"] = json.loads(body, parse_float=str)["qtyMultiplier"]
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
        signal = normalize(data, "")
        if signal["instId"] not in app.state.engines:
            raise ValueError("instId is not in the configured symbol list")
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        log.warning("VALIDATION_FAILED reason=%s", exc)
        raise HTTPException(422, str(exc)) from None
    engine = app.state.engines[signal["instId"]]
    if app.state.tasks[signal["instId"]].done():
        raise HTTPException(503, "Worker stopped")
    try:
        result = engine.store.accept(signal)
    except ValueError as exc:
        log.warning("VALIDATION_FAILED reason=%s", exc)
        raise HTTPException(422, str(exc)) from None
    except sqlite3.Error:
        raise HTTPException(503, "Signal was NOT recorded; state storage unavailable") from None
    engine.wake.set()
    log.info("GATE symbol=%s decision=%s bar=%s interval=%s source_before=%s:%s source_after=%s:%s execution_target=%s",
             signal["instId"], result, signal.get("barTime", "missing"),
             signal.get("interval", "missing"), signal["prevMarketPosition"],
             signal["prevPositionSize"], signal["marketPosition"], signal["targetPositionSize"],
             engine.store.state["signal"]["target"])
    log.info("SIGNAL_%s event=%s target=%s ack_ms=%.1f signal_age_ms=%.0f",
             result.upper(), signal["event"][:12], engine.store.state["signal"]["target"],
             (time.monotonic() - start) * 1000, (time.time() - signal["ts"]) * 1000)
    return {"status": result, "event": signal["event"], "execution": "asynchronous",
            "gate_reason": engine.store.state["gate_reason"],
            "executionTarget": engine.store.state["signal"]["target"]}


@app.get("/check")
async def check(instId: str = "", x_webhook_secret: str = Header(default="")):
    """Read-only connection check. NEVER submits or queues an order."""
    if app.state.secret and not hmac.compare_digest(
            x_webhook_secret.encode(), app.state.secret.encode()):
        raise HTTPException(401, "Invalid webhook secret")
    try:
        symbol = valid_symbol(instId or app.state.symbol)
    except ValueError as exc:
        log.warning("VALIDATION_FAILED reason=%s", exc)
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
