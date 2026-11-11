from fastapi import FastAPI
from typing import Dict, Any
import os
import json
import hmac
import base64
import httpx
from hashlib import sha256
from datetime import datetime, timezone

app = FastAPI()

API_KEY = os.getenv("OKX_API_KEY")
SECRET_KEY = os.getenv("OKX_SECRET_KEY")
PASSPHRASE = os.getenv("OKX_PASSPHRASE")

FLAG = os.getenv("OKX_FLAG", "0")

INST_ID = os.getenv("OKX_INST_ID", "BTC-USDT-SWAP")


@app.get("/")
def home():
    return {"status": "running"}


def sign(timestamp, method, path, body=""):

    message = timestamp + method + path + body

    mac = hmac.new(
        SECRET_KEY.encode(),
        message.encode(),
        digestmod=sha256
    )

    return base64.b64encode(mac.digest()).decode()


async def test_okx_connection():

    print("========== DEBUG ==========")
    print("API_KEY =", API_KEY[:8] if API_KEY else None)
    print("PASSPHRASE =", PASSPHRASE)
    print("SECRET_EXISTS =", SECRET_KEY is not None)
    print("FLAG =", FLAG)
    print("INST_ID =", INST_ID)
    print("===========================")

    path = "/api/v5/account/balance"

    timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    signature = sign(
        timestamp,
        "GET",
        path,
        ""
    )

    headers = {
        "OK-ACCESS-KEY": API_KEY,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": PASSPHRASE,
        "Content-Type": "application/json"
    }

    if FLAG == "1":
        headers["x-simulated-trading"] = "1"

    async with httpx.AsyncClient() as client:

        r = await client.get(
            "https://www.okx.com" + path,
            headers=headers
        )

        print("OKX RESPONSE:")
        print(r.text)

        return r.text


@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    print("SIGNAL RECEIVED:")
    print(data)

    result = await test_okx_connection()

    return {
        "status": "test",
        "okx": result
    }
