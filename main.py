from fastapi import FastAPI
from typing import Dict, Any
import os
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

BASE_URL = "https://my.okx.com"


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

    return base64.b64encode(
        mac.digest()
    ).decode()


async def test_okx_connection():

    print("\n========== OKX DEBUG ==========")

    print(
        "API_KEY_LENGTH =",
        len(API_KEY) if API_KEY else 0
    )

    print(
        "API_KEY_LAST4 =",
        API_KEY[-4:] if API_KEY else None
    )

    print(
        "PASSPHRASE_EXISTS =",
        PASSPHRASE is not None
    )

    print(
        "SECRET_EXISTS =",
        SECRET_KEY is not None
    )

    print(
        "FLAG =",
        FLAG
    )

    print(
        "INST_ID =",
        INST_ID
    )

    print(
        "BASE_URL =",
        BASE_URL
    )

    print("================================\n")

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

    async with httpx.AsyncClient(timeout=30) as client:

        response = await client.get(
            BASE_URL + path,
            headers=headers
        )

        print("\n========== OKX RESPONSE ==========")
        print(response.status_code)
        print(response.text)
        print("==================================\n")

        return response.text


@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    print("\nSIGNAL RECEIVED:")
    print(data)

    result = await test_okx_connection()

    return {
        "status": "test",
        "okx": result
    }
