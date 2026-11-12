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


async def place_order(inst_id, side, size):

    try:
        
    size = max(
        0.01,
        round(float(size), 2)
    )
    except:
    size = 0.01

    path = "/api/v5/trade/order"

    body_dict = {
        "instId": inst_id,
        "tdMode": "isolated",
        "side": side,
        "ordType": "market",
        "sz": str(size)
    }

    body = json.dumps(body_dict)

    timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    signature = sign(
        timestamp,
        "POST",
        path,
        body
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

        response = await client.post(
            BASE_URL + path,
            headers=headers,
            content=body
        )

        print("===================================")
        print("SIGNAL EXECUTED")
        print(body_dict)
        print("OKX RESPONSE")
        print(response.text)
        print("===================================")

        return response.text


@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    print("SIGNAL RECEIVED:")
    print(data)

    action = str(
        data.get("action", "")
    ).lower()

    amount = str(
        data.get("amount", "1")
    )

    inst_id = str(
        data.get("instId", "")
    )

    if action not in ["buy", "sell"]:
        return {
            "status": "ignored"
        }

    result = await place_order(
        inst_id,
        action,
        amount
    )

    return {
        "status": "executed",
        "okx": result
    }
