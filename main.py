from fastapi import FastAPI
from typing import Dict, Any
import os
import json
import hmac
import base64
import httpx
import asyncio

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


async def place_order(
    inst_id,
    side,
    pos_side,
    size
):

    try:
        size = max(
            1,
            int(float(size))
        )
    except:
        size = 1

    path = "/api/v5/trade/order"

    body_dict = {
        "instId": inst_id,
        "tdMode": "isolated",
        "side": side,
        "posSide": pos_side,
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
        print("OKX RESPONSE")
        print(response.text)
        print("===================================")

        return response.text


async def execute_order(data):

    action = str(
        data.get("action", "")
    ).lower()

    amount = str(
        data.get("amount", "1")
    )

    inst_id = str(
        data.get("instId", "")
    )

    strategy = str(
        data.get("strategy", "")
    )

    print("SIGNAL RECEIVED:")
    print(strategy)
    print(data)

    if action == "buy":

        result = await place_order(
            inst_id,
            "buy",
            "long",
            amount
        )

        print(result)

    elif action == "sell":

        result = await place_order(
            inst_id,
            "sell",
            "short",
            amount
        )

        print(result)


@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    asyncio.create_task(
        execute_order(data)
    )

    return {
        "status": "received"
    }
