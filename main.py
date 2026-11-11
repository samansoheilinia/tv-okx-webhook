async def place_order(side, amount):

    path = "/api/v5/trade/order"

    body_dict = {
        "instId": INST_ID,
        "tdMode": "cross",
        "side": side,
        "ordType": "market",
        "sz": str(amount)
    }

    body = json.dumps(body_dict)

    timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00","Z")
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

    async with httpx.AsyncClient() as client:

        response = await client.post(
            "https://my.okx.com" + path,
            headers=headers,
            content=body
        )

        return response.text
