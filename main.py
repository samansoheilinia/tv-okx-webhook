async def place_order(inst_id, side, size):

    path = "/api/v5/trade/order"

    pos_side = "long"

    if side == "sell":
        pos_side = "short"

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

    async with httpx.AsyncClient(timeout=30) as client:

        response = await client.post(
            BASE_URL + path,
            headers=headers,
            content=body
        )

        print(response.text)

        return response.text
