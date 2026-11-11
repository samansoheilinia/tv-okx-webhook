from fastapi import FastAPI
from typing import Dict, Any
import os

app = FastAPI()

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    print("SIGNAL RECEIVED:")
    print(data)

    return {
        "status": "success",
        "received": data
    }
