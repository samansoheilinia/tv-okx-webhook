from fastapi import FastAPI
from typing import Dict, Any

app = FastAPI()

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/webhook")
async def webhook(data: Dict[str, Any]):

    print(data)

    return {
        "status": "success",
        "received": data
    }
