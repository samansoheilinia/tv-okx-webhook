from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/webhook")
async def webhook(request: Request):

    data = await request.json()

    print(data)

    return {
        "status": "success",
        "data": data
    }
