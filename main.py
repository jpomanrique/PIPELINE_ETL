import os
from fastapi import FastAPI, Header, HTTPException
from pymongo import MongoClient

app = FastAPI()

# MONGODB_URI = os.getenv("MONGODB_URI")
ATLAS_URI = os.getenv("ATLAS_URI")

API_KEY = os.getenv("API_KEY")

# if not MONGODB_URI or not API_KEY:
#    raise RuntimeError("Variáveis de ambiente não configuradas")

# client = MongoClient(MONGODB_URI)

if not ATLAS_URI:
    raise RuntimeError("ATLAS_URI não configurada")

client = MongoClient(ATLAS_URI)

db = client["test"]
collection = db["users"]
collection.create_index("id", unique=True)

def check_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/users/bulk")
def bulk_users(users: list[dict], x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    from pymongo import UpdateOne

    ops = [
        UpdateOne(
            {"id": u["id"]},
            {"$setOnInsert": u},
            upsert=True
        )
        for u in users
    ]

    if ops:
        result = collection.bulk_write(ops, ordered=False)
        return {
            "inserted": len(result.upserted_ids),
            "total": len(users)
        }

    return {"inserted": 0, "total": 0}
