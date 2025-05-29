from fastapi import Request, HTTPException
from supabase import create_client
from security import hash_key
import os
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

async def verify_api_key(request: Request):
    key = request.headers.get("x-api-key")
    if not key:
        raise HTTPException(status_code=401, detail="Missing API key")
    hashed = hash_key(key)
    res = supabase.table("api_keys").select("*").eq("key", hashed).eq("is_active", True).execute()
    if not res.data:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return res.data[0]

async def verify_supabase_user(request: Request):
    # You can implement JWT checking or forward cookies
    raise HTTPException(status_code=501, detail="Supabase auth not implemented yet")
