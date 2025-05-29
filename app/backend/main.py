from fastapi import FastAPI, Depends
from auth import verify_api_key, verify_supabase_user

app = FastAPI()

@app.get("/v1/data")
def get_data(user=Depends(verify_api_key)):
    return {"message": f"Hello API user {user['user_id']}"}

@app.get("/dashboard/my-keys")
def get_my_keys(user=Depends(verify_supabase_user)):
    return {"message": f"Hello frontend user {user['id']}", "keys": []}  # Replace with actual key fetch
