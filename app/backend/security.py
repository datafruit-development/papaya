import hashlib, secrets

def generate_api_key():
    return secrets.token_urlsafe(32)

def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()
