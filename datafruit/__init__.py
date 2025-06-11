from datafruit.datafruit import postgres_db

EXPORTED_DATABASES = []

def export(dbs: list):
    global EXPORTED_DATABASES
    EXPORTED_DATABASES = dbs

__all__ = ["postgres_db", "export", "EXPORTED_DATABASES"]
