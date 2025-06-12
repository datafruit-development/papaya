from datafruit.datafruit import PostgresDB

EXPORTED_DATABASES = []

def export(dbs: list):
    global EXPORTED_DATABASES
    EXPORTED_DATABASES = dbs

__all__ = ["PostgresDB", "export", "EXPORTED_DATABASES"]
