from datafruit.database import Database, PostgresDB
from sqlmodel import SQLModel as Table, Field

EXPORTED_DATABASES = []

def export(dbs: list):
    global EXPORTED_DATABASES
    EXPORTED_DATABASES = dbs

__all__ = ["Database", "PostgresDB", "export", "EXPORTED_DATABASES", "Table", "Field"]
