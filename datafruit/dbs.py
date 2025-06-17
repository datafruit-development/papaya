from pydantic import BaseModel, Field

INT = "INT"
BIGINT = "BIGINT"
SMALLINT = "SMALLINT"
DECIMAL = "DECIMAL"
NUMERIC = "NUMERIC"
FLOAT = "FLOAT"
DOUBLE = "DOUBLE"
CHAR = "CHAR"
VARCHAR = "VARCHAR"
TEXT = "TEXT"
DATE = "DATE"
TIME = "TIME"
TIMESTAMP = "TIMESTAMP"


def convert_to_sqlalchemy_table(**data):

    

class Table(BaseModel):
    def __init__(self, **data):
        super().__init__(**data) 
        self.table = 

class sqlDB:
    def __init__(self, db_url: str, tables: list[Table]):
        self.db_url = db_url
        self.tables = tables or []
        self.engine = None

    def connect(self):
        from sqlalchemy import create_engine
        self.engine = create_engine(self.db_url)

    def create_table_from_model(self, model):
        from sqlmodel import SQLModel
        SQLModel.metadata.create_all(self.engine, tables=[model.__table__])

    def close(self):
        if self.engine:
            self.engine.dispose()