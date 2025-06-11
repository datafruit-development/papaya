from sqlalchemy import inspect, MetaData, create_engine, Engine
from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlmodel import SQLModel
from dataclasses import dataclass
from enum import Enum


"""
---- OPEN ISSUES ----

1. Type compatibility checks are too basic and may lead to false positives/negatives.
2. Altering existing tables to match the model schema is not implemented in sync_local_to_online.
3. Does not handle foreign key relations - need to implement a way to create, check, and sync foreign keys.
"""

class postgres_db:
    def __init__(self, connection_string: str, tables: list[type[SQLModel]]):
        self.connection_string = connection_string
        self.tables = tables
        self.engine = create_engine(self.connection_string)

    def connect(self) -> Engine:
        """
        returns a SQLAlchemy engine connected to the database.
        """
        return create_engine(self.connection_string)

    def get_online_db_schema(self) -> MigrationContext:
        """
        returns the online database schema as a MigrationContext object.
        """
        mc = MigrationContext.configure(self.engine.connect())
        return mc

    def create_table_from_model(self, model: type[SQLModel]):
        """
        Create a table from a SQLModel class.
        """
        model.metadata.create_all(self.engine, tables=[model.__table__])

    def get_local_metadata(self) -> MetaData:
        """
        Get metadata for the tables specified in self.tables
        """
        metadata = MetaData()
        for table in self.tables:
            # Add each table's metadata to our metadata object
            table.__table__.to_metadata(metadata)
        return metadata

    def produce_migrations(self) -> List[str]:
        """
        Generate migration scripts based on the differences between the local model and the online schema.
        """
        local_schema = self.get_local_metadata()
        online_schema = self.get_online_db_schema()
        migrations = compare_metadata(online_schema, local_schema)

        return migrations

    def sync_local_to_online(self):
        """
        Synchronize local SQLModel definitions with online database tables.
        """

if __name__ == "__main__":
    import os
    from sqlmodel import Field
    from dotenv import load_dotenv
    load_dotenv()

    class users(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(unique=True)
        email: str = Field(unique=True)
        full_name: Optional[str] = None
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)

    class posts(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        title: str = Field(index=True)
        content: str
        published: bool = Field(default=False)
        created_at: datetime = Field(default_factory=datetime.utcnow)
        updated_at: Optional[datetime] = None

    # Note: Connection string format for SQLAlchemy/SQLModel
    db = postgres_db(
        os.getenv("PG_DB_URL"),
        [users, posts]
    )

    print(db.produce_migrations())
