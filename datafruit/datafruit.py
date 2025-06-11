<<<<<<< HEAD
from sqlalchemy import MetaData, create_engine, Engine
from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata
from typing import Optional
=======
import sqlalchemy
from sqlalchemy import inspect, MetaData, create_engine, Engine
from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata, produce_migrations, render_python_code
from alembic.operations import Operations
from alembic.operations.ops import MigrationScript
from typing import Optional, List, Dict, Any
>>>>>>> 3498c7754091298c5a2d69bc3602c31bb0c5f081
from datetime import datetime
from sqlmodel import SQLModel
from datafruit.diff import print_diffs, Diff

<<<<<<< HEAD
"""
---- OPEN ISSUES ----

1. Type compatibility checks are too basic and may lead to false positives/negatives.
2. Altering existing tables to match the model schema is not implemented in sync_local_to_online.
3. Does not handle foreign key relations - need to implement a way to create, check, and sync foreign keys.
"""

=======

>>>>>>> 3498c7754091298c5a2d69bc3602c31bb0c5f081
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
        Returns the online database schema as a MigrationContext object.
        """
        conn = self.engine.connect()
        mc = MigrationContext.configure(
            connection=conn,
            opts={
                "compare_type": True,
                "compare_server_default": True,
                "compare_comment": True,
                "target_metadata": self.get_local_metadata()
            }
        )
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

    def compare_local_to_online_schema(self):
        local_schema = self.get_local_metadata()
        online_schema = self.get_online_db_schema()
        migrations = compare_metadata(online_schema, local_schema)

        return migrations

    def produce_migrations(self) -> MigrationScript:
        """
        Generate migration scripts based on the differences between the local model and the online schema.
        """
        local_schema = self.get_local_metadata()
        online_schema = self.get_online_db_schema()
        migrations = produce_migrations(online_schema, local_schema)

        return migrations

    def sync_local_to_online(self):
        """
        Synchronize local SQLModel definitions with online database tables.
        """

        migrations = self.produce_migrations()
        conn = self.engine.connect()
        ctx = MigrationContext.configure(conn)
        upgrade_ops_code = "import sqlmodel\nif True:\n" + render_python_code(migrations.upgrade_ops, migration_context=ctx)
        #downgrade_ops_code = "import sqlmodel\nif True:\n" + render_python_code(migrations.downgrade_ops, migration_context=ctx)
        op = Operations(ctx)

        print(upgrade_ops_code)

        #print(downgrade_ops_code)
        exec(upgrade_ops_code, {'op': op, 'conn': conn, 'sa': sqlalchemy})
        #exec(downgrade_ops_code, {'op': op, 'conn': conn})

if __name__ == "__main__":
    from sqlmodel import SQLModel, Field, Relationship
    import os
    from dotenv import load_dotenv

    load_dotenv()

    class Habitat(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        location: str
        climate: str

    class Animal(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str
        type: str
        breed: str | None = None
        is_domestic: bool = False
        date: int

        # New foreign keys
        habitat_id: int = Field(foreign_key="habitat.id")
        caretaker_id: int = Field(foreign_key="user.id")


    # ✅ New table — tests add_table, add_index, add_fk, modify_nullable, modify_default
    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(index=True, unique=True, description="The user's unique username")
        email: str = Field(index=True)
        bio: Optional[str] = Field(default=None, description="Short bio")
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)

        posts: list["Post"] = Relationship(back_populates="author")

    # ✅ New table — tests FK, index
    class Post(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        title: str = Field(index=True)
        content: str
        published: bool = Field(default=False)
        views: int = Field(default=0)
        author_id: int = Field(foreign_key="user.id")

        author: Optional[User] = Relationship(back_populates="posts")

    # ✅ New table — tests multiple foreign keys, composite constraints
    class Comment(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        post_id: int = Field(foreign_key="post.id")
        author_id: int = Field(foreign_key="user.id")
        content: str
        created_at: datetime = Field(default_factory=datetime.utcnow)

        __table_args__ = (
            {"sqlite_autoincrement": True},  # table-level arg for constraint test
        )

    # ✅ Table to test deletion (remove_table) — must exist in DB but be removed from schema
    # To simulate removal, you'd comment this out later
    class Deprecated(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        notes: str

    db = postgres_db(
        os.getenv("PG_DB_URL") or "",
        [Animal, User, Post, Comment, Deprecated, Habitat ]
    )
<<<<<<< HEAD

    print_diffs(db.schema_diff())

def export(databases: list[postgres_db]):

    for db in databases:
        ...
        print(__name__)
=======

    print(db.sync_local_to_online())

>>>>>>> 3498c7754091298c5a2d69bc3602c31bb0c5f081
