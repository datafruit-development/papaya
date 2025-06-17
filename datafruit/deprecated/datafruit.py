import sqlalchemy
import sqlmodel
from sqlalchemy import MetaData, create_engine, Engine, text
from alembic.migration import MigrationContext
from alembic.autogenerate import produce_migrations, render_python_code, compare_metadata
from alembic.operations import Operations
from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel


class PostgresDB:
    def __init__(self, connection_string: str, tables: list[type[SQLModel]]):
        self.connection_string = connection_string
        self.tables = tables
        self.engine = self._create_engine()

    def _create_engine(self) -> Engine:
        try:
            return create_engine(self.connection_string)
        except sqlalchemy.exc.ArgumentError as e:
            raise ValueError(f"Invalid database connection string: {e}") from e

    def _connect(self):
        try:
            return self.engine.connect()
        except sqlalchemy.exc.OperationalError as e:
            raise ConnectionError(f"Could not connect to database: {e}") from e

    def validate_connection(self) -> bool:
        try:
            with self._connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def _get_local_metadata(self) -> MetaData:
        metadata = MetaData()
        for table in self.tables:
            table.__table__.to_metadata(metadata)
        return metadata

    def _produce_migrations(self):
        local_metadata = self._get_local_metadata()
        with self._connect() as conn:
            context = MigrationContext.configure(conn)
            return produce_migrations(context, local_metadata)

    def get_schema_diff(self):
        """Get schema differences between local models and database."""
        local_metadata = self._get_local_metadata()
        with self._connect() as conn:
            context = MigrationContext.configure(conn)
            return compare_metadata(context, local_metadata)

    def sync_schema(self) -> bool:
        try:
            if not self.validate_connection():
                return False

            migrations = self._produce_migrations()

            if not migrations.upgrade_ops.ops:
                return True

            with self._connect() as conn:
                with conn.begin():
                    context = MigrationContext.configure(conn)
                    migration_code = render_python_code(
                        migrations.upgrade_ops,
                        migration_context=context
                    )

                    clean_lines = []
                    for line in migration_code.split('\n'):
                        if line.strip().startswith(('import ', 'if True:')):
                            continue
                        clean_line = line.lstrip()
                        if clean_line:
                            clean_lines.append(clean_line)

                    final_code = "import sqlalchemy as sa\nimport sqlmodel\n" + '\n'.join(clean_lines)

                    op = Operations(context)
                    exec_globals = {
                        'op': op,
                        'sa': sqlalchemy,
                        'sqlmodel': sqlmodel,
                        '__builtins__': __builtins__
                    }

                    exec(final_code, exec_globals)
            return True

        except Exception as e:
            print(f"Migration error: {e}")
            return False

    def execute_sql(self, sql: str):
        try:
            with self._connect() as conn:
                with conn.begin():
                    return conn.execute(text(sql))
        except Exception as e:
            print(f"SQL execution error: {e}")
            return None

    def get_table_info(self, table_name: str = None):
        try:
            with self._connect() as conn:
                if table_name:
                    query = """
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_name = :table_name
                        ORDER BY ordinal_position
                    """
                    result = conn.execute(text(query), {"table_name": table_name})
                else:
                    query = """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                    """
                    result = conn.execute(text(query))

                return result.fetchall()
        except Exception as e:
            print(f"Error getting table info: {e}")
            return []


if __name__ == "__main__":
    import os
    from sqlmodel import Field
    from dotenv import load_dotenv
    load_dotenv()

    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(unique=True)
        email: str = Field(unique=True)
        full_name: Optional[str] = None
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)

    class Post(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        title: str = Field(index=True)
        content: str
        published: bool = Field(default=False)
        created_at: datetime = Field(default_factory=datetime.utcnow)
        updated_at: Optional[datetime] = None

    db = PostgresDB(os.getenv("PG_DB_URL"), [User, Post])

    print("Testing sync...")
    result = db.sync_schema()
    print(f"Sync result: {result}")
