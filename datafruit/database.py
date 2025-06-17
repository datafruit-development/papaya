import sqlalchemy
import sqlmodel
from sqlalchemy import MetaData, create_engine, Engine, text
from alembic.migration import MigrationContext
from alembic.autogenerate import produce_migrations, render_python_code, compare_metadata
from alembic.operations import Operations
from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel
import sqlglot

class Database:
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

    def execute_sql(self, sql: str, args: Optional[dict] = None):
        try:
            with self._connect() as conn:
                with conn.begin():
                    return conn.execute(text(sql), args or {})
        except Exception as e:
            print(f"SQL execution error: {e}")
            return None

    def get_table_info(self, table_name: str = None, schema_name: str = 'public'):
        try:
            if table_name:
                # Get column information for specific table
                sql = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = :table_name AND table_schema = :schema_name
                    ORDER BY ordinal_position
                """
                args = {"table_name": table_name, "schema_name": schema_name}
            else:
                # Get all table names in schema
                sql = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                """
                args = {"schema_name": schema_name}
            
            # Use sqlglot to translate SQL for database-specific dialects
            engine_dialect = self.engine.dialect.name
            if engine_dialect != 'postgresql':
                try:
                    sql = sqlglot.transpile(sql, read="postgres", write=engine_dialect)[0]
                except Exception as e:
                    print(f"SQL translation warning: {e}")
            
            result = self.execute_sql(sql, args)
            return result.fetchall() if result else []
            
        except Exception as e:
            print(f"Error getting table info: {e}")
            return []

class PostgresDB(Database):
    def __init__(self, connection_string: str, tables: list[type[SQLModel]]):
        super().__init__(connection_string, tables)

    def get_version(self) -> Optional[str]:
        try:
            with self._connect() as conn:
                result = conn.execute(text("SELECT version()"))
                return result.scalar()
        except Exception as e:
            print(f"Error getting PostgreSQL version: {e}")
            return None
    
    def execute_sql(self, sql: str, args: Optional[dict] = None):
        sql = sqlglot.transpile(sql, write="hive")[0]
        return super().execute_sql(sql, args)