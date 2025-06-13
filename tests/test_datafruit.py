import pytest
import pytest_postgresql
from datafruit.datafruit import PostgresDB
from sqlmodel import SQLModel, Field
from sqlalchemy import Engine, MetaData, create_engine, text
from sqlalchemy.exc import ArgumentError, OperationalError  # Fixed import
from typing import Optional
from datetime import datetime, timedelta
import json
import os
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open  # Fixed import
import datafruit.cli as cli_module

class TestUser(SQLModel, table=True):
    __tablename__ = "test_users"
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: str = Field(unique=True)
    full_name: Optional[str] = None
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class TestPost(SQLModel, table=True):
    __tablename__ = "test_posts"
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(index=True)
    content: str
    published: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

class TestProfile(SQLModel, table=True):
    __tablename__ = "test_profiles"
    id: Optional[int] = Field(default=None, primary_key=True)
    bio: str
    age: int
    salary: Optional[float] = None

@pytest.fixture
def postgresql_db_conn_str(postgresql):
    connection = f'postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'
    return connection

@pytest.fixture
def db_instance(postgresql_db_conn_str):
    return PostgresDB(postgresql_db_conn_str, [TestUser, TestPost, TestProfile])

@pytest.fixture
def db_with_tables(db_instance):
    """Create a database instance with tables already created"""
    db_instance.create_all_tables()
    return db_instance

@pytest.fixture
def empty_db_instance(postgresql_db_conn_str):
    """Create a database instance without any tables"""
    return PostgresDB(postgresql_db_conn_str, [])

@pytest.fixture
def temp_dir():
    """Create a temp dir for testing file operations"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)

@pytest.fixture
def mock_dft_dir(temp_dir):
    """mock the DFT_DIR to use temp directory"""
    with patch.object(cli_module, 'DFT_DIR', temp_dir / '.dft'):
        yield temp_dir / '.dft'
        
@pytest.fixture
def sample_schema_content():
    return '''import datafruit as dft
from sqlmodel import Field, SQLModel

class User(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str

db = dft.PostgresDB("postgresql://localhost/test", [User])
dft.export([db])'''


def test_init_creates_engine(postgresql_db_conn_str):
    db = PostgresDB(postgresql_db_conn_str, [TestUser])
    assert db.connection_string == postgresql_db_conn_str
    assert db.tables == [TestUser]
    assert db.engine is not None

def test_connect_returns_session(db_instance):
    engine = db_instance.get_engine()
    assert isinstance(engine, Engine)

def test_get_engine_returns_new_engine_instance(db_instance):
    """Test that get_engine() returns a new Engine instance each time"""
    engine1 = db_instance.get_engine()
    engine2 = db_instance.get_engine()

    assert isinstance(engine1, Engine)
    assert isinstance(engine2, Engine)
    # Should be different instances but same connection string
    assert engine1 is not engine2
    assert str(engine1.url) == str(engine2.url)

def test_get_local_metadata_returns_metadata_object(db_instance):
    """Test that get_local_metadata() returns a MetaData object"""
    metadata = db_instance.get_local_metadata()
    assert isinstance(metadata, MetaData)

def test_get_local_metadata_contains_specified_tables(db_instance):
    """Test that local metadata contains all tables specified in __init__"""
    metadata = db_instance.get_local_metadata()
    table_names = [table.name for table in metadata.tables.values()]

    expected_tables = ["test_users", "test_posts", "test_profiles"]
    for expected_table in expected_tables:
        assert expected_table in table_names

def test_get_local_metadata_empty_when_no_tables(empty_db_instance):
    """Test that get_local_metadata() returns empty metadata when no tables specified"""
    metadata = empty_db_instance.get_local_metadata()
    assert isinstance(metadata, MetaData)
    assert len(metadata.tables) == 0

# ADD these test classes to your existing test file:

class TestPostgresDBInit:
    """Test PostgresDB initialization edge cases"""
    
    def test_init_with_multiple_tables(self, postgresql_db_conn_str):
        tables = [TestUser, TestPost, TestProfile]
        db = PostgresDB(postgresql_db_conn_str, tables)
        assert db.tables == tables
        assert len(db.tables) == 3

    def test_init_with_invalid_connection_string(self):
        with pytest.raises(ValueError, match="Invalid database connection string"):
            PostgresDB("invalid://connection", [TestUser])

    def test_init_creates_engine_instance(self, postgresql_db_conn_str):
        db = PostgresDB(postgresql_db_conn_str, [TestUser])
        assert hasattr(db, 'engine')
        assert db.engine.url.drivername == 'postgresql+psycopg2'


class TestPostgresDBConnection:
    """Test PostgresDB connection methods"""
    
    def test_connect_successful(self, db_instance):
        connection = db_instance._connect()
        assert connection is not None
        connection.close()

    def test_validate_connection_successful(self, db_instance):
        assert db_instance.validate_connection() is True

    def test_validate_connection_failed(self):
        db = PostgresDB("postgresql://invalid:invalid@localhost:5432/nonexistent", [TestUser])
        assert db.validate_connection() is False

    @patch('datafruit.datafruit.create_engine')
    def test_connect_handles_operational_error(self, mock_create_engine):
        mock_engine = Mock()
        mock_engine.connect.side_effect = OperationalError("Connection failed", None, None)
        mock_create_engine.return_value = mock_engine
        
        db = PostgresDB("postgresql://test", [TestUser])
        with pytest.raises(ConnectionError):
            db._connect()


class TestPostgresDBSchemaDiff:
    """Test PostgresDB schema diff operations"""
    
    @patch('datafruit.datafruit.compare_metadata')
    def test_get_schema_diff_calls_compare_metadata(self, mock_compare, db_instance):
        mock_compare.return_value = []
        
        result = db_instance.get_schema_diff()
        
        mock_compare.assert_called_once()
        assert result == []

    def test_get_schema_diff_integration(self, db_instance):
        # This will return actual differences since tables don't exist yet
        diffs = db_instance.get_schema_diff()
        assert isinstance(diffs, list)
        # Should have differences since tables don't exist in the database yet


class TestPostgresDBSchemaSync:
    """Test PostgresDB schema synchronization"""
    
    @patch('datafruit.datafruit.PostgresDB.validate_connection')
    def test_sync_schema_fails_when_connection_invalid(self, mock_validate, db_instance):
        mock_validate.return_value = False
        
        result = db_instance.sync_schema()
        
        assert result is False
        mock_validate.assert_called_once()

    @patch('datafruit.datafruit.PostgresDB._produce_migrations')
    @patch('datafruit.datafruit.PostgresDB.validate_connection')
    def test_sync_schema_returns_true_when_no_migrations(self, mock_validate, mock_migrations, db_instance):
        mock_validate.return_value = True
        mock_migration_obj = Mock()
        mock_migration_obj.upgrade_ops.ops = []
        mock_migrations.return_value = mock_migration_obj
        
        result = db_instance.sync_schema()
        
        assert result is True


class TestPostgresDBSQLExecution:
    """Test PostgresDB SQL execution methods"""
    
    def test_execute_sql_successful(self, db_instance):
        result = db_instance.execute_sql("SELECT 1 as test_column")
        assert result is not None

    def test_execute_sql_with_invalid_sql(self, db_instance):
        result = db_instance.execute_sql("INVALID SQL STATEMENT")
        assert result is None

    def test_get_table_info_all_tables(self, db_instance):
        # First create some tables
        db_instance.execute_sql("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR(50))")
        
        result = db_instance.get_table_info()
        assert isinstance(result, list)

    def test_get_table_info_specific_table(self, db_instance):
        # Create a test table
        db_instance.execute_sql("CREATE TABLE IF NOT EXISTS specific_table (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL)")
        
        result = db_instance.get_table_info("specific_table")
        assert isinstance(result, list)
        if result:  # If table was created successfully
            assert len(result) >= 2  # Should have at least id and name columns

    def test_get_table_info_nonexistent_table(self, db_instance):
        result = db_instance.get_table_info("nonexistent_table")
        assert result == []

    @patch('datafruit.datafruit.PostgresDB._connect')
    def test_get_table_info_handles_exception(self, mock_connect, db_instance):
        mock_connect.side_effect = Exception("Connection failed")
        
        result = db_instance.get_table_info()
        
        assert result == []