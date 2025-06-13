import pytest
import pytest_postgresql  # Required for postgresql fixture
from datafruit.datafruit import PostgresDB  
from sqlmodel import SQLModel, Field
from sqlalchemy import Engine, MetaData, create_engine, text
from sqlalchemy.exc import ArgumentError, OperationalError
from typing import Optional
from datetime import datetime, timedelta
import json
import os
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
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

# Basic tests
def test_init_creates_engine(postgresql_db_conn_str):
    db = PostgresDB(postgresql_db_conn_str, [TestUser])
    assert db.connection_string == postgresql_db_conn_str
    assert db.tables == [TestUser]
    assert db.engine is not None

def test_get_local_metadata_returns_metadata_object(db_instance):
    """Test that _get_local_metadata() returns a MetaData object"""
    metadata = db_instance._get_local_metadata()  
    assert isinstance(metadata, MetaData)

def test_get_local_metadata_contains_specified_tables(db_instance):
    """Test that local metadata contains all tables specified in __init__"""
    metadata = db_instance._get_local_metadata()  
    table_names = [table.name for table in metadata.tables.values()]

    expected_tables = ["test_users", "test_posts", "test_profiles"]
    for expected_table in expected_tables:
        assert expected_table in table_names

def test_get_local_metadata_empty_when_no_tables(empty_db_instance):
    """Test that _get_local_metadata() returns empty metadata when no tables specified"""
    metadata = empty_db_instance._get_local_metadata()  
    assert isinstance(metadata, MetaData)
    assert len(metadata.tables) == 0

# Connection tests
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

# Schema diff tests
class TestPostgresDBSchemaDiff:
    """Test PostgresDB schema diff operations"""
    
    @patch('datafruit.datafruit.compare_metadata')
    def test_get_schema_diff_calls_compare_metadata(self, mock_compare, db_instance):
        mock_compare.return_value = []
        
        result = db_instance.get_schema_diff()
        
        mock_compare.assert_called_once()
        assert result == []

    def test_get_schema_diff_integration(self, db_instance):
        diffs = db_instance.get_schema_diff()
        assert isinstance(diffs, list)

# SQL execution tests
class TestPostgresDBSQLExecution:
    """Test PostgresDB SQL execution methods"""
    
    def test_execute_sql_successful(self, db_instance):
        result = db_instance.execute_sql("SELECT 1 as test_column")
        assert result is not None

    def test_execute_sql_with_invalid_sql(self, db_instance):
        result = db_instance.execute_sql("INVALID SQL STATEMENT")
        assert result is None

    def test_get_table_info_all_tables(self, db_instance):
        result = db_instance.get_table_info()
        assert isinstance(result, list)

    def test_get_table_info_specific_table(self, db_instance):
        db_instance.execute_sql("CREATE TABLE IF NOT EXISTS specific_table (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL)")
        
        result = db_instance.get_table_info("specific_table")
        assert isinstance(result, list)

    def test_get_table_info_nonexistent_table(self, db_instance):
        result = db_instance.get_table_info("nonexistent_table")
        assert result == []

# CLI tests
class TestCLIUtilityFunctions:
    """Test CLI utility functions"""
    
    def test_get_schema_hash(self, temp_dir):
        schema_file = temp_dir / "test_schema.py"
        content = "test content for hashing"
        schema_file.write_text(content)
        
        hash_result = cli_module.get_schema_hash(schema_file)
        
        expected_hash = hashlib.sha256(content.encode()).hexdigest()
        assert hash_result == expected_hash

    def test_get_schema_hash_different_content(self, temp_dir):
        schema_file1 = temp_dir / "schema1.py"
        schema_file2 = temp_dir / "schema2.py"
        
        schema_file1.write_text("content 1")
        schema_file2.write_text("content 2")
        
        hash1 = cli_module.get_schema_hash(schema_file1)
        hash2 = cli_module.get_schema_hash(schema_file2)
        
        assert hash1 != hash2

# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])