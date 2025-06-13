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
        
        
# ADD these CLI test classes to your test file:

class TestCLIUtilityFunctions:
    """Test CLI utility functions"""
    
    def test_ensure_dft_dir_creates_directory(self, mock_dft_dir):
        assert not mock_dft_dir.exists()
        
        cli_module.ensure_dft_dir()
        
        assert mock_dft_dir.exists()
        assert mock_dft_dir.is_dir()

    def test_ensure_dft_dir_adds_to_gitignore(self, temp_dir, mock_dft_dir):
        gitignore = temp_dir / '.gitignore'
        gitignore.write_text("existing_content\n")
        
        with patch('datafruit.cli.Path.cwd', return_value=temp_dir):
            cli_module.ensure_dft_dir()
        
        content = gitignore.read_text()
        assert ".dft/" in content

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


class TestCLIDiffSerialization:
    """Test diff serialization functions"""
    
    def test_serialize_diff_add_table(self):
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_column1 = Mock()
        mock_column1.name = "col1"
        mock_column2 = Mock()
        mock_column2.name = "col2"
        mock_table.columns = [mock_column1, mock_column2]
        
        diff = ("add_table", mock_table)
        result = cli_module.serialize_diff(diff)
        
        expected = {
            "type": "add_table",
            "table_name": "test_table",
            "columns": ["col1", "col2"]
        }
        assert result == expected

    def test_serialize_diff_add_column(self):
        mock_column = Mock()
        mock_column.name = "new_column"
        mock_column.type = "VARCHAR(50)"
        
        diff = ("add_column", None, "test_table", mock_column)
        result = cli_module.serialize_diff(diff)
        
        expected = {
            "type": "add_column",
            "table_name": "test_table",
            "column_name": "new_column",
            "column_type": "VARCHAR(50)"
        }
        assert result == expected

    def test_serialize_diff_unknown_type(self):
        diff = ("unknown_diff_type", "some", "random", "data")
        result = cli_module.serialize_diff(diff)
        
        expected = {
            "type": "unknown_diff_type",
            "details": str(diff)
        }
        assert result == expected


class TestCLIPlanManagement:
    """Test plan save/load/clear functions"""
    
    def test_save_plan(self, mock_dft_dir):
        diffs_by_db = [
            ("postgresql://test1", [("add_table", Mock())]),
            ("postgresql://test2", [("remove_column", Mock(), "table", Mock())])
        ]
        schema_hash = "test_hash_value"
        
        with patch('datafruit.cli.serialize_diff') as mock_serialize:
            mock_serialize.return_value = {"type": "test", "details": "test"}
            cli_module.save_plan(diffs_by_db, schema_hash)
        
        plan_file = mock_dft_dir / "plan.json"
        assert plan_file.exists()
        
        plan_data = json.loads(plan_file.read_text())
        assert plan_data["schema_hash"] == schema_hash
        assert len(plan_data["databases"]) == 2

    def test_load_plan_existing_valid(self, mock_dft_dir):
        plan_data = {
            "timestamp": datetime.now().isoformat(),
            "schema_hash": "test_hash",
            "schema_file": "dft.py",
            "databases": [],
            "expiry_minutes": 10
        }
        
        plan_file = mock_dft_dir / "plan.json"
        mock_dft_dir.mkdir(parents=True, exist_ok=True)
        plan_file.write_text(json.dumps(plan_data))
        
        result = cli_module.load_plan()
        
        assert result is not None
        assert result["schema_hash"] == "test_hash"

    def test_load_plan_nonexistent(self, mock_dft_dir):
        result = cli_module.load_plan()
        assert result is None

    def test_load_plan_expired(self, mock_dft_dir):
        expired_time = datetime.now() - timedelta(minutes=15)
        plan_data = {
            "timestamp": expired_time.isoformat(),
            "schema_hash": "test_hash",
            "expiry_minutes": 10
        }
        
        plan_file = mock_dft_dir / "plan.json"
        mock_dft_dir.mkdir(parents=True, exist_ok=True)
        plan_file.write_text(json.dumps(plan_data))
        
        result = cli_module.load_plan()
        
        assert result is None
        assert not plan_file.exists()  # Should be deleted

    def test_clear_plan_existing(self, mock_dft_dir):
        plan_file = mock_dft_dir / "plan.json"
        mock_dft_dir.mkdir(parents=True, exist_ok=True)
        plan_file.write_text('{"test": "data"}')
        
        assert plan_file.exists()
        
        cli_module.clear_plan()
        
        assert not plan_file.exists()


class TestCLIProjectManagement:
    """Test project management functions"""
    
    def test_datafruit_default_init_success(self, temp_dir):
        target_dir = temp_dir / "new_project"
        
        result = cli_module.datafruit_default_init(str(target_dir))
        
        assert result is True
        assert target_dir.exists()
        dft_file = target_dir / "dft.py"
        assert dft_file.exists()
        content = dft_file.read_text()
        assert "import datafruit as dft" in content
        assert "class User(SQLModel, table=True):" in content

    def test_project_exists_with_python_files(self, temp_dir):
        (temp_dir / "test.py").write_text("# python file")
        
        result = cli_module.project_exists(temp_dir)
        
        assert result is True

    def test_project_exists_without_python_files(self, temp_dir):
        (temp_dir / "test.txt").write_text("not python")
        
        result = cli_module.project_exists(temp_dir)
        
        assert result is False

    def test_is_valid_project_name_valid_names(self):
        valid_names = ["project1", "my-project", "my_project", "Project123", "p"]
        for name in valid_names:
            assert cli_module.is_valid_project_name(name) is True

    def test_is_valid_project_name_invalid_names(self):
        invalid_names = ["", "project with spaces", "project@symbol", "project.dot", None]
        for name in invalid_names:
            assert cli_module.is_valid_project_name(name) is False


class TestCLICommands:
    """Test CLI command functions"""
    
    @patch('datafruit.cli.typer.Exit')
    @patch('datafruit.cli.console')
    def test_init_command_existing_project(self, mock_console, mock_exit, temp_dir):
        with patch('datafruit.cli.Path.cwd', return_value=temp_dir), \
             patch('datafruit.cli.project_exists', return_value=True):
            
            cli_module.init()
            
            mock_exit.assert_called_with(1)

    @patch('datafruit.cli.typer.Exit')
    @patch('datafruit.cli.console')
    def test_plan_command_no_schema_file(self, mock_console, mock_exit, temp_dir):
        with patch('datafruit.cli.Path', return_value=temp_dir / "nonexistent.py"):
            cli_module.plan()
            mock_exit.assert_called_with(1)

    @patch('datafruit.cli.console')
    def test_apply_command_no_plan(self, mock_console, temp_dir):
        schema_file = temp_dir / "dft.py"
        schema_file.write_text("# schema")
        
        with patch('datafruit.cli.Path', return_value=schema_file), \
             patch('datafruit.cli.load_plan', return_value=None), \
             patch('datafruit.cli.typer.Exit') as mock_exit:
            
            cli_module.apply()
            mock_exit.assert_called_with(1)
            