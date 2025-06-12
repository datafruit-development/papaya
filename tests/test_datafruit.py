import pytest
import pytest_postgresql
from datafruit.datafruit import PostgresDB
from sqlmodel import SQLModel, Field
from sqlalchemy import Engine, MetaData
from typing import Optional
from datetime import datetime

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
