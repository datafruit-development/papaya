import pytest
import pytest_postgresql 
from datafruit.datafruit import postgres_db
from sqlmodel import SQLModel, Field, Session
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
    return postgresql.info.dsn()

@pytest.fixture
def db_instance(postgresql_db_conn_str):
    return postgres_db(postgresql_db_conn_str, [TestUser, TestPost, TestProfile])

@pytest.fixture
def db_with_tables(db_instance):
    """Create a database instance with tables already created"""
    db_instance.create_all_tables()
    return db_instance

def test_init_creates_engine(postgresql_db_conn_str):
    db = postgres_db(postgresql_db_conn_str, [TestUser])
    assert db.connection_string == postgresql_db_conn_str
    assert db.tables == [TestUser]
    assert db.engine is not None

def test_connect_returns_session(db_instance):
    session = db_instance.connect()
    assert isinstance(session, Session)
    session.close()

