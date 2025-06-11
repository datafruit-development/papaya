import datafruit as dft
import os
from sqlmodel import Field, SQLModel
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime
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

db = dft.postgres_db(
    os.getenv("PG_DB_URL") or "",
    [users, posts]
)

dft.export([db])
