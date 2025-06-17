<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/datafruit-dev/datafruit/blob/main/docs/logodark.svg?raw=true">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/datafruit-dev/datafruit/blob/main/docs/logolight.svg?raw=true">
    <img alt="datafruit Logo" src="https://github.com/datafruit-dev/datafruit/blob/main/docs/logolight.svg?raw=true" width="300" style="display: block; margin: 0 auto;">
  </picture>
</p>


### 1. Install Datafruit

```bash
pip install datafruit
```

### 2. Define Your Data Models

In your Python code, import Datafruit. Declare models as classes:

```python
import datafruit as dft
from datafruit import Table, Field
import os
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime
load_dotenv()

class users(Table, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: str = Field(unique=True)
    full_name: Optional[str] = None
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class posts(Table, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(index=True)
    content: str
    published: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

db = dft.PostgresDB(
    os.getenv("PG_DB_URL") or "",
    [users, posts]
)

dft.export([db])
```

### 3. Inspect Schema Changes

Use the `plan` command to see pending migrations:

```bash
dft plan
```

This prints a diff of tables/columns to be added, removed, or modified.

### 4. Apply Migrations

When you’re ready, apply changes to your database:

```bash
dft apply
```

Under the hood, Datafruit generates and runs Alembic migration scripts for you.


You now have a working Datafruit setup: define models in code, preview changes, and apply migrations seamlessly. For more details, see the full documentation.

