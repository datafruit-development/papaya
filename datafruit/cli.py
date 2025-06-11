import sys
from pathlib import Path

import typer
import re
import importlib.util

from rich.console import Console
from rich.prompt import Prompt

from datafruit import EXPORTED_DATABASES
from datafruit.diff import print_diffs



DEFAULT_FILE = """import datafruit as dft
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

dft.export([db])"""

Prompt.prompt_suffix = " › "

console = Console()
app = typer.Typer()

def load_schema_from_file(path: Path):
    """Dynamically load the schema file (e.g., main.py) and return exported databases."""
    module_name = "datafruit_schema"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Get the exported databases from the loaded module's datafruit instance
    if hasattr(module, 'dft') and hasattr(module.dft, 'EXPORTED_DATABASES'):
        return module.dft.EXPORTED_DATABASES

    # Fallback: try to find databases by looking for postgres_db instances
    databases = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if hasattr(attr, '__class__') and attr.__class__.__name__ == 'postgres_db':
            databases.append(attr)

    return databases

def datafruit_default_init(directory: str) -> bool:
    """Initialize a datafruit project in the given directory."""

    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / "dft.py"

    try:
        file_path.write_text(DEFAULT_FILE.strip() + "\n", encoding="utf-8")
        return True
    except Exception as e:
        console.print(f"[red]Error creating file: {e}[/red]")
        return False


def project_exists(directory: Path) -> bool:
    # TODO: this is implemented as a heuristic for now
    return any(file.suffix == '.py' for file in directory.iterdir() if file.is_file())

def is_valid_project_name(name: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_-]+$', name)) if name else False

def get_project_name() -> str:
    while True:
        console.print("[blue]?[/blue] What should we name your project [grey53](leave empty for current directory)[/grey53]")
        project_name = Prompt.ask("", default="", show_default=False)

        if project_name.strip() == "" or is_valid_project_name(project_name.strip()):
            return project_name.strip()

        console.print("[red]Invalid project name. Use only letters, numbers, underscores, and dashes.[/red]")


@app.command()
def init(name = None):
    """Initialize a new datafruit project."""

    target_dir = Path.cwd()
    if project_exists(target_dir):
        console.print("[bold yellow]Existing project found in current directory. Not initializing.[/bold yellow]")
        raise typer.Exit(1)

    console.print("[bold yellow]No project found in current directory. Creating a new project.[/bold yellow]")
    if name:
        project_name = name
    else:
        project_name = get_project_name()

    target_dir = target_dir / project_name

    success = datafruit_default_init(str(target_dir))

    if success:
        console.print(f"[bold green]✓ Successfully initialized datafruit project in {target_dir}[/bold green]")
    else:
        console.print("[red]✗ Failed to initialize datafruit project[/red]")
        raise typer.Exit(1)


@app.command()
def plan():
    """Plan a datafruit project."""
    console.print("[bold blue]Planning schema changes...[/bold blue]")

    schema_file = Path("dft.py")
    if not schema_file.exists():
        console.print("[red]No dft.py schema file found.[/red]")
        raise typer.Exit(1)

    # Load and execute the schema to get the exported databases
    exported_databases = load_schema_from_file(schema_file)
    console.print(f"Found {len(exported_databases)} exported database(s)")

    if not exported_databases:
        console.print("[red]No database exported. Did you forget to call dft.export()?[/red]")
        raise typer.Exit(1)

    for db in exported_databases:
        diffs = db.compare_local_to_online_schema()
        print_diffs(diffs)


@app.command()
def apply():
    """Apply a datafruit project."""
    console.print("[bold green]Applying a datafruit project...[/bold green]")

    schema_file = Path("dft.py")
    if not schema_file.exists():
        console.print("[red]No dft.py schema file found.[/red]")
        raise typer.Exit(1)

    # Load and execute the schema to get the exported databases
    exported_databases = load_schema_from_file(schema_file)
    console.print(f"Found {len(exported_databases)} exported database(s)")

    if not exported_databases:
        console.print("[red]No database exported. Did you forget to call dft.export()?[/red]")
        raise typer.Exit(1)

    for db in exported_databases:
        diffs = db.compare_local_to_online_schema()
        if diffs:
            sync_result = db.sync_local_to_online()
            if sync_result:
                console.print(f"[bold green]✓ Successfully applied changes to {db.connection_string}[/bold green]")
            else:
                console.print(f"[red]✗ Failed to apply changes to {db.connection_string}[/red]")
        else:
            console.print(f"[bold yellow]No changes needed for {db.connection_string}[/bold yellow]")
 