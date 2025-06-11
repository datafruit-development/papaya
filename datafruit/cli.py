import typer
import re
from pathlib import Path
from typing import Optional

from sqlmodel import Field, SQLModel
from rich.console import Console
from rich.prompt import Prompt

from diff import print_diffs

DEFAULT_FILE = """import datafruit as dft
from sqlmodel import SQLModel
import os

class Animal(SQLModel, table=True):
    id: int
    name: str
    type: str
    breed: str | None
    is_domestic: bool = False

tables = [
    Animal,
]

db = dft.postgre_db(os.environ.get("postgres_url"), tables)

dft.export([db])
"""

Prompt.prompt_suffix = " › "

console = Console()
app = typer.Typer()

def datafruit_default_init(directory: str) -> bool:
    """Initialize a datafruit project in the given directory."""

    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / "main.py"

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
def init():
    """Initialize a new datafruit project."""

    target_dir = Path.cwd()
    if project_exists(target_dir):
        console.print("[bold yellow]Existing project found in current directory. Not initializing.[/bold yellow]")
        raise typer.Exit(1)

    console.print("[bold yellow]No project found in current directory. Creating a new project.[/bold yellow]")
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

    print_diffs()

@app.command()
def apply():
    """Apply a datafruit project."""
    console.print("[bold green]Applying a datafruit project...[/bold green]")
