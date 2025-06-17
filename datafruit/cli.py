from pathlib import Path
import typer
import re
import importlib.util
from rich.console import Console
from rich.prompt import Prompt, Confirm
from datafruit.diff import print_diffs
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

DEFAULT_FILE = """import datafruit as dft
import os
from datafruit import Field, SQLModel
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime

load_dotenv()

class User(Table, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: str = Field(unique=True)
    full_name: Optional[str] = None
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Post(Table, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(index=True)
    content: str
    published: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

db = dft.PostgresDB(
    os.getenv("PG_DB_URL") or "",
    [User, Post]
)

dft.export([db])"""

Prompt.prompt_suffix = " › "
console = Console()
app = typer.Typer()

DFT_DIR = Path(".dft")
PLAN_FILE = DFT_DIR / "plan.json"
PLAN_EXPIRY_MINUTES = 10

def ensure_dft_dir():
    """Ensure the .dft directory exists."""
    DFT_DIR.mkdir(exist_ok=True)
    # Add .dft to .gitignore if it exists
    gitignore = Path(".gitignore")
    if gitignore.exists():
        content = gitignore.read_text()
        if ".dft/" not in content and ".dft\n" not in content:
            with gitignore.open("a") as f:
                f.write("\n.dft/\n")

def get_schema_hash(schema_file: Path) -> str:
    """Generate a hash of the schema file content."""
    content = schema_file.read_text(encoding="utf-8")
    return hashlib.sha256(content.encode()).hexdigest()

def serialize_diff(diff: Tuple[Any, ...]) -> Dict[str, Any]:
    """Convert a diff tuple to a JSON-serializable format."""
    kind = diff[0]

    # Extract key information based on diff type
    if kind in ["add_table", "remove_table"]:
        return {
            "type": kind,
            "table_name": diff[1].name,
            "columns": [col.name for col in diff[1].columns] if kind == "add_table" else []
        }
    elif kind in ["add_column", "remove_column"]:
        return {
            "type": kind,
            "table_name": diff[2],
            "column_name": diff[3].name,
            "column_type": str(diff[3].type)
        }
    elif kind in ["modify_nullable", "modify_type", "modify_default", "modify_comment"]:
        return {
            "type": kind,
            "table_name": diff[2],
            "column_name": diff[3],
            "old_value": str(diff[5]),
            "new_value": str(diff[6])
        }
    elif kind in ["add_index", "remove_index"]:
        return {
            "type": kind,
            "table_name": diff[1].table.name,
            "index_name": diff[1].name
        }
    elif kind in ["add_constraint", "remove_constraint", "add_fk", "remove_fk"]:
        return {
            "type": kind,
            "table_name": diff[1].table.name if hasattr(diff[1], 'table') else "unknown",
            "constraint_name": diff[1].name if hasattr(diff[1], 'name') else "unnamed"
        }
    else:
        return {
            "type": kind,
            "details": str(diff)
        }

def save_plan(diffs_by_db: List[Tuple[str, List[Any]]], schema_hash: str):
    """Save the plan to a file in the .dft directory."""
    ensure_dft_dir()

    # Serialize all diffs
    serialized_diffs_by_db = []
    for conn_str, diffs in diffs_by_db:
        serialized_diffs = [serialize_diff(diff) for diff in diffs]
        serialized_diffs_by_db.append({
            "connection_string": conn_str,
            "diffs": serialized_diffs
        })

    plan_data = {
        "timestamp": datetime.now().isoformat(),
        "schema_hash": schema_hash,
        "schema_file": "dft.py",
        "databases": serialized_diffs_by_db,
        "expiry_minutes": PLAN_EXPIRY_MINUTES
    }

    PLAN_FILE.write_text(json.dumps(plan_data, indent=2), encoding="utf-8")

def load_plan() -> Dict[str, Any] | None:
    """Load the saved plan if it exists and is valid."""
    if not PLAN_FILE.exists():
        return None

    try:
        plan_data = json.loads(PLAN_FILE.read_text(encoding="utf-8"))
        plan_time = datetime.fromisoformat(plan_data["timestamp"])

        # Check if plan is expired
        expiry_minutes = plan_data.get("expiry_minutes", PLAN_EXPIRY_MINUTES)
        if datetime.now() - plan_time > timedelta(minutes=expiry_minutes):
            PLAN_FILE.unlink()  # Delete expired plan
            return None

        return plan_data
    except Exception as e:
        console.print(f"[red]Error loading plan: {e}[/red]")
        return None

def clear_plan():
    """Remove the saved plan file."""
    if PLAN_FILE.exists():
        PLAN_FILE.unlink()

def load_schema_from_file(path: Path):
    """Dynamically load the schema file (e.g., dft.py) and return exported databases."""
    module_name = "datafruit_schema"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Get the exported databases from the loaded module's datafruit instance
    if hasattr(module, 'dft') and hasattr(module.dft, 'EXPORTED_DATABASES'):
        return module.dft.EXPORTED_DATABASES

    # Fallback: try to find databases by looking for PostgresDB instances
    databases = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if hasattr(attr, '__class__') and attr.__class__.__name__ == 'PostgresDB':
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
    """Plan schema changes for a datafruit project."""
    console.print("\n[bold blue]Planning schema changes...[/bold blue]")

    schema_file = Path("dft.py")
    if not schema_file.exists():
        console.print("[red]No dft.py schema file found.[/red]")
        raise typer.Exit(1)

    # Clear any existing plan when running a new plan
    clear_plan()

    # Get schema hash
    schema_hash = get_schema_hash(schema_file)

    # Load and execute the schema to get the exported databases
    exported_databases = load_schema_from_file(schema_file)
    console.print(f"Found {len(exported_databases)} exported database(s)")

    if not exported_databases:
        console.print("[red]No database exported. Did you forget to call dft.export()?[/red]")
        raise typer.Exit(1)

    # Collect diffs for each database
    diffs_by_db = []
    has_changes = False

    for db in exported_databases:
        diffs = db.get_schema_diff()
        if diffs:
            has_changes = True
            diffs_by_db.append((db.connection_string, diffs))
        print_diffs(diffs)

    # Always save the plan, even if there are no changes
    save_plan(diffs_by_db, schema_hash)

    if has_changes:
        console.print("\n[bold green]✓ Plan saved to .dft/plan.json[/bold green]")
        console.print(f"[dim]Plan expires in {PLAN_EXPIRY_MINUTES} minutes.[/dim]")
        console.print("[dim]Run 'dft apply' to apply these changes.[/dim]")
    else:
        console.print("\n[bold yellow]No changes detected.[/bold yellow]")
        console.print("[dim]Plan saved to .dft/plan.json (no changes needed)[/dim]")

@app.command()
def apply():
    """Apply schema changes for a datafruit project."""
    console.print("\n[bold green]Checking for saved plan...[/bold green]")

    schema_file = Path("dft.py")
    if not schema_file.exists():
        console.print("[red]No dft.py schema file found.[/red]")
        raise typer.Exit(1)

    # Check if a plan exists
    saved_plan = load_plan()
    if not saved_plan:
        console.print("[red]No valid plan found. Please run 'dft plan' first.[/red]")
        raise typer.Exit(1)

    # Verify schema hasn't changed since plan was created
    current_schema_hash = get_schema_hash(schema_file)
    if current_schema_hash != saved_plan["schema_hash"]:
        console.print("[red]Schema file (dft.py) has changed since the plan was created.[/red]")
        console.print("[red]Please run 'dft plan' again to create a new plan.[/red]")
        clear_plan()
        raise typer.Exit(1)

    # Check if the plan has any changes
    plan_has_changes = any(db_info["diffs"] for db_info in saved_plan.get("databases", []))

    if not plan_has_changes:
        console.print("[bold yellow]No changes to apply.[/bold yellow]")
        console.print("[dim]The saved plan indicates no schema changes are needed.[/dim]")
        clear_plan()
        return

    # Show plan metadata
    plan_time = datetime.fromisoformat(saved_plan["timestamp"])
    time_diff = datetime.now() - plan_time
    minutes_ago = int(time_diff.total_seconds() / 60)
    console.print(f"[dim]Using plan created {minutes_ago} minute(s) ago[/dim]")

    # Load and execute the schema to get the exported databases
    exported_databases = load_schema_from_file(schema_file)

    if not exported_databases:
        console.print("[red]No database exported. Did you forget to call dft.export()?[/red]")
        clear_plan()
        raise typer.Exit(1)

    # Show the plan again
    console.print("\n[bold yellow]The following changes will be applied:[/bold yellow]")

    for db in exported_databases:
        diffs = db.get_schema_diff()
        if diffs:
            print_diffs(diffs)

    # Ask for confirmation
    console.print("\n[bold yellow]⚠️  This action will modify your database schema.[/bold yellow]")
    confirmed = Confirm.ask("[bold]Do you want to apply these changes?[/bold]", default=False)

    if not confirmed:
        console.print("[yellow]Apply cancelled. Plan has been preserved.[/yellow]")
        console.print("[dim]Run 'dft apply' again when ready, or 'dft plan' to create a new plan.[/dim]")
        return

    # Apply the changes
    console.print("\n[bold green]Applying schema changes...[/bold green]")

    all_success = True
    for db in exported_databases:
        diffs = db.get_schema_diff()
        if diffs:
            success = db.sync_schema()
            if success:
                console.print(f"[bold green]✓ Successfully applied changes to[/bold green] [medium_purple]{db.connection_string}[/medium_purple]")
            else:
                console.print(f"[red]✗ Failed to apply changes to {db.connection_string}[/red]")
                all_success = False

    if all_success:
        # Clear the plan after successful apply
        clear_plan()
        console.print("\n[bold green]✓ All changes applied successfully![/bold green]")
    else:
        console.print("\n[red]Some changes failed to apply. Plan has been preserved for retry.[/red]")
        console.print("[dim]Fix the issues and run 'dft apply' again.[/dim]")
        raise typer.Exit(1)
