from collections import defaultdict
from rich.console import Console
from sqlalchemy.schema import Table
from typing import Any, List, Tuple, Union

console = Console()
Diff = Union[Tuple[Any, ...], List[Tuple[Any, ...]]]

def get_table_name(diff: Tuple[Any, ...]) -> str:
    kind = diff[0]
    match kind:
        case "add_table" | "remove_table" | "add_table_comment" | "remove_table_comment":
            return diff[1].name
        case "add_column" | "remove_column" | "modify_nullable" | "modify_type" | "modify_default" | "modify_comment":
            return diff[2]
        case "add_index" | "remove_index" | "add_constraint" | "remove_constraint" | "add_fk" | "remove_fk":
            return diff[1].table.name
        case "execute":
            return "__EXECUTE__"
        case _:
            return "__UNKNOWN__"

# When the code later prints column additions,
# it can check this map to see if any indexes are
# being created on that same column. This allows
# it to display more informative output
def build_index_map(diffs: List[Diff]) -> dict:
    index_map = {}
    for diff in diffs:
        if isinstance(diff, tuple) and diff[0] == "add_index":
            idx = diff[1]
            for expr in idx.expressions:
                if hasattr(expr, "name"):
                    index_map.setdefault((idx.table.name, expr.name), []).append(idx)
    return index_map

# Prints all the diffs so the changes appear under the same table
def group_diffs_by_table(diffs: List[Diff]) -> dict:
    table_diffs = defaultdict(list)
    for diff in diffs:
        if isinstance(diff, list):
            for sub in diff:
                table_diffs[get_table_name(sub)].append(sub)
        else:
            table_diffs[get_table_name(diff)].append(diff)
    return table_diffs

def get_table_prefix(diffs: List[Tuple[Any, ...]]) -> str:
    for diff in diffs:
        if diff[0] == "add_table":
            return "[green]+[/green]"
        if diff[0] == "remove_table":
            return "[red]-[/red]"
    return "[yellow]~[/yellow]"

def print_table_creation(table: Table, index_map: dict, claimed_indexes: set):
    for col in table.columns:
        console.print(f"[bold grey53]│[/bold grey53] [green]+ Add[/green] column [medium_purple]{col.name}[/medium_purple] [grey53]({col.type})[/grey53]")
        for idx in index_map.get((table.name, col.name), []):
            if idx.name not in claimed_indexes:
                console.print(f"[bold grey53]│   [/bold grey53]↳ with index [cyan]{idx.name}[/cyan]")
                claimed_indexes.add(idx.name)

def print_column_modification(diff: Tuple[Any, ...]):
    _, _, _, col, _, old, new = diff
    kind = diff[0]
    console.print(f"[bold grey53]│[/bold grey53] [yellow]~ Modify[/yellow] column [medium_purple]{col}[/medium_purple]")

    labels = {
        "modify_nullable": "Nullability",
        "modify_type": "Type",
        "modify_default": "Default",
        "modify_comment": "Comment"
    }

    label = labels[kind]
    old_val = repr(old) if kind == "modify_comment" else old
    new_val = repr(new) if kind == "modify_comment" else new
    console.print(f"[bold grey53]│   [/bold grey53][dim]-[/dim] {label} changed: {old_val} → {new_val}")

def print_constraint(diff: Tuple[Any, ...]):
    constraint = diff[1]
    kind = diff[0]

    actions = {
        "add_constraint": ("[green]+ Add[/green]", "constraint"),
        "remove_constraint": ("[red]- Remove[/red]", "constraint"),
        "add_fk": ("[green]+ Add[/green]", "foreign key"),
        "remove_fk": ("[red]- Remove[/red]", "foreign key"),
    }

    action, type_name = actions[kind]
    label = constraint.name or f"(unnamed on {', '.join(c.name for c in constraint.columns)})"
    console.print(f"[bold grey53]│[/bold grey53] {action} {type_name} [medium_purple]{label}[/medium_purple]")

def print_diff_item(diff: Tuple[Any, ...], index_map: dict, claimed_indexes: set, table_name: str):
    kind = diff[0]

    match kind:
        case "add_table":
            print_table_creation(diff[1], index_map, claimed_indexes)

        case "remove_table":
            console.print(f"[bold grey53]│[/bold grey53] [red]- Drop[/red] table [blue]{diff[1].name}[/blue]")

        case "add_column":
            col = diff[3]
            console.print(f"[bold grey53]│[/bold grey53] [green]+ Add[/green] column [medium_purple]{col.name}[/medium_purple] [grey53]({col.type})[/grey53]")
            for idx in index_map.get((table_name, col.name), []):
                if idx.name not in claimed_indexes:
                    console.print(f"[bold grey53]│   [/bold grey53]↳ with index [cyan]{idx.name}[/cyan]")
                    claimed_indexes.add(idx.name)

        case "remove_column":
            col = diff[3]
            console.print(f"[bold grey53]│[/bold grey53] [red]- Remove[/red] column [medium_purple]{col.name}[/medium_purple]")

        case "modify_nullable" | "modify_type" | "modify_default" | "modify_comment":
            print_column_modification(diff)

        case "add_index":
            idx = diff[1]
            if idx.name not in claimed_indexes:
                console.print(f"[bold grey53]│[/bold grey53] [green]+ Add[/green] index [cyan]{idx.name}[/cyan]")
                claimed_indexes.add(idx.name)

        case "remove_index":
            idx = diff[1]
            console.print(f"[bold grey53]│[/bold grey53] [red]- Remove[/red] index [cyan]{idx.name}[/cyan]")

        case "add_constraint" | "remove_constraint" | "add_fk" | "remove_fk":
            print_constraint(diff)

        case "add_table_comment":
            table = diff[1]
            console.print(f"[bold grey53]│[/bold grey53] [green]+ Add[/green] comment: [italic]{table.comment!r}[/italic]")

        case "remove_table_comment":
            console.print("[bold grey53]│[/bold grey53] [red]- Remove[/red] comment")

        case "execute":
            sql = diff[1]
            console.print(f"[bold grey53]│[/bold grey53] [cyan]Execute raw SQL:[/cyan] {sql!r}")

        case _:
            console.print(f"[bold grey53]│[/bold grey53] [red]? Unknown diff type[/red]: {kind} → {diff}")

def print_diffs(diffs: List[Diff]) -> None:
    if not diffs:
        console.print("[grey53]no schema changes detected[/grey53]")
        return

    console.print("[bold yellow]\nDatafruit will perform the following actions:")

    index_map = build_index_map(diffs)
    claimed_indexes = set()
    table_diffs = group_diffs_by_table(diffs)

    regular_tables = sorted(k for k in table_diffs if k not in ("__EXECUTE__", "__UNKNOWN__"))

    for table_name in regular_tables:
        table_changes = table_diffs[table_name]
        prefix = get_table_prefix(table_changes)
        console.print(f"\n{prefix} Table: [blue]{table_name}[/blue]")

        for diff in table_changes:
            print_diff_item(diff, index_map, claimed_indexes, table_name)

    for diff in table_diffs.get("__EXECUTE__", []):
        console.print(f"[bold grey53]│[/bold grey53] [cyan]Execute raw SQL:[/cyan] {diff[1]!r}")
