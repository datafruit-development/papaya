from rich.console import Console
from sqlalchemy.schema import Column, Table, Index, Constraint
from typing import Any, List, Tuple, Union

console = Console()
Diff = Union[Tuple[Any, ...], List[Tuple[Any, ...]]]


def print_alembic_diffs(diffs: List[Diff]) -> None:
    if not diffs:
        console.print("\n[grey53]no schema changes detected[/grey53]\n")
        return

    console.print("[bold yellow]\nAlembic will perform the following actions:\n")

    index_map = {}  # (table_name, column_name) -> list of indexes
    claimed_indexes = set()  # names of indexes already printed with columns
    add_table_names = set()

    # First pass: gather add_table tables and map indexes to columns
    for diff in diffs:
        if isinstance(diff, tuple):
            kind = diff[0]
            match kind:
                case "add_table":
                    table: Table = diff[1]
                    add_table_names.add(table.name)
                case "add_index":
                    idx: Index = diff[1]
                    for expr in idx.expressions:
                        if hasattr(expr, "name"):
                            index_map.setdefault((idx.table.name, expr.name), []).append(idx)

    last_table = None
    already_rendered_indexes = set()

    for diff in diffs:
        if isinstance(diff, list):
            for subdiff in diff:
                last_table = print_alembic_diff(
                    subdiff,
                    index_map,
                    already_rendered_indexes,
                    claimed_indexes,
                    last_table,
                    add_table_names
                )
        else:
            last_table = print_alembic_diff(
                diff,
                index_map,
                already_rendered_indexes,
                claimed_indexes,
                last_table,
                add_table_names
            )


def print_alembic_diff(
    diff: Tuple[Any, ...],
    index_map,
    already_rendered_indexes,
    claimed_indexes,
    last_table,
    add_table_names,
) -> str:
    kind = diff[0]

    def grey_type(t):
        return f"[grey53]({t})[/grey53]"

    match kind:
        case "add_table":
            table: Table = diff[1]
            if last_table is not None:
                console.print()
            console.print(f"[green]+[/green] Table: [blue]{table.name}[/blue]")
            for col in table.columns:
                console.print(
                    f"[bold grey53]│[/bold grey53] [green]+ Add[/green] column [medium_purple]{col.name}[/medium_purple] {grey_type(col.type)}"
                )
                for idx in index_map.get((table.name, col.name), []):
                    if idx.name not in claimed_indexes:
                        console.print(
                            f"[bold grey53]│   [/bold grey53]↳ with index [cyan]{idx.name}[/cyan]"
                        )
                        claimed_indexes.add(idx.name)
            return table.name

        case "remove_table":
            table: Table = diff[1]
            if last_table != table.name:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [red]- Drop[/red] table [blue]{table.name}[/blue]"
            )
            return table.name

        case "add_column":
            _, schema, table, col = diff
            if last_table != table:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [green]+ Add[/green] column [medium_purple]{col.name}[/medium_purple] {grey_type(col.type)}"
            )
            for idx in index_map.get((table, col.name), []):
                if idx.name not in claimed_indexes:
                    console.print(
                        f"[bold grey53]│   [/bold grey53]↳ with index [cyan]{idx.name}[/cyan]"
                    )
                    claimed_indexes.add(idx.name)
            return table

        case "remove_column":
            _, schema, table, col = diff
            if last_table != table:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [red]- Remove[/red] column [medium_purple]{col.name}[/medium_purple]"
            )
            return table

        case "modify_nullable" | "modify_type" | "modify_default" | "modify_comment":
            _, schema, table, col, _, old, new = diff
            if last_table != table:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [yellow]~ Modify[/yellow] column [medium_purple]{col}[/medium_purple]"
            )

            match kind:
                case "modify_nullable":
                    console.print(
                        f"[bold grey53]│   [/bold grey53][dim]-[/dim] Nullability changed: {old} → {new}"
                    )
                case "modify_type":
                    console.print(
                        f"[bold grey53]│   [/bold grey53][dim]-[/dim] Type changed: {old} → {new}"
                    )
                case "modify_default":
                    console.print(
                        f"[bold grey53]│   [/bold grey53][dim]-[/dim] Default changed: {old} → {new}"
                    )
                case "modify_comment":
                    console.print(
                        f"[bold grey53]│   [/bold grey53][dim]-[/dim] Comment changed: {old!r} → {new!r}"
                    )

            return table

        case "add_index":
            idx: Index = diff[1]
            if idx.name in claimed_indexes:
                return idx.table.name
            if last_table != idx.table.name:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{idx.table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [green]+ Add[/green] index [cyan]{idx.name}[/cyan]"
            )
            claimed_indexes.add(idx.name)
            return idx.table.name

        case "remove_index":
            idx: Index = diff[1]
            if last_table != idx.table.name:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{idx.table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [red]- Remove[/red] index [cyan]{idx.name}[/cyan]"
            )
            return idx.table.name

        case "add_constraint" | "remove_constraint" | "add_fk" | "remove_fk":
            constraint: Constraint = diff[1]
            action = {
                "add_constraint": ("[green]+ Add[/green]", "constraint"),
                "remove_constraint": ("[red]- Remove[/red]", "constraint"),
                "add_fk": ("[green]+ Add[/green]", "foreign key"),
                "remove_fk": ("[red]- Remove[/red]", "foreign key"),
            }[kind]

            if last_table != constraint.table.name:
                console.print()

            # Handle missing constraint name
            constraint_label = constraint.name or f"(unnamed on {', '.join(c.name for c in constraint.columns)})"

            console.print(f"[yellow]~[/yellow] Table: [blue]{constraint.table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] {action[0]} [cyan]{action[1]}[/cyan] [medium_purple]{constraint_label}[/medium_purple]"
            )
            return constraint.table.name


        case "add_table_comment":
            table: Table = diff[1]
            if last_table != table.name:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [green]+ Add[/green] comment: [italic]{table.comment!r}[/italic]"
            )
            return table.name

        case "remove_table_comment":
            table: Table = diff[1]
            if last_table != table.name:
                console.print()
            console.print(f"[yellow]~[/yellow] Table: [blue]{table.name}[/blue]")
            console.print(
                f"[bold grey53]│[/bold grey53] [red]- Remove[/red] comment"
            )
            return table.name

        case "execute":
            sql = diff[1]
            console.print(f"[bold grey53]│[/bold grey53] [cyan]Execute raw SQL:[/cyan] {sql!r}")
            return last_table

        case _:
            console.print(f"[bold grey53]│[/bold grey53] [red]? Unknown diff type[/red]: {kind} → {diff}")
            return last_table
