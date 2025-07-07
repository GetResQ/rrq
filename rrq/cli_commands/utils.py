"""Utilities for RRQ CLI formatting and display"""

from datetime import datetime
from typing import Any

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

from ..job import JobStatus


console = Console()


def create_table(title: str | None = None, **kwargs) -> Table:
    """Create a rich table with default styling"""
    table = Table(
        title=title,
        show_header=True,
        header_style="bold magenta",
        show_lines=False,
        expand=False,
        **kwargs,
    )
    return table


def format_status(status: JobStatus | str) -> Text:
    """Format job status with color"""
    if isinstance(status, JobStatus):
        status_str = status.value
    else:
        status_str = str(status)

    color_map = {
        "pending": "yellow",
        "active": "blue",
        "completed": "green",
        "failed": "red",
        "retrying": "orange",
        "cancelled": "dim",
    }

    color = color_map.get(status_str.lower(), "white")
    return Text(status_str.upper(), style=color)


def format_timestamp(ts: float | None) -> str:
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(ts)
        # Show relative time if recent
        now = datetime.now()
        diff = now - dt

        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}s ago"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}m ago"
        elif diff.total_seconds() < 86400:
            return f"{int(diff.total_seconds() / 3600)}h ago"
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def format_duration(seconds: float | None) -> str:
    """Format duration for display"""
    if seconds is None:
        return "N/A"

    if seconds < 0.001:
        return f"{seconds * 1000000:.0f}μs"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def format_bytes(size: int) -> str:
    """Format byte size for display"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"


def print_error(message: str) -> None:
    """Print an error message"""
    console.print(f"[red]ERROR:[/red] {message}")


def print_success(message: str) -> None:
    """Print a success message"""
    console.print(f"[green]✓[/green] {message}")


def print_warning(message: str) -> None:
    """Print a warning message"""
    console.print(f"[yellow]WARNING:[/yellow] {message}")


def print_info(message: str) -> None:
    """Print an info message"""
    console.print(f"[blue]INFO:[/blue] {message}")


def create_progress() -> Progress:
    """Create a progress bar"""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    )


def print_json(data: Any, title: str | None = None) -> None:
    """Print JSON data with syntax highlighting"""
    import json

    json_str = json.dumps(data, indent=2, default=str)
    syntax = Syntax(json_str, "json", theme="monokai", line_numbers=False)

    if title:
        panel = Panel(syntax, title=title, border_style="blue")
        console.print(panel)
    else:
        console.print(syntax)


def truncate_string(s: str, max_length: int = 50) -> str:
    """Truncate a string to a maximum length"""
    if len(s) <= max_length:
        return s
    return s[: max_length - 3] + "..."


def format_queue_name(queue: str) -> Text:
    """Format queue name with color based on type"""
    if queue.endswith("_dlq"):
        return Text(queue, style="red")
    elif queue == "default":
        return Text(queue, style="cyan")
    else:
        return Text(queue, style="blue")
