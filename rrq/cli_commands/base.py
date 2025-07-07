"""Base classes and utilities for RRQ CLI commands"""

import asyncio
import importlib
import os
import pkgutil
from abc import ABC, abstractmethod
from typing import Callable

import click

from ..settings import RRQSettings
from ..store import JobStore


class BaseCommand(ABC):
    """Base class for all RRQ CLI commands"""

    @abstractmethod
    def register(self, cli_group: click.Group) -> None:
        """Register the command with the CLI group"""
        pass


class AsyncCommand(BaseCommand):
    """Base class for async CLI commands"""

    def make_async(self, func: Callable) -> Callable:
        """Wrapper to run async functions in click commands"""

        def wrapper(*args, **kwargs):
            return asyncio.run(func(*args, **kwargs))

        return wrapper


def load_app_settings(settings_object_path: str | None = None) -> RRQSettings:
    """Load the settings object from the given path.

    If not provided, the RRQ_SETTINGS environment variable will be used.
    If the environment variable is not set, will create a default settings object.
    """
    # Import the original function from cli.py
    from ..cli import _load_app_settings

    return _load_app_settings(settings_object_path)


def resolve_settings_source(
    settings_object_path: str | None = None,
) -> tuple[str | None, str]:
    """Resolve the settings path and its source."""
    # Import the original function from cli.py
    from ..cli import _resolve_settings_source

    return _resolve_settings_source(settings_object_path)


def auto_discover_commands(package_path: str) -> list[type[BaseCommand]]:
    """Auto-discover command classes in the given package"""
    commands = []

    # Get the package module
    try:
        package = importlib.import_module(package_path)
        package_dir = os.path.dirname(package.__file__)
    except ImportError:
        # Return empty list for non-existent packages
        return commands

    # Iterate through all modules in the package
    for _, module_name, is_pkg in pkgutil.iter_modules([package_dir]):
        if is_pkg:
            continue

        # Import the module
        module_path = f"{package_path}.{module_name}"
        try:
            module = importlib.import_module(module_path)

            # Look for BaseCommand subclasses
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseCommand)
                    and attr not in (BaseCommand, AsyncCommand)
                ):
                    commands.append(attr)
        except ImportError:
            # Skip modules that can't be imported
            continue

    return commands


async def get_job_store(settings: RRQSettings) -> JobStore:
    """Create and return a JobStore instance"""
    job_store = JobStore(settings=settings)
    # Test connection
    await job_store.redis.ping()
    return job_store
