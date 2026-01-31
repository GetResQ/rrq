"""Registry for runner handlers."""

from typing import Any, Callable, Optional


class Registry:
    """Manages the registration and retrieval of job handler functions.

    Handlers are asynchronous functions that perform the actual work of a job.
    They are registered with a unique name, which is used by the producer to
    enqueue jobs and by the runner runtime to look up the appropriate handler.
    """

    def __init__(self) -> None:
        """Initializes an empty job registry."""
        self._handlers: dict[str, Callable[..., Any]] = {}

    def register(
        self, name: str, handler: Callable[..., Any], replace: bool = False
    ) -> None:
        """Registers a job handler function.

        Args:
            name: The unique name for this handler. Used when enqueuing jobs.
            handler: The asynchronous callable function that will execute the job.
                     It should accept an ExecutionRequest and return an ExecutionOutcome
                     or a raw result that will be wrapped as success.
            replace: If True, an existing handler with the same name will be replaced.
                     If False (default) and a handler with the same name exists,
                     a ValueError is raised.

        Raises:
            ValueError: If a handler with the same name is already registered and `replace` is False,
                        or if the provided handler is not callable.
        """
        if not callable(handler):
            raise ValueError(f"Handler for '{name}' must be a callable.")
        if name in self._handlers and not replace:
            raise ValueError(
                f"Handler with name '{name}' already registered. Set replace=True to override."
            )
        self._handlers[name] = handler

    def unregister(self, name: str) -> None:
        """Unregisters a job handler function.

        Args:
            name: The name of the handler to unregister.
        """
        # If the handler exists, remove it. Otherwise, do nothing.
        if name in self._handlers:
            del self._handlers[name]

    def get_handler(self, name: str) -> Optional[Callable[..., Any]]:
        """Retrieves a registered job handler function by its name.

        Args:
            name: The name of the handler to retrieve.

        Returns:
            The callable handler function if found, otherwise None.
        """
        return self._handlers.get(name)

    def get_registered_functions(self) -> list[str]:
        """Returns a list of names of all registered handler functions."""
        return list(self._handlers.keys())

    def clear(self) -> None:
        """Clears all registered handlers from the registry."""
        self._handlers.clear()
