"""
Command registry, base execution context, and validation decorators.
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class CommandError(Exception):
    """Raised when command execution encounters a Redis user error."""
    pass


class WrongTypeError(CommandError):
    def __init__(self):
        super().__init__("WRONGTYPE Operation against a key holding the wrong kind of value")


class WrongArgCountError(CommandError):
    def __init__(self, cmd: str):
        super().__init__(f"ERR wrong number of arguments for '{cmd}' command")


@dataclass
class CommandSpec:
    name: str
    handler: Callable[..., Any]
    is_mutating: bool = False
    min_args: int = 0
    max_args: Optional[int] = None  # None means unlimited


class CommandRegistry:
    """Central registry of supported Redis commands."""

    def __init__(self):
        self._commands: Dict[str, CommandSpec] = {}

    def register(
        self,
        name: str,
        is_mutating: bool = False,
        min_args: int = 0,
        max_args: Optional[int] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a command handler."""
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            cmd_name = name.upper()
            self._commands[cmd_name] = CommandSpec(
                name=cmd_name,
                handler=fn,
                is_mutating=is_mutating,
                min_args=min_args,
                max_args=max_args,
            )
            return fn
        return decorator

    def get(self, name: str) -> Optional[CommandSpec]:
        return self._commands.get(name.upper())

    def __iter__(self):
        return iter(self._commands.values())


registry = CommandRegistry()
