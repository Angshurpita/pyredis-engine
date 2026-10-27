"""
Commands package. Registers all standard Redis commands upon import.
"""
from .base import (
    CommandError,
    CommandRegistry,
    CommandSpec,
    WrongArgCountError,
    WrongTypeError,
    registry,
)

# Import command modules to trigger registration
from . import strings
from . import hashes
from . import lists
from . import sets
from . import zsets
from . import keys
from . import server
from . import pubsub
from . import transactions
from . import replication

__all__ = [
    "registry",
    "CommandRegistry",
    "CommandSpec",
    "CommandError",
    "WrongTypeError",
    "WrongArgCountError",
]
