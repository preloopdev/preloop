"""Safe benchmark runtime adapters.

Adapters never execute commands unless an explicit runner/transport is supplied;
this makes contract tests deterministic and prevents accidental production use.
"""
from .base import Adapter, AdapterError, EventWriter, LifecycleEvent
from .smolvm import SmolVMAdapter
from .hypeman import HypemanFirecrackerAdapter

__all__ = ["Adapter", "AdapterError", "EventWriter", "LifecycleEvent", "SmolVMAdapter", "HypemanFirecrackerAdapter"]
