"""Run blocking work on worker threads without abandoning it on cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


async def run_in_thread(
    operation: Callable[[], T], on_cancel: Callable[[], None] = lambda: None
) -> T:
    worker = asyncio.create_task(asyncio.to_thread(operation))
    cancelled = False
    # Threads cannot be stopped: drain the operation before completing cancellation.
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancelled = True
            on_cancel()
        except Exception:
            break
    if cancelled:
        if not worker.cancelled():
            worker.exception()
        raise asyncio.CancelledError
    return worker.result()
