import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class FVMaterializationStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class FVResult:
    name: str
    status: FVMaterializationStatus
    elapsed_seconds: Optional[float] = None
    error: Optional[str] = None


def poll_materialization_status(
    feature_view_names: List[str],
    status_fn: Callable[[str], FVMaterializationStatus],
    poll_interval: float = 5.0,
    timeout: float = 3600.0,
    on_status_change: Optional[Callable[[FVResult], None]] = None,
) -> List[FVResult]:
    """Poll materialization status for a list of feature views.

    This function is engine-agnostic. The caller provides a status_fn
    that maps a feature view name to its current materialization status.

    Used by:
    - Client-side remote materialize: status_fn reads FV state from remote registry
    - Server-side batch engines: status_fn reads from engine job objects

    Args:
        feature_view_names: FVs to track.
        status_fn: Returns current status for a given FV name.
        poll_interval: Seconds between polls.
        timeout: Max seconds to wait before declaring timeout.
        on_status_change: Callback invoked when a FV's status changes.

    Returns:
        List of FVResult with final status for each FV.
    """
    start = time.monotonic()
    pending: Set[str] = set(feature_view_names)
    results: Dict[str, FVResult] = {
        name: FVResult(name=name, status=FVMaterializationStatus.PENDING)
        for name in feature_view_names
    }
    previous_statuses: Dict[str, FVMaterializationStatus] = {}

    while pending and (time.monotonic() - start) < timeout:
        for name in list(pending):
            try:
                current = status_fn(name)
            except Exception as e:
                logger.warning(f"Error polling status for {name}: {e}")
                continue

            elapsed = time.monotonic() - start

            if current != previous_statuses.get(name):
                result = FVResult(name=name, status=current, elapsed_seconds=elapsed)
                results[name] = result
                previous_statuses[name] = current
                if on_status_change:
                    on_status_change(result)

            if current in (
                FVMaterializationStatus.SUCCEEDED,
                FVMaterializationStatus.FAILED,
            ):
                pending.discard(name)

        if pending:
            time.sleep(poll_interval)

    for name in pending:
        results[name] = FVResult(
            name=name,
            status=FVMaterializationStatus.FAILED,
            elapsed_seconds=time.monotonic() - start,
            error=f"Timed out after {timeout}s",
        )

    return list(results.values())
