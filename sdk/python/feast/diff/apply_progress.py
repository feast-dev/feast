"""
Progress tracking infrastructure for feast apply operations.

This module provides the ApplyProgressContext class that manages progress bars
during apply operations, following the same tqdm_builder pattern used in
materialization operations.
"""

from dataclasses import dataclass
from typing import Callable, Optional

from tqdm import tqdm


@dataclass
class ApplyProgressContext:
    """
    Context object for tracking progress during feast apply operations.

    This class manages progress bars for the main phases of apply:
    1. Planning changes (computing diffs)
    2. Updating infrastructure (table creation/deletion)
    3. Updating registry (metadata updates)

    Follows the same tqdm_builder pattern used throughout Feast for consistency.
    """

    tqdm_builder: Callable[[int], tqdm]
    current_phase: str = ""
    overall_progress: Optional[tqdm] = None
    phase_progress: Optional[tqdm] = None

    # Phase tracking
    total_phases: int = 3
    completed_phases: int = 0

    # Infrastructure operation tracking
    total_infra_operations: int = 0
    completed_infra_operations: int = 0

    def start_overall_progress(self):
        """Initialize the overall progress bar for apply phases."""
        if self.overall_progress is None:
            self.overall_progress = self.tqdm_builder(self.total_phases)
            self.overall_progress.set_description("Applying changes")

    def start_phase(self, phase_name: str, operations_count: int = 0):
        """
        Start tracking a new phase.

        Args:
            phase_name: Human-readable name of the phase
            operations_count: Number of operations in this phase (0 for unknown)
        """
        self.current_phase = phase_name
        if operations_count > 0:
            self.phase_progress = self.tqdm_builder(operations_count)
            self.phase_progress.set_description(f"{phase_name}")

    def update_phase_progress(self, description: Optional[str] = None):
        """
        Update progress within the current phase.

        Args:
            description: Optional description of current operation
        """
        if self.phase_progress:
            self.phase_progress.update(1)
            if description:
                self.phase_progress.set_description(
                    f"{self.current_phase}: {description}"
                )

    def complete_phase(self):
        """Mark current phase as complete and advance overall progress."""
        if self.phase_progress:
            self.phase_progress.close()
            self.phase_progress = None
        if self.overall_progress:
            self.overall_progress.update(1)
        self.completed_phases += 1

    def cleanup(self):
        """Clean up all progress bars. Should be called in finally blocks."""
        if self.phase_progress:
            self.phase_progress.close()
            self.phase_progress = None
        if self.overall_progress:
            self.overall_progress.close()
            self.overall_progress = None
