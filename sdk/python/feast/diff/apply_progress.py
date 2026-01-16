"""
Enhanced progress tracking infrastructure for feast apply operations.

This module provides the ApplyProgressContext class that manages positioned,
color-coded progress bars during apply operations with fixed-width formatting
for perfect alignment.
"""

from dataclasses import dataclass
from typing import Optional

from tqdm import tqdm

try:
    from feast.diff.progress_utils import (
        create_positioned_tqdm,
        get_color_for_phase,
        is_tty_available,
    )

    _PROGRESS_UTILS_AVAILABLE = True
except ImportError:
    # Graceful fallback when progress_utils is not available (e.g., in tests)
    _PROGRESS_UTILS_AVAILABLE = False

    def create_positioned_tqdm(*args, **kwargs):
        return None

    def get_color_for_phase(phase):
        return "blue"

    def is_tty_available():
        return False


@dataclass
class ApplyProgressContext:
    """
    Enhanced context object for tracking progress during feast apply operations.

    This class manages multiple positioned progress bars with fixed-width formatting:
    1. Overall progress (position 0) - tracks main phases
    2. Phase progress (position 1) - tracks operations within current phase

    Features:
    - Fixed-width alignment for perfect visual consistency
    - Color-coded progress bars by phase
    - Position coordination to prevent overlap
    - TTY detection for CI/CD compatibility
    """

    # Core tracking state
    current_phase: str = ""
    overall_progress: Optional[tqdm] = None
    phase_progress: Optional[tqdm] = None

    # Progress tracking
    total_phases: int = 3
    completed_phases: int = 0
    tty_available: bool = True

    # Position allocation
    OVERALL_POSITION = 0
    PHASE_POSITION = 1

    def __post_init__(self):
        """Initialize TTY detection after dataclass creation."""
        self.tty_available = _PROGRESS_UTILS_AVAILABLE and is_tty_available()

    def start_overall_progress(self):
        """Initialize the overall progress bar for apply phases."""
        if not self.tty_available:
            return

        if self.overall_progress is None:
            try:
                self.overall_progress = create_positioned_tqdm(
                    position=self.OVERALL_POSITION,
                    description="Applying changes",
                    total=self.total_phases,
                    color=get_color_for_phase("overall"),
                )
            except (TypeError, AttributeError):
                # Handle case where fallback functions don't work as expected
                self.overall_progress = None

    def start_phase(self, phase_name: str, operations_count: int = 0):
        """
        Start tracking a new phase.

        Args:
            phase_name: Human-readable name of the phase
            operations_count: Number of operations in this phase (0 for unknown)
        """
        if not self.tty_available:
            return

        self.current_phase = phase_name

        # Close previous phase progress if exists
        if self.phase_progress:
            try:
                self.phase_progress.close()
            except (AttributeError, TypeError):
                pass
            self.phase_progress = None

        # Create new phase progress bar if operations are known
        if operations_count > 0:
            try:
                self.phase_progress = create_positioned_tqdm(
                    position=self.PHASE_POSITION,
                    description=phase_name,
                    total=operations_count,
                    color=get_color_for_phase(phase_name.lower()),
                )
            except (TypeError, AttributeError):
                # Handle case where fallback functions don't work as expected
                self.phase_progress = None

    def update_phase_progress(self, description: Optional[str] = None):
        """
        Update progress within the current phase.

        Args:
            description: Optional description of current operation
        """
        if not self.tty_available or not self.phase_progress:
            return

        try:
            if description:
                # Update postfix with current operation
                self.phase_progress.set_postfix_str(description)

            self.phase_progress.update(1)
        except (AttributeError, TypeError):
            # Handle case where phase_progress is None or fallback function returned None
            pass

    def complete_phase(self):
        """Mark current phase as complete and advance overall progress."""
        if not self.tty_available:
            return

        # Close phase progress
        if self.phase_progress:
            try:
                self.phase_progress.close()
            except (AttributeError, TypeError):
                pass
            self.phase_progress = None

        # Update overall progress
        if self.overall_progress:
            try:
                self.overall_progress.update(1)
                # Update postfix with phase completion
                phase_text = f"({self.completed_phases + 1}/{self.total_phases} phases)"
                self.overall_progress.set_postfix_str(phase_text)
            except (AttributeError, TypeError):
                pass

        self.completed_phases += 1

    def cleanup(self):
        """Clean up all progress bars. Should be called in finally blocks."""
        if self.phase_progress:
            try:
                self.phase_progress.close()
            except (AttributeError, TypeError):
                pass
            self.phase_progress = None
        if self.overall_progress:
            try:
                self.overall_progress.close()
            except (AttributeError, TypeError):
                pass
            self.overall_progress = None
