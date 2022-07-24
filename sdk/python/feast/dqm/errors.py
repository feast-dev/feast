from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .profilers.profiler import ValidationReport


class ValidationFailed(Exception):
    def __init__(self, validation_report: "ValidationReport"):
        self.validation_report = validation_report

    @property
    def report(self) -> "ValidationReport":
        return self.validation_report
