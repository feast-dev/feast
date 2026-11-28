import importlib

_torch = None
_torch_import_error = None
_torch_checked = False


def _ensure_torch():
    global _torch, _torch_import_error, _torch_checked
    if not _torch_checked:
        try:
            _torch = importlib.import_module("torch")
        except Exception as e:
            # Catch import errors including CUDA lib missing
            _torch = None
            _torch_import_error = e
        _torch_checked = True


def is_torch_available() -> bool:
    """Check if torch is available without crashing."""
    _ensure_torch()
    return _torch is not None


def get_torch():
    """
    Return the torch module if available, else raise a friendly error.

    This prevents crashing on import if CUDA libs are missing.
    """
    _ensure_torch()
    if _torch is not None:
        return _torch
    else:
        error_message = (
            "Torch is not available or failed to import.\n"
            "Original error:\n"
            f"{_torch_import_error}\n\n"
            "If you are on a CPU-only system, make sure you install the CPU-only torch wheel:\n"
            "  pip install torch torchvision -f https://download.pytorch.org/whl/cpu\n"
            "Or check your CUDA installation if using GPU torch.\n"
        )
        raise ImportError(error_message) from _torch_import_error


def __getattr__(name: str):
    if name == "TORCH_AVAILABLE":
        return is_torch_available()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
