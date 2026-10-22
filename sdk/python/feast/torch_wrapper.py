import importlib

TORCH_AVAILABLE = False
_torch = None
_torch_import_error = None


def _import_torch():
    global _torch, TORCH_AVAILABLE, _torch_import_error
    try:
        _torch = importlib.import_module("torch")
        TORCH_AVAILABLE = True
    except Exception as e:
        # Catch import errors including CUDA lib missing
        TORCH_AVAILABLE = False
        _torch_import_error = e


_import_torch()


def get_torch():
    """
    Return the torch module if available, else raise a friendly error.

    This prevents crashing on import if CUDA libs are missing.
    """
    if TORCH_AVAILABLE:
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
