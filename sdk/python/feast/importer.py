import importlib
from typing import Optional

from feast.errors import (
    FeastClassImportError,
    FeastClassInvalidName,
    FeastModuleImportError,
)


def get_class_from_module(
    module_name: str, class_name: str, class_type: Optional[str] = None
):
    """
    Dynamically loads and returns a class from a module.

    Args:
        module_name: The name of the module.
        class_name: The name of the class.
        class_type: Optional suffix of the class.

    Raises:
        FeastClassInvalidName: If the class name does not end with the specified suffix.
        FeastModuleImportError: If the module cannot be imported.
        FeastClassImportError: If the class cannot be imported.
    """
    if class_type and not class_name.endswith(class_type):
        raise FeastClassInvalidName(class_name, class_type)

    # Try importing the module.
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # The original exception can be anything - either module not found,
        # or any other kind of error happening during the module import time.
        # So we should include the original error as well in the stack trace.
        raise FeastModuleImportError(module_name, class_name) from e

    # Try getting the class.
    try:
        _class = getattr(module, class_name)
    except AttributeError:
        # This can only be one type of error, when class_name attribute does not exist in the module
        # So we don't have to include the original exception here
        raise FeastClassImportError(module_name, class_name) from None

    return _class
