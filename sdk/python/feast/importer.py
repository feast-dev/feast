import importlib

from feast.errors import (
    FeastClassImportError,
    FeastInvalidBaseClass,
    FeastModuleImportError,
)


def import_class(module_name: str, class_name: str, class_type: str = ""):
    """
    Dynamically loads and returns a class from a module.

    Args:
        module_name: The name of the module.
        class_name: The name of the class.
        class_type: Optional name of a base class of the class.

    Raises:
        FeastInvalidBaseClass: If the class name does not end with the specified suffix.
        FeastModuleImportError: If the module cannot be imported.
        FeastClassImportError: If the class cannot be imported.
    """
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

    # Check if the class is a subclass of the base class.
    if class_type and not any(
        base_class.__name__ == class_type for base_class in _class.mro()
    ):
        raise FeastInvalidBaseClass(class_name, class_type)

    return _class
