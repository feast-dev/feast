import importlib

from feast import errors


def get_class_from_type(module_name: str, class_name: str, class_type: str):
    if not class_name.endswith(class_type):
        raise errors.FeastClassInvalidName(class_name, class_type)

    # Try importing the module that contains the custom provider
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # The original exception can be anything - either module not found,
        # or any other kind of error happening during the module import time.
        # So we should include the original error as well in the stack trace.
        raise errors.FeastModuleImportError(module_name, class_type) from e

    # Try getting the provider class definition
    try:
        _class = getattr(module, class_name)
    except AttributeError:
        # This can only be one type of error, when class_name attribute does not exist in the module
        # So we don't have to include the original exception here
        raise errors.FeastClassImportError(
            module_name, class_name, class_type=class_type
        ) from None
    return _class
