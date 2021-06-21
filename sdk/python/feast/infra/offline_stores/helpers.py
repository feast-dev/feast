import importlib
from typing import Any

from feast import errors
from feast.infra.offline_stores.offline_store import OfflineStore


def get_offline_store_from_config(offline_store_config: Any,) -> OfflineStore:
    """Get the offline store from offline store config"""

    module_name = offline_store_config.__module__
    qualified_name = type(offline_store_config).__name__
    store_class_name = qualified_name.replace("Config", "")
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # The original exception can be anything - either module not found,
        # or any other kind of error happening during the module import time.
        # So we should include the original error as well in the stack trace.
        raise errors.FeastModuleImportError(module_name, "OfflineStore") from e

    # Try getting the provider class definition
    try:
        offline_store_class = getattr(module, store_class_name)
    except AttributeError:
        # This can only be one type of error, when class_name attribute does not exist in the module
        # So we don't have to include the original exception here
        raise errors.FeastClassImportError(
            module_name, store_class_name, class_type="OfflineStore"
        ) from None
    return offline_store_class()
