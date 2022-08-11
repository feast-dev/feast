import warnings

from feast.infra.registry.registry import BaseRegistry, Registry

warnings.warn(
    "The BaseRegistry class has been moved to the feast.infra.registry.base_registry module. "
    "The Registry class has been moved to the feast.infra.registry.registry module. Please "
    "import these classes from their new modules.",
    RuntimeWarning,
)
