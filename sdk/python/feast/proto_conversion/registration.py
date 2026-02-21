# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Converter registration utilities.

This module provides functions to register all converters with the global
registry. Call register_all_converters() to set up the conversion system.
"""

import logging
from typing import Optional

from feast.proto_conversion.registry import ConverterRegistry, get_registry

logger = logging.getLogger(__name__)

_initialized = False


def register_all_converters(registry: Optional[ConverterRegistry] = None) -> None:
    """
    Register all converters with the registry.

    This function registers all built-in converters for Feast objects.
    It should be called once during application startup or the first
    time the conversion system is used.

    Args:
        registry: The registry to register converters with. If None,
                  uses the global registry instance.

    Note:
        This function is idempotent - calling it multiple times has
        no additional effect after the first call.
    """
    global _initialized

    if _initialized:
        return

    if registry is None:
        registry = get_registry()

    # Import converters here to avoid circular imports
    from feast.proto_conversion.converters import (
        EntityConverter,
        FeatureServiceConverter,
        FeatureViewConverter,
        OnDemandFeatureViewConverter,
    )

    # Register object converters
    registry.register(EntityConverter())
    registry.register(FeatureViewConverter())
    registry.register(OnDemandFeatureViewConverter())
    registry.register(FeatureServiceConverter())

    logger.debug("Registered all proto converters")
    _initialized = True


def reset_registration() -> None:
    """
    Reset the registration state.

    This is primarily useful for testing to ensure converters
    can be re-registered.
    """
    global _initialized
    _initialized = False
    ConverterRegistry.reset_instance()


def ensure_converters_registered() -> None:
    """
    Ensure converters are registered.

    This is a convenience function that can be called before using
    the conversion system to ensure all converters are available.
    """
    if not _initialized:
        register_all_converters()
