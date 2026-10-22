"""Sample prewriting hooks for the Feast Aerospike online store.

Reference the callable from ``feature_store.yaml`` via its import string,
e.g.::

    online_store:
      type: aerospike
      ...
      prewriting_hook: examples.online_store.aerospike_overrides_and_hooks.hooks.hash_pii_string_features

The Aerospike online store invokes the configured callable once per
``online_write_batch`` call, passing the rows about to be written. The
callable must return a row list with the same schema. Returning ``[]``
short-circuits the write — same path as an empty input, no wire call is
issued.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import List, Optional, Tuple

from feast import FeatureView
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig

# Names of features that must never reach the online store as plaintext.
# Match is by exact feature name; tweak to your project's conventions
# (regex, suffix-based, FV-tag-driven, etc.).
_SENSITIVE_FEATURES = frozenset({"email", "phone_number", "ssn"})

# Type alias for the per-row payload Feast hands to ``online_write_batch``.
WriteRow = Tuple[
    EntityKeyProto,
    dict,
    datetime,
    Optional[datetime],
]


def hash_pii_string_features(
    config: RepoConfig,
    table: FeatureView,
    data: List[WriteRow],
) -> List[WriteRow]:
    """Replace any sensitive string feature with a salted SHA-256 hex digest.

    Determinism: same plaintext + same ``FEAST_PII_SALT`` → same digest.
    Downstream lookups that hash the candidate value the same way still
    hit; lookups against the raw plaintext silently miss.

    Safety: an unset salt raises rather than falling back to plaintext.
    Set ``FEAST_PII_SALT`` on every process that materialises features
    (workers, registry CLI host, feature server).
    """
    salt = os.environ.get("FEAST_PII_SALT")
    if salt is None:
        raise RuntimeError(
            "FEAST_PII_SALT is not set; refusing to write feature batches "
            "without a configured PII salt."
        )
    salt_bytes = salt.encode("utf-8")

    def _digest(plaintext: str) -> str:
        h = hashlib.sha256()
        h.update(salt_bytes)
        h.update(plaintext.encode("utf-8"))
        return h.hexdigest()

    transformed: List[WriteRow] = []
    for entity_key, values, event_ts, created_ts in data:
        new_values = dict(values)
        for feature_name in _SENSITIVE_FEATURES.intersection(new_values):
            v: ValueProto = new_values[feature_name]
            if v.HasField("string_val") and v.string_val:
                new_values[feature_name] = ValueProto(string_val=_digest(v.string_val))
        transformed.append((entity_key, new_values, event_ts, created_ts))
    return transformed


def drop_rows_with_negative_amounts(
    config: RepoConfig,
    table: FeatureView,
    data: List[WriteRow],
) -> List[WriteRow]:
    """Defensive sample hook: filter rows whose ``amount`` feature is < 0.

    Demonstrates that hooks can also *remove* rows. Returning an empty
    list short-circuits the wire call entirely — useful for emergency
    feature-write quarantines without a code deploy.
    """
    keep: List[WriteRow] = []
    for entity_key, values, event_ts, created_ts in data:
        amount: Optional[ValueProto] = values.get("amount")
        if (
            amount is not None
            and amount.HasField("double_val")
            and amount.double_val < 0
        ):
            continue
        if amount is not None and amount.HasField("float_val") and amount.float_val < 0:
            continue
        if amount is not None and amount.HasField("int64_val") and amount.int64_val < 0:
            continue
        keep.append((entity_key, values, event_ts, created_ts))
    return keep
