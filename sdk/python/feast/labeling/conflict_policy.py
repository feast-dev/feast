from enum import Enum

from feast.protos.feast.core.LabelView_pb2 import (
    ConflictResolutionPolicy as ConflictResolutionPolicyProto,
)


class ConflictPolicy(Enum):
    """Determines how conflicting labels from different labelers are resolved.

    When multiple labelers write labels for the same entity key, the conflict
    policy controls which resolved value is returned for offline/batch reads
    (training data generation, UI browse, quality metrics).

    .. note::

        **Enforcement scope:** The conflict policy is enforced for **offline
        store reads** — all batch queries (``pull_all_from_table_or_query``,
        UI endpoints, training data generation) apply the configured resolution
        strategy. The **online store** always uses ``LAST_WRITE_WINS`` semantics
        regardless of this setting, as labeling is primarily a training-time
        concern.

    Attributes:
        LAST_WRITE_WINS: The most recently written label for a given entity key
            takes precedence, regardless of which labeler wrote it.
        LABELER_PRIORITY: Labels are ranked by a pre-configured labeler priority
            order. Higher-priority labelers override lower-priority ones.
        MAJORITY_VOTE: The label value that appears most frequently across all
            labelers is selected. Useful for consensus-based annotation workflows.
    """

    LAST_WRITE_WINS = "last_write_wins"
    LABELER_PRIORITY = "labeler_priority"
    MAJORITY_VOTE = "majority_vote"

    def to_proto(self) -> int:
        """Converts this ConflictPolicy to its protobuf enum value.

        Returns:
            The integer protobuf enum value corresponding to this policy.
        """
        return _POLICY_TO_PROTO[self]

    @classmethod
    def from_proto(cls, proto_val: int) -> "ConflictPolicy":
        """Creates a ConflictPolicy from a protobuf enum value.

        Args:
            proto_val: The integer protobuf enum value.

        Returns:
            The ConflictPolicy corresponding to the given protobuf value.
        """
        return _PROTO_TO_POLICY[proto_val]


_POLICY_TO_PROTO = {
    ConflictPolicy.LAST_WRITE_WINS: ConflictResolutionPolicyProto.LAST_WRITE_WINS,
    ConflictPolicy.LABELER_PRIORITY: ConflictResolutionPolicyProto.LABELER_PRIORITY,
    ConflictPolicy.MAJORITY_VOTE: ConflictResolutionPolicyProto.MAJORITY_VOTE,
}

_PROTO_TO_POLICY: dict[int, ConflictPolicy] = {
    v: k for k, v in _POLICY_TO_PROTO.items()
}
