from enum import Enum

from feast.protos.feast.core.LabelView_pb2 import (
    ConflictResolutionPolicy as ConflictResolutionPolicyProto,
)


class ConflictPolicy(Enum):
    """Determines how conflicting labels from different labelers are resolved.

    When multiple labelers write labels for the same entity key, the conflict
    policy controls which value is surfaced during online retrieval.

    .. note::

        **Alpha limitation:** The conflict policy is persisted in the registry
        alongside the ``LabelView`` definition, but it is **not yet enforced**
        during ``get_online_features``. The online store currently returns the
        last-written row for a given entity key regardless of the configured
        policy. Enforcement will be added in a future release.

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
