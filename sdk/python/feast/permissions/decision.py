import enum
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DecisionStrategy(enum.Enum):
    """
    The strategy to be adopted in case multiple policies are defined.
    """

    UNANIMOUS = "unanimous"  # All policies must evaluate to a positive decision for the final decision to be also positive.
    AFFIRMATIVE = (
        "affirmative"  # At least one policy must evaluate to a positive decision
    )
    CONSENSUS = "consensus"  # The number of positive decisions must be greater than the number of negative decisions.


class DecisionEvaluator:
    def __init__(
        self,
        decision_strategy: DecisionStrategy,
        num_of_voters: int,
    ):
        self.num_of_voters = num_of_voters

        self.grant_count = 0
        self.deny_count = 0

        self.grant_quorum = (
            1
            if decision_strategy == DecisionStrategy.AFFIRMATIVE
            else num_of_voters
            if decision_strategy == DecisionStrategy.UNANIMOUS
            else num_of_voters // 2 + 1
        )
        self.deny_quorum = (
            num_of_voters
            if decision_strategy == DecisionStrategy.AFFIRMATIVE
            else 1
            if decision_strategy == DecisionStrategy.UNANIMOUS
            else num_of_voters // 2 + 1
        )
        self.grant_decision: Optional[bool] = None
        self.explanations: list[str] = []
        logger.info(
            f"Decision evaluation started with grant_quorum={self.grant_quorum}, deny_quorum={self.deny_quorum}"
        )

    def is_decided(self) -> bool:
        return self.grant_decision is not None

    def grant(self) -> tuple[bool, list[str]]:
        logger.info(
            f"Decided grant is {self.grant_decision}, explanations={self.explanations}"
        )
        return bool(self.grant_decision), self.explanations

    def add_grant(self, label, grant: bool, explanation: str):
        if self.is_decided():
            logger.warning("Grant decision already decided, discarding vote")
            return
        if grant:
            self.grant_count += 1
        else:
            self.deny_count += 1
            self.explanations.append(explanation)

        if self.grant_count >= self.grant_quorum:
            self.grant_decision = True
        if self.deny_count >= self.deny_quorum:
            self.grant_decision = False
        logger.info(
            f"After {label}: grants={self.grant_count}, deny_count={self.deny_count}, grant_decision={self.grant_decision}"
        )
