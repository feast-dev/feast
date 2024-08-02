import enum
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DecisionStrategy(enum.Enum):
    """
    The strategy to be adopted in case multiple permissions match an execution request.
    """

    UNANIMOUS = "unanimous"  # All policies must evaluate to a positive decision for the final decision to be also positive.
    AFFIRMATIVE = (
        "affirmative"  # At least one policy must evaluate to a positive decision
    )
    CONSENSUS = "consensus"  # The number of positive decisions must be greater than the number of negative decisions.


class DecisionEvaluator:
    """
    A class to implement the decision logic, according to the selected strategy.

    Args:
        decision_strategy: The associated `DecisionStrategy`.
        num_of_voters: The expected number of votes to complete the decision.

    Examples:
        Create the instance and specify the strategy and number of decisions:
        `evaluator = DecisionEvaluator(DecisionStrategy.UNANIMOUS, 3)

        For each vote that you receivem, add a decision grant: `evaluator.add_grant(vote, message)`
        and check if the decision process ended: `if evaluator.is_decided():`
        Once decided, get the result and the failure explanations using:
        `grant, explanations = evaluator.grant()`
    """

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
        """
        Returns:
            bool: `True` when the decision process completed (e.g. we added as many votes as specified in the `num_of_voters` creation argument).
        """
        return self.grant_decision is not None

    def grant(self) -> tuple[bool, list[str]]:
        """
        Returns:
            tuple[bool, list[str]]: The tuple of decision computation: a `bool` with the computation decision and a `list[str]` with the
            denial explanations (possibly empty).
        """
        logger.info(
            f"Decided grant is {self.grant_decision}, explanations={self.explanations}"
        )
        return bool(self.grant_decision), self.explanations

    def add_grant(self, grant: bool, explanation: str):
        """
        Add a single vote to the decision computation, with a possible denial reason.
        If the evalluation process already ended, additional votes are discarded.

        Args:
            grant: `True` is the decision is accepted, `False` otherwise.
            explanation: Denial reason (not considered when `vote` is `True`).
        """

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
        logger.debug(
            f"After new grant: grants={self.grant_count}, deny_count={self.deny_count}, grant_decision={self.grant_decision}"
        )
