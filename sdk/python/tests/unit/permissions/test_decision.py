import assertpy
import pytest

from feast.permissions.decision import DecisionEvaluator, DecisionStrategy

# Each vote is a tuple of `current_vote` and expected output of `is_decided`


@pytest.mark.parametrize(
    "evaluator, votes, decision, no_of_explanations",
    [
        (DecisionEvaluator(DecisionStrategy.AFFIRMATIVE, 3), [(True, True)], True, 0),
        (DecisionEvaluator(DecisionStrategy.AFFIRMATIVE, 3), [(True, True)], True, 0),
        (
            DecisionEvaluator(DecisionStrategy.AFFIRMATIVE, 3),
            [(False, False), (False, False), (False, True)],
            False,
            3,
        ),
        (
            DecisionEvaluator(DecisionStrategy.UNANIMOUS, 3),
            [(True, False), (True, False), (True, True)],
            True,
            0,
        ),
        (
            DecisionEvaluator(DecisionStrategy.UNANIMOUS, 3),
            [(True, False), (False, True)],
            False,
            1,
        ),
        (DecisionEvaluator(DecisionStrategy.CONSENSUS, 1), [(True, True)], True, 0),
        (DecisionEvaluator(DecisionStrategy.CONSENSUS, 1), [(False, True)], False, 1),
        (
            DecisionEvaluator(DecisionStrategy.CONSENSUS, 5),
            [
                (True, False),
                (False, False),
                (False, False),
                (True, False),
                (True, True),
            ],
            True,
            2,
        ),
        (
            DecisionEvaluator(DecisionStrategy.CONSENSUS, 5),
            [(True, False), (False, False), (False, False), (False, True)],
            False,
            3,
        ),
        (
            DecisionEvaluator(DecisionStrategy.UNANIMOUS, 2),
            [(True, False), (True, True), (False, True), (False, True), (False, True)],
            True,
            0,
        ),
        (
            DecisionEvaluator(DecisionStrategy.UNANIMOUS, 2),
            [(False, True), (False, True), (False, True), (False, True), (False, True)],
            False,
            1,
        ),
    ],
)
def test_decision_evaluator(evaluator, votes, decision, no_of_explanations):
    for v in votes:
        vote = v[0]
        decided = v[1]
        evaluator.add_grant(vote, "" if vote else "a message")
        if decided:
            assertpy.assert_that(evaluator.is_decided()).is_true()
        else:
            assertpy.assert_that(evaluator.is_decided()).is_false()

    grant, explanations = evaluator.grant()
    assertpy.assert_that(grant).is_equal_to(decision)
    assertpy.assert_that(explanations).is_length(no_of_explanations)
