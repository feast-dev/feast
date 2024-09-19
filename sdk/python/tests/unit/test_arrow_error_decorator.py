import pyarrow.flight as fl
import pytest

from feast.arrow_error_handler import arrow_client_error_handling_decorator
from feast.errors import PermissionNotFoundException

permissionError = PermissionNotFoundException("dummy_name", "dummy_project")


@arrow_client_error_handling_decorator
def decorated_method(error):
    raise error


@pytest.mark.parametrize(
    "error, expected_raised_error",
    [
        (fl.FlightError("Flight error: "), fl.FlightError("Flight error: ")),
        (
            fl.FlightError(f"Flight error: {permissionError.to_error_detail()}"),
            permissionError,
        ),
        (fl.FlightError("Test Error"), fl.FlightError("Test Error")),
        (RuntimeError("Flight error: "), RuntimeError("Flight error: ")),
        (permissionError, permissionError),
    ],
)
def test_rest_error_handling_with_feast_exception(error, expected_raised_error):
    with pytest.raises(
        type(expected_raised_error),
        match=str(expected_raised_error),
    ):
        decorated_method(error)
