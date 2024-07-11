import assertpy
import pytest

from feast.permissions.server.utils import AuthType, str_to_auth_manager_type


@pytest.mark.parametrize(
    "label, value",
    [(t.value, t) for t in AuthType]
    + [(t.value.upper(), t) for t in AuthType]
    + [(t.value.lower(), t) for t in AuthType]
    + [("none", AuthType.NONE)],
)
def test_str_to_auth_manager_type(label, value):
    assertpy.assert_that(str_to_auth_manager_type(label)).is_equal_to(value)
