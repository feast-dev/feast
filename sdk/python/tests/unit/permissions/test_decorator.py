import assertpy
import pytest

from feast.errors import FeastPermissionError


@pytest.mark.parametrize(
    "username, can_read, can_write",
    [
        (None, False, False),
        ("r", True, False),
        ("w", False, True),
        ("rw", True, True),
    ],
)
def test_access_SecuredFeatureView(
    security_manager, feature_views, users, username, can_read, can_write
):
    sm = security_manager
    fv = feature_views[0]
    user = users.get(username)

    sm.set_current_user(user)
    if can_read:
        fv.read_protected()
    else:
        with pytest.raises(FeastPermissionError):
            fv.read_protected()
    if can_write:
        fv.write_protected()
    else:
        with pytest.raises(FeastPermissionError):
            fv.write_protected()
    assertpy.assert_that(fv.unprotected()).is_true()
