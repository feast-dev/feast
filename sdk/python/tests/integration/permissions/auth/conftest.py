import pytest

from tests.unit.permissions.auth.server.test_utils import (
    invalid_list_entities_perm,
    read_entities_perm,
    read_fv_perm,
    read_odfv_perm,
    read_permissions_perm,
    read_projects_perm,
    read_sfv_perm,
)


@pytest.fixture(
    scope="module",
    params=[
        [],
        [invalid_list_entities_perm],
        [
            read_entities_perm,
            read_permissions_perm,
            read_fv_perm,
            read_odfv_perm,
            read_sfv_perm,
            read_projects_perm,
        ],
    ],
)
def applied_permissions(request):
    return request.param
