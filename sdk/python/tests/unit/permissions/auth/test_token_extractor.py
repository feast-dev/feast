from unittest.mock import Mock

import assertpy
import pytest
from fastapi.requests import Request
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.server.arrow_flight_token_extractor import (
    ArrowFlightTokenExtractor,
)
from feast.permissions.server.rest_token_extractor import RestTokenExtractor


@pytest.mark.parametrize(
    "error_type, dict, header",
    [
        (ValueError, {}, None),
        (ValueError, {"other": 123}, None),
        (AuthenticationError, {}, ""),
        (AuthenticationError, {}, "abcd"),
        (AuthenticationError, {}, "other-scheme abcd"),
    ],
)
def test_rest_token_extractor_failures(error_type, dict, header):
    token_extractor = RestTokenExtractor()

    request = None
    if header is not None:
        request = Mock(spec=Request)
        if header != "":
            request.headers = {"authorization": header}
        else:
            request.headers = {}
    with pytest.raises(error_type):
        if request is None:
            token_extractor.extract_access_token(**dict)
        else:
            token_extractor.extract_access_token(request=request)


def test_rest_token_extractor():
    token_extractor = RestTokenExtractor()
    request: Request = Mock(spec=Request)
    token = "abcd"

    request.headers = {"authorization": f"Bearer {token}"}
    assertpy.assert_that(
        token_extractor.extract_access_token(request=request)
    ).is_equal_to(token)

    request.headers = {"authorization": f"bearer {token}"}
    assertpy.assert_that(
        token_extractor.extract_access_token(request=request)
    ).is_equal_to(token)


@pytest.mark.parametrize(
    "error_type, dict, header",
    [
        (ValueError, {}, None),
        (ValueError, {"other": 123}, None),
        (AuthenticationError, {}, ""),
        (AuthenticationError, {}, "abcd"),
        (AuthenticationError, {}, ["abcd"]),
        (AuthenticationError, {}, ["other-scheme abcd"]),
    ],
)
def test_arrow_flight_token_extractor_failures(error_type, dict, header):
    token_extractor = ArrowFlightTokenExtractor()

    headers = None
    if header is not None:
        if header != "":
            headers = {"authorization": header}
        else:
            headers = {}
    with pytest.raises(error_type):
        if headers is None:
            token_extractor.extract_access_token(**dict)
        else:
            token_extractor.extract_access_token(headers=headers)


def test_arrow_flight_token_extractor():
    token_extractor = ArrowFlightTokenExtractor()
    token = "abcd"

    headers = {"authorization": [f"Bearer {token}"]}
    assertpy.assert_that(
        token_extractor.extract_access_token(headers=headers)
    ).is_equal_to(token)

    headers = {"authorization": [f"bearer {token}"]}
    assertpy.assert_that(
        token_extractor.extract_access_token(headers=headers)
    ).is_equal_to(token)
