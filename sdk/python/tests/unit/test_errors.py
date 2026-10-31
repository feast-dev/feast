import re

import assertpy

import feast.errors as errors


def test_error_error_detail():
    e = errors.FeatureViewNotFoundException("abc")

    d = e.to_error_detail()

    assertpy.assert_that(d).is_not_none()
    assertpy.assert_that(d).contains('"module": "feast.errors"')
    assertpy.assert_that(d).contains('"class": "FeatureViewNotFoundException"')
    assertpy.assert_that(re.search(r"abc", d)).is_true()

    converted_e = errors.FeastError.from_error_detail(d)
    assertpy.assert_that(converted_e).is_not_none()
    assertpy.assert_that(str(converted_e)).is_equal_to(str(e))
    assertpy.assert_that(repr(converted_e)).is_equal_to(repr(e))


def test_invalid_error_error_detail():
    e = errors.FeastError.from_error_detail("invalid")
    assertpy.assert_that(e).is_none()


def test_from_error_detail_rejects_non_feast_modules():
    """Security: from_error_detail should reject modules outside feast.*"""
    malicious_detail = '{"module": "os", "class": "system", "message": "whoami"}'
    e = errors.FeastError.from_error_detail(malicious_detail)
    assertpy.assert_that(e).is_none()


def test_from_error_detail_allows_feast_modules():
    """from_error_detail should still work for legitimate feast errors."""
    original = errors.FeatureViewNotFoundException("test")
    detail = original.to_error_detail()
    restored = errors.FeastError.from_error_detail(detail)
    assertpy.assert_that(restored).is_not_none()
    assertpy.assert_that(str(restored)).is_equal_to(str(original))
