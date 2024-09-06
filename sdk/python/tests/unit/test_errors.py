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
