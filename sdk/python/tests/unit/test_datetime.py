# -*- coding: utf-8 -*-

from datetime import datetime

from feast.utils import _utc_now

"""
Test the retirement of datetime.utcnow() function.
"""


def test_utc_now():
    dt_obj = _utc_now()
    assert isinstance(dt_obj, datetime)
    # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
    assert dt_obj.tzinfo
    assert dt_obj.tzinfo.utcoffset(dt_obj) is not None
    # Check if the timezone if UTC
    assert dt_obj.tzname() == "UTC"
