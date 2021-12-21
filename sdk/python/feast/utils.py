from datetime import datetime

from pytz import utc


def make_tzaware(t: datetime) -> datetime:
    """ We assume tz-naive datetimes are UTC """
    if t.tzinfo is None:
        return t.replace(tzinfo=utc)
    else:
        return t


def to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(utc).replace(tzinfo=None)
