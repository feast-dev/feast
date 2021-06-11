from datetime import datetime

from pytz import utc


def make_tzaware(t: datetime) -> datetime:
    """ We assume tz-naive datetimes are UTC """
    if t.tzinfo is None:
        return t.replace(tzinfo=utc)
    else:
        return t
