
import time
import datetime
from typing import Tuple

from .base import TimePeriod

def get_period_span(period:'TimePeriod', session=None) -> Tuple[datetime.datetime, datetime.datetime]:

    # To prevent circular import
    from rocketry.parse import parse_time

    if period is None:
        return TimePeriod.min, TimePeriod.max
    if isinstance(period, str):
        period = parse_time(period)

    if session is None:
        now = datetime.datetime.fromtimestamp(time.time())
    else:
        # Note: the span is formed using the actual current time.
        # Log records written during the ongoing scheduler cycle
        # (after the cycle's business time was read) are still
        # considered to be arrived and are not excluded.
        now = session._format_timestamp(session.get_time())

    if hasattr(period, "use_reference"):
        # Period requires reference date
        # (usually timedelta related)
        # and it is current datetime
        period = period.use_reference(now)

    interval = period.rollback(now)
    start = interval.left
    end = interval.right
    return start, end
