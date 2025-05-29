from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Optional, Sequence

from pydantic import BaseModel, Field, PrivateAttr

MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

WEEKDAY_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def _parse_value(value: str, names: dict[str, int], min_val: int, max_val: int) -> int:
    if value.lower() in names:
        return names[value.lower()]
    num = int(value)
    if names is WEEKDAY_NAMES and num == 7:
        num = 0
    if not (min_val <= num <= max_val):
        raise ValueError(f"value {num} out of range {min_val}-{max_val}")
    return num


def _parse_field(field: str, *, names: dict[str, int] | None, min_val: int, max_val: int) -> Sequence[int]:
    names = names or {}
    if field == "*":
        return list(range(min_val, max_val + 1))
    values: set[int] = set()
    for part in field.split(','):
        step = 1
        if '/' in part:
            base, step_str = part.split('/', 1)
            step = int(step_str)
        else:
            base = part
        if base == "*":
            start, end = min_val, max_val
        elif '-' in base:
            a, b = base.split('-', 1)
            start = _parse_value(a, names, min_val, max_val)
            end = _parse_value(b, names, min_val, max_val)
        else:
            val = _parse_value(base, names, min_val, max_val)
            start = end = val
        if start > end:
            raise ValueError(f"invalid range {base}")
        for v in range(start, end + 1, step):
            values.add(v)
    return sorted(values)


class CronSchedule:
    """Represents a cron schedule expression."""

    def __init__(self, expression: str) -> None:
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError("Cron expression must have 5 fields")
        minute, hour, dom, month, dow = fields
        self.minutes = _parse_field(minute, names=None, min_val=0, max_val=59)
        self.hours = _parse_field(hour, names=None, min_val=0, max_val=23)
        self.dom = _parse_field(dom, names=None, min_val=1, max_val=31)
        self.months = _parse_field(month, names=MONTH_NAMES, min_val=1, max_val=12)
        self.dow = _parse_field(dow, names=WEEKDAY_NAMES, min_val=0, max_val=6)
        self.dom_all = dom == "*"
        self.dow_all = dow == "*"

    def next_after(self, dt: datetime) -> datetime:
        dt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        while True:
            if dt.month not in self.months:
                dt += timedelta(minutes=1)
                continue
            if dt.hour not in self.hours or dt.minute not in self.minutes:
                dt += timedelta(minutes=1)
                continue
            dom_match = dt.day in self.dom
            # Convert Python weekday (Monday=0) to cron weekday (Sunday=0)
            # Python: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
            # Cron:   Sun=0, Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6
            python_weekday = dt.weekday()
            cron_weekday = (python_weekday + 1) % 7
            dow_match = cron_weekday in self.dow
            
            if self.dom_all and self.dow_all:
                condition = True
            elif self.dom_all:
                # Only day-of-week constraint
                condition = dow_match
            elif self.dow_all:
                # Only day-of-month constraint
                condition = dom_match
            else:
                # Both constraints specified - use OR logic (standard cron behavior)
                condition = dom_match or dow_match
            if condition:
                return dt
            dt += timedelta(minutes=1)



class CronJob(BaseModel):
    """Simple cron job specification based on a cron schedule."""

    function_name: str
    schedule: str = Field(
        description="Cron expression 'm h dom mon dow'. Resolution is one minute."
    )
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    queue_name: Optional[str] = None
    unique: bool = False

    # Next run time and parsed schedule are maintained at runtime
    next_run_time: Optional[datetime] = Field(default=None, exclude=True)
    _cron: CronSchedule | None = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:  # type: ignore[override]
        self._cron = CronSchedule(self.schedule)

    def schedule_next(self, now: Optional[datetime] = None) -> None:
        """Compute the next run time strictly after *now*."""
        now = (now or datetime.now(UTC)).replace(second=0, microsecond=0)
        if self._cron is None:
            self._cron = CronSchedule(self.schedule)
        self.next_run_time = self._cron.next_after(now)

    def due(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.now(UTC)
        if self.next_run_time is None:
            self.schedule_next(now)
        return now >= (self.next_run_time or now)
