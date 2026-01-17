from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class TLCSources:
    base_url: str
    trips_path_template: str
    zones_path: str

    def trip_url(self, year: int, month: int) -> str:
        yyyy = f"{year:04d}"
        mm = f"{month:02d}"
        path = self.trips_path_template.format(yyyy=yyyy, mm=mm)
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def zones_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.zones_path.lstrip('/')}"


def month_start_n_months_back(today: date, months_back: int) -> list[tuple[int, int]]:
    """Return list of (year, month) for the last N months including current month."""
    if months_back <= 0:
        raise ValueError("months_back must be > 0")

    y, m = today.year, today.month
    res: list[tuple[int, int]] = []
    for _ in range(months_back):
        res.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return res
