"""Download Chicago Taxi Trips from the Socrata (SODA) API, one month at a time.

The API is paginated with $limit/$offset and ordered on trip_id so that pages are
stable. Each page is kept *exactly* as returned by the API (raw CSV, header
included) -> that is what lands in the bronze layer.

Usable standalone to pre-download a local cache:

    python -m pipeline.ingestion.soda --start-month 2023-01 --end-month 2023-04 --out data/raw
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.config import settings

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Page:
    index: int
    content: bytes
    rows: int


def next_month(month: str) -> str:
    year, mon = (int(x) for x in month.split("-"))
    return f"{year + (mon // 12)}-{(mon % 12) + 1:02d}"


def month_range(start_month: str, end_month: str) -> list[str]:
    """Months in [start_month, end_month) as 'YYYY-MM'."""
    months, current = [], start_month
    while current < end_month:
        months.append(current)
        current = next_month(current)
    return months


def build_params(month: str, limit: int, offset: int) -> dict[str, str]:
    start, end = f"{month}-01T00:00:00", f"{next_month(month)}-01T00:00:00"
    return {
        "$where": f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'",
        "$order": "trip_id",
        "$limit": str(limit),
        "$offset": str(offset),
    }


def count_csv_rows(content: bytes) -> int:
    """Number of data rows (header excluded), robust to quoted newlines."""
    reader = csv.reader(io.StringIO(content.decode("utf-8")))
    return max(sum(1 for _ in reader) - 1, 0)


def make_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    if settings.soda_app_token:
        session.headers["X-App-Token"] = settings.soda_app_token
    return session


def iter_api_pages(
    month: str,
    page_size: int | None = None,
    session: requests.Session | None = None,
    base_url: str | None = None,
) -> Iterator[Page]:
    page_size = page_size or settings.soda_page_size
    session = session or make_http_session()
    base_url = base_url or settings.soda_base_url
    index = 0
    while True:
        params = build_params(month, limit=page_size, offset=index * page_size)
        response = session.get(base_url, params=params, timeout=300)
        response.raise_for_status()
        rows = count_csv_rows(response.content)
        if rows == 0:
            return
        log.info("month=%s page=%s rows=%s", month, index, rows)
        yield Page(index=index, content=response.content, rows=rows)
        if rows < page_size:
            return
        index += 1


def local_month_dir(month: str, raw_dir: str | None = None) -> Path:
    return Path(raw_dir or settings.raw_local_dir) / f"trip_month={month}"


def iter_local_pages(month: str, raw_dir: str | None = None) -> Iterator[Page]:
    for i, path in enumerate(sorted(local_month_dir(month, raw_dir).glob("*.csv"))):
        content = path.read_bytes()
        yield Page(index=i, content=content, rows=count_csv_rows(content))


def has_local_cache(month: str, raw_dir: str | None = None) -> bool:
    folder = local_month_dir(month, raw_dir)
    return folder.is_dir() and any(folder.glob("*.csv"))


def download_to_disk(start_month: str, end_month: str, out_dir: str) -> None:
    for month in month_range(start_month, end_month):
        target = local_month_dir(month, out_dir)
        target.mkdir(parents=True, exist_ok=True)
        for old in target.glob("*.csv"):
            old.unlink()
        total = 0
        for page in iter_api_pages(month):
            (target / f"page_{page.index:05d}.csv").write_bytes(page.content)
            total += page.rows
        print(f"{month}: {total:,} rows -> {target}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-month", default=settings.start_month, help="inclusive, YYYY-MM")
    parser.add_argument("--end-month", default=settings.end_month, help="exclusive, YYYY-MM")
    parser.add_argument("--out", default=settings.raw_local_dir)
    args = parser.parse_args()
    download_to_disk(args.start_month, args.end_month, args.out)


if __name__ == "__main__":
    main()
