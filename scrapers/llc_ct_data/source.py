"""
CT Data source — Connecticut Business Registry from data.ct.gov.

Fetches 5 Socrata Open Data API datasets:
  - businesses  (n7gp-d28j)  Business Master
  - filings     (ah3s-bes7)  Filing History
  - agents      (qh2m-n44y)  Agents
  - principals  (ka36-64k6)  Principals
  - name_changes (enwv-52we) Name Change History

Write logic has been replaced by append-only parquet via the engine's
ParquetWriter. SCD2 versioning is derived at query time.
"""

import logging
import time
from pathlib import Path
from typing import Iterator, List, Optional

from curl_cffi import requests as curl_requests

from src.engine.base import ResolvedParams, SourceConfig, SourceDefinition

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

PAGE_SIZE = 50000

DATASETS = {
    "n7gp-d28j": "businesses",
    "ah3s-bes7": "filings",
    "qh2m-n44y": "agents",
    "ka36-64k6": "principals",
    "enwv-52we": "name_changes",
}

# Field renames: API field name → our column name (to avoid conflicts with PK)
FIELD_RENAMES = {
    "businesses": {"id": "business_id"},
}


class InvalidDatasetException(Exception):
    """Raised when a dataset ID is not recognized or returns an error."""

    pass


# ============================================================================
# HTTP — Fetching datasets via SODA API
# ============================================================================


def _fetch_page_with_retry(
    url: str,
    params: dict,
    max_retries: int = 3,
    initial_delay: float = 1,
    backoff_factor: float = 2,
    timeout: int = 120,
) -> list:
    """Fetch a single SODA API page with retry and exponential backoff."""
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            resp = curl_requests.get(url, params=params, timeout=timeout)
            if resp.status_code != 200:
                raise InvalidDatasetException(
                    f"API error: HTTP {resp.status_code}"
                )

            page = resp.json()
            if not isinstance(page, list):
                raise InvalidDatasetException(
                    f"Unexpected response type: {type(page)}"
                )

            if attempt > 0:
                logger.info(f"Request succeeded on attempt {attempt + 1}")

            return page

        except InvalidDatasetException:
            raise
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"Request failed after {max_retries + 1} attempts: {e}")

    raise last_exception or RuntimeError("Request failed with no recorded exception")


def fetch_dataset(base_url: str, entry_key: int | str) -> dict:
    """
    Fetch dataset rows from the SODA API.

    Accepts two entry_key formats:
    - "dataset_id:offset" — fetches a single page (used by load)
    - "dataset_id" — fetches all pages (used by refresh)

    Returns:
        {"dataset_id": str, "table_name": str, "rows": list[dict]}
    """
    entry_key = str(entry_key)

    if ":" in entry_key:
        # Page mode: fetch one page at the given offset
        dataset_id, offset_str = entry_key.rsplit(":", 1)
        offset = int(offset_str)
        table_name = DATASETS.get(dataset_id)
        if table_name is None:
            raise InvalidDatasetException(f"Unknown dataset ID: {dataset_id}")

        url = f"{base_url}{dataset_id}.json"
        params = {"$limit": str(PAGE_SIZE), "$offset": str(offset)}

        logger.info(f"Fetching {table_name} (offset={offset}, limit={PAGE_SIZE})")
        rows = _fetch_page_with_retry(url, params)
        logger.info(f"  Got {len(rows)} rows")
    else:
        # Full mode: fetch all pages (used by refresh for known datasets)
        dataset_id = entry_key
        table_name = DATASETS.get(dataset_id)
        if table_name is None:
            raise InvalidDatasetException(f"Unknown dataset ID: {dataset_id}")

        url = f"{base_url}{dataset_id}.json"
        rows = []
        offset = 0

        while True:
            params = {"$limit": str(PAGE_SIZE), "$offset": str(offset)}
            logger.info(f"Fetching {table_name} (offset={offset}, limit={PAGE_SIZE})")
            page = _fetch_page_with_retry(url, params)
            rows.extend(page)
            logger.info(f"  Got {len(page)} rows (total so far: {len(rows)})")
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        logger.info(f"Finished {table_name}: {len(rows)} total rows")

    # Apply field renames (e.g. businesses "id" → "business_id")
    renames = FIELD_RENAMES.get(table_name)
    if renames:
        rows = [{renames.get(k, k): v for k, v in row.items()} for row in rows]

    return {
        "dataset_id": dataset_id,
        "table_name": table_name,
        "rows": rows,
    }


def _count_dataset_pages(base_url: str, dataset_id: str) -> int:
    """Query the SODA API for total row count and return number of pages."""
    url = f"{base_url}{dataset_id}.json"
    params = {"$select": "count(*)", "$limit": "1"}
    try:
        page = _fetch_page_with_retry(url, params)
        total = int(page[0]["count"])
        pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        logger.info(f"Dataset {dataset_id}: {total} rows, {pages} pages")
        return max(pages, 1)
    except Exception as e:
        logger.warning(f"Could not count rows for {dataset_id}: {e}, using 1 page")
        return 1


# ============================================================================
# Flatten — extract per-table rows from scrape results
# ============================================================================


def flatten_llc_ct_data(results: List[dict]) -> dict[str, list[dict]]:
    """
    Flatten CT Data scrape results into per-table row dicts.

    Each result is {"dataset_id": str, "table_name": str, "rows": list[dict]}.
    Returns {table_name: [row_dicts]}.
    """
    tables: dict[str, list[dict]] = {}
    for result in results:
        table_name = result["table_name"]
        rows = result.get("rows", [])
        if rows:
            if table_name in tables:
                tables[table_name].extend(rows)
            else:
                tables[table_name] = list(rows)
    return tables


# ============================================================================
# Iteration & entry IDs
# ============================================================================


def make_load_iter(dataset_ids: Optional[List[str]] = None):
    """
    Returns an iter_entries_fn that yields page keys like "dataset_id:offset".

    Queries each dataset's row count first, then yields one key per page.
    This keeps memory usage constant regardless of dataset size.
    """

    def iter_entries(base_url: str, data_dir: str, scope_key: str) -> Iterator[str]:
        ids = dataset_ids or list(DATASETS.keys())
        for did in ids:
            num_pages = _count_dataset_pages(base_url, did)
            for page_num in range(num_pages):
                yield f"{did}:{page_num * PAGE_SIZE}"

    return iter_entries



# ============================================================================
# Source Definition
# ============================================================================


CT_DATA_SOURCE = SourceDefinition(
    source_key="llc_ct_data",
    scrape_fn=fetch_dataset,
    flatten_fn=flatten_llc_ct_data,
    entry_id_source=list(DATASETS.keys()),
    invalid_entry_exception=InvalidDatasetException,
)


# ============================================================================
# Source Config (CLI / orchestration)
# ============================================================================


class CTDataConfig(SourceConfig):
    def __init__(self):
        super().__init__(
            source=CT_DATA_SOURCE,
            default_base_url="https://data.ct.gov/resource/",
        )

    def add_args(self, parser):
        parser.add_argument(
            "--datasets",
            help="Comma-separated dataset IDs to load (default: all)",
        )

    def resolve(self, args):
        base_url = args.base_url or self.default_base_url
        if not base_url:
            raise ValueError("base_url is required (provide --base-url or set default_base_url)")
        scope_key = "llc_ct_data"

        iter_fn = None
        if not getattr(args, "refresh", False):
            dataset_ids = None
            if getattr(args, "datasets", None):
                dataset_ids = [d.strip() for d in args.datasets.split(",")]
            iter_fn = make_load_iter(dataset_ids)

        return ResolvedParams(
            base_url=base_url,
            scope_key=scope_key,
            iter_entries_fn=iter_fn,
        )

    def get_all_scope_keys(self, data_dir):
        base = Path(data_dir)
        if not base.exists():
            return []
        table_names = set(DATASETS.values())
        return [
            d.name
            for d in sorted(base.iterdir())
            if d.is_dir()
            and not d.name.startswith("_")
            and any((d / t).exists() for t in table_names)
        ]


CT_DATA_CONFIG = CTDataConfig()
