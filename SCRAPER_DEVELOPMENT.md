# Scraper Development Guide

This guide walks through adding a new data source to the scraper engine.

## Architecture Overview

The engine (`src/engine/`) is source-agnostic. Each scraper lives in `scrapers/<name>/` and provides two things:

- **`SourceDefinition`** — the engine contract: how to scrape, flatten, and query entries
- **`SourceConfig`** — the CLI contract: what args the source needs and how to resolve them

```
src/engine/base.py     →  SourceDefinition, SourceConfig, ResolvedParams
src/engine/engine.py   →  run_load(), run_refresh() (parallel execution)
src/engine/database.py →  ParquetWriter (append-only parquet storage)
src/engine/hash.py     →  compute_row_hash() (change detection)
```

The engine handles threading, rate limiting, batching, checkpoints, and compaction. Your scraper only needs to fetch and structure data.

## Step-by-Step

### 1. Create the scraper directory

```
scrapers/
└── my_source/
    ├── __init__.py
    └── source.py
```

### 2. Define your scrape function

The scrape function takes `(base_url, entry_id)` and returns a result dict. Raise a custom exception for invalid entries — the engine catches it and skips silently.

```python
# scrapers/my_source/source.py
from curl_cffi import requests as curl_requests

class InvalidEntryException(Exception):
    pass

def scrape_entry(base_url: str, entry_id: int | str) -> dict:
    url = f"{base_url}{entry_id}"
    resp = curl_requests.get(url, timeout=30)

    if resp.status_code == 404:
        raise InvalidEntryException(f"Entry {entry_id} not found")
    if resp.status_code != 200:
        raise InvalidEntryException(f"HTTP {resp.status_code}")

    data = resp.json()
    return {
        "entry_id": entry_id,
        "name": data["name"],
        "records": data.get("records", []),
    }
```

### 3. Define your flatten function

The engine calls `flatten_fn` with a batch of scrape results. It must return `{table_name: [row_dicts]}` — one key per output table.

```python
def flatten_results(results: list[dict]) -> dict[str, list[dict]]:
    entries = []
    records = []

    for result in results:
        entries.append({
            "entry_id": result["entry_id"],
            "name": result["name"],
        })
        for rec in result.get("records", []):
            rec["entry_id"] = result["entry_id"]
            records.append(rec)

    tables = {}
    if entries:
        tables["entries"] = entries
    if records:
        tables["records"] = records
    return tables
```

The engine automatically adds `scraped_at` and `row_hash` to every row before writing.

### 4. Define iteration and known-entry-ID functions

```python
from pathlib import Path
from typing import Iterator

def make_load_iter(start: int = 1, end: int = 1000):
    def iter_entries(base_url: str, data_dir: str, scope_key: str) -> Iterator[int]:
        yield from range(start, end + 1)
    return iter_entries

def get_known_entry_ids(data_dir: str, scope_key: str) -> list:
    import duckdb
    parquet_dir = Path(data_dir) / scope_key / "entries"
    files = list(parquet_dir.glob("*.parquet"))
    if not files:
        return []

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT entry_id FROM read_parquet('{parquet_dir}/*.parquet')"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()
```

### 5. Create the SourceDefinition

```python
from src.engine.base import SourceDefinition

MY_SOURCE = SourceDefinition(
    source_key="my_source",
    scrape_fn=scrape_entry,
    flatten_fn=flatten_results,
    get_known_entry_ids_fn=get_known_entry_ids,
    invalid_entry_exception=InvalidEntryException,
)
```

### 6. Create the SourceConfig

The `SourceConfig` tells the CLI how to handle your source — what args it needs and how to resolve them into engine parameters.

```python
from src.engine.base import ResolvedParams, SourceConfig

class MySourceConfig(SourceConfig):
    def __init__(self):
        super().__init__(
            source=MY_SOURCE,
            default_base_url="https://api.example.com/v1/",
        )

    def add_args(self, parser):
        """Register source-specific CLI arguments."""
        parser.add_argument("--start-id", type=int, default=1)
        parser.add_argument("--end-id", type=int, help="Last entry ID to scrape")

    def resolve(self, args):
        """Turn CLI args into engine parameters."""
        base_url = args.base_url or self.default_base_url
        scope_key = args.city or "my_source"

        iter_fn = None
        if not getattr(args, "refresh", False):
            if not args.end_id:
                raise ValueError("--end-id is required for load")
            iter_fn = make_load_iter(args.start_id, args.end_id)

        return ResolvedParams(
            base_url=base_url,
            scope_key=scope_key,
            iter_entries_fn=iter_fn,
        )

MY_SOURCE_CONFIG = MySourceConfig()
```

**SourceConfig methods you can override:**

| Method | Default | Override when |
|--------|---------|---------------|
| `add_args(parser)` | No-op | Your source has CLI flags |
| `resolve(args)` | Required | Always (must return `ResolvedParams`) |
| `get_all_scope_keys(data_dir)` | All non-`_` dirs | Your source uses a specific directory pattern |
| `run_admin(args)` | Returns False | You have admin commands (e.g. seed data) |

### 7. Export and register

```python
# scrapers/my_source/__init__.py
from .source import MY_SOURCE as MY_SOURCE
from .source import MY_SOURCE_CONFIG as MY_SOURCE_CONFIG
```

Add one line to `scrapers/__init__.py`:

```python
from .my_source import MY_SOURCE_CONFIG

REGISTRY = {
    "vgsi": VGSI_CONFIG,
    "ct_data": CT_DATA_CONFIG,
    "my_source": MY_SOURCE_CONFIG,  # <-- add this
}
```

That's it. The CLI automatically picks up the new source:

```bash
scrape load my_source --end-id 5000
scrape refresh my_source
scrape refresh-all  # includes my_source
```

### 8. Add tests

Create `tests/test_my_source.py`. Mock HTTP calls, test flatten logic, and verify parquet writes:

```python
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from scrapers.my_source.source import (
    MY_SOURCE,
    InvalidEntryException,
    flatten_results,
    scrape_entry,
)
from src.engine.database import ParquetWriter


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def writer(temp_dir):
    w = ParquetWriter("my_scope", MY_SOURCE, temp_dir)
    yield w
    w.close()


class TestFlatten:
    def test_basic(self):
        results = [{"entry_id": 1, "name": "Test", "records": []}]
        tables = flatten_results(results)
        assert "entries" in tables
        assert len(tables["entries"]) == 1


class TestParquetWrite:
    def test_write_and_query(self, writer, temp_dir):
        results = [{"entry_id": 1, "name": "Test", "records": []}]
        writer.write_batch(results)

        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/my_scope/entries/*.parquet')"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] == 1


class TestScrape:
    @patch("scrapers.my_source.source.curl_requests")
    def test_fetch(self, mock_curl):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "Test", "records": []}
        mock_curl.get.return_value = mock_resp

        result = scrape_entry("https://example.com/api/", 1)
        assert result["name"] == "Test"
```

## SourceDefinition Reference

| Field | Type | Description |
|-------|------|-------------|
| `source_key` | `str` | Unique identifier (e.g. `"vgsi"`) |
| `scrape_fn` | `(base_url, entry_id) -> dict` | Fetch one entry |
| `flatten_fn` | `(list[dict]) -> dict[str, list[dict]]` | Batch results to per-table rows |
| `get_known_entry_ids_fn` | `(data_dir, scope_key) -> list` | Entry IDs in parquet |
| `invalid_entry_exception` | `type[Exception]` | Exception for "entry not found" |
| `get_photo_items_fn` | `(result, scope_key, entry_id) -> list[tuple]` | Optional: photo download items |
| `download_fn` | `Callable` | Optional: photo download function |

## CLI Reference

```bash
# Load new entries
scrape load <source> [city] [--source-specific-args]

# Refresh known entries
scrape refresh <source> [city]

# Refresh everything (cron-friendly)
scrape refresh-all --quiet

# Admin commands
scrape admin <source> [--source-specific-args]
```

## How Storage Works

- **Append-only**: every scrape appends new parquet files, nothing is mutated
- **Compaction**: at the end of a run, batch files from the session are merged into one file per table
- **Metadata**: `scraped_at` and `row_hash` are added to every row automatically
- **Change detection**: done at query time by comparing `row_hash` across versions using window functions
- **Checkpoints**: stored as JSON in `data/_checkpoints/`, enabling resume after interruption
