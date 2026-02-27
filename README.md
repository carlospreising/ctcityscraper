# CT City Scraper

A multi-source scraper engine for extracting public data from Connecticut. Currently supports:

- **VGSI** — property assessment data from ~80 CT cities/towns
- **CT Data** — business registry datasets from data.ct.gov

## Quick Start

### Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
uv sync
uv sync --extra dev   # for tests
```

### Scraping VGSI Property Data

```bash
# Populate the cities lookup table (one-time setup)
uv run scrape admin vgsi --fetch-cities

# Scrape a range of properties
uv run scrape load vgsi newhaven --entry-id-max 27000 --workers 10

# Resume after interruption (checkpoints are automatic)
uv run scrape load vgsi newhaven --entry-id-max 27000

# Re-scrape known properties to detect changes
uv run scrape refresh vgsi newhaven

# Download building photos
uv run scrape load vgsi newhaven --entry-id-max 27000 --download-photos
```

### Scraping CT Data (Business Registry)

```bash
# Load all 5 datasets
uv run scrape load ct_data

# Load specific datasets
uv run scrape load ct_data --datasets n7gp-d28j,enwv-52we

# Refresh existing data
uv run scrape refresh ct_data
```

### Cron / Scheduled Runs

```bash
# Refresh all known sources and scopes (no flags needed)
uv run scrape refresh-all --quiet

# Example crontab entry (daily at 2am)
0 2 * * * cd /path/to/ctcityscraper && uv run scrape refresh-all --quiet --log-level WARNING
```

## Project Structure

```
ctcityscraper/
├── scrape.py              # CLI entry point
├── src/engine/            # Generic scraper engine (source-agnostic)
│   ├── base.py            #   SourceDefinition, SourceConfig contracts
│   ├── database.py        #   ParquetWriter (append-only storage)
│   ├── engine.py          #   Parallel execution, rate limiting, checkpoints
│   └── hash.py            #   Row hashing for change detection
├── scrapers/              # Source-specific scrapers
│   ├── __init__.py        #   REGISTRY of all sources
│   ├── vgsi/              #   VGSI property data
│   │   └── source.py
│   └── ct_data/           #   CT business registry
│       └── source.py
└── tests/
```

## Data Storage

All data is stored as **append-only parquet files** under `data/`:

```
data/
├── newhaven/
│   ├── properties/
│   │   └── 20260226_143000_123456.parquet
│   ├── buildings/
│   ├── ownership/
│   └── ...
├── ct_data/
│   ├── businesses/
│   ├── filings/
│   └── ...
└── _checkpoints/
    └── newhaven.json
```

Each scrape session produces one parquet file per table (batch files are compacted at the end of each run). Every row includes `scraped_at` and `row_hash` metadata. Change detection is done at query time — no data is ever mutated.

### Querying with DuckDB

```sql
-- Current state of all properties
SELECT * FROM read_parquet('data/newhaven/properties/*.parquet')
QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY scraped_at DESC) = 1;

-- Properties that changed between scrapes
SELECT * FROM (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY uuid ORDER BY scraped_at) AS prev_hash
    FROM read_parquet('data/newhaven/properties/*.parquet')
)
WHERE prev_hash IS NOT NULL AND row_hash != prev_hash;
```

## CLI Reference

```
scrape <command> [options]

Commands:
  load <source> [city]     Load new entries
  refresh <source> [city]  Re-scrape known entries to detect changes
  refresh-all              Refresh all known sources and scopes
  admin <source>           Source-specific admin commands

Global options:
  --data-dir DIR           Parquet output directory (default: data)
  --db PATH                DuckDB path for city lookup (default: ctcityscraper.duckdb)
  --workers N              Concurrent threads (default: 10)
  --rate N                 Requests per second (default: 5)
  --batch-size N           Batch write size (default: 10)
  --checkpoint-every N     Checkpoint frequency (default: 100)
  --no-resume              Don't resume from checkpoint
  --base-url URL           Override base URL for the source
  --quiet                  Suppress progress bars, log to file
  --log-level LEVEL        DEBUG, INFO, WARNING, ERROR (default: INFO)

VGSI-specific:
  --entry-id-min N         Starting entry ID (default: 1)
  --entry-id-max N         Ending entry ID (required for load)
  --fetch-cities           Fetch VGSI city list (admin command)
  --download-photos        Download building photos
  --photo-dir DIR          Photo directory (default: photos)

CT Data-specific:
  --datasets IDS           Comma-separated dataset IDs (default: all)
```

## Adding a New Scraper

See [SCRAPER_DEVELOPMENT.md](SCRAPER_DEVELOPMENT.md) for a step-by-step guide.

## Running Tests

```bash
uv run pytest tests/ -v
```
