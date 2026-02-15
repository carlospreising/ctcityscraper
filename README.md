# CT City Scraper

A web scraper for extracting property data from Connecticut cities and towns via the VGSI online database.

## Quick Start

### Installation

This project uses [uv](https://github.com/astral-sh/uv) for fast dependency management.

```bash
# Install dependencies
uv sync

# Run the test script
./run_tests.sh
```

## Data Structure

### Property Data
- **Identifiers**: PID, account number, certificate
- **Location**: Address, town, zip code, owner address
- **Ownership**: Owner, co-owner, sale information
- **Valuations**: Sale price, assessment value, appraisal value
- **Land**: Acreage, frontage, depth, use code, zone, neighborhood
- **Building**: Count, use type, building records
- **Historical**: Ownership records, assessment/appraisal history

### Retry Logic

Default configuration:
- Max retries: 3
- Initial delay: 1 second
- Backoff factor: 2x (1s → 2s → 4s)
- Request timeout: 30 seconds

### Logging
Logs are written to:
- **Console**: INFO level and above
- **File**: `logs/vgsi_scraper_YYYYMMDD_HHMMSS.log` (DEBUG level)

## Project Structure

```
ctcityscraper/
├── src/
│   └── vgsi/
│       ├── vgsi_objects.py      # Core data models (Property, Building, etc.)
│       ├── vgsi_utils.py        # Scraping utilities (load_city, etc.)
│       ├── logging_config.py    # Logging configuration
│       └── validators.py        # Data validation functions
├── logs/                        # Auto-generated log files
├── test_robustness.py           # Test script
├── pyproject.toml               # Project dependencies (uv/pip)
├── uv.lock                      # Locked dependency versions
└── vgsi_cities_ct.json          # City registry (169 CT cities)
```

## Available Cities

The scraper supports all 80~ Connecticut cities and towns with VGSI databases. Common examples:

- New Haven (`newhaven`)
- Hartford (`hartford`)
- Stamford (`stamford`)
- Bridgeport (`bridgeport`)
- Greenwich (`greenwich`)

Full list available in `vgsi_cities_ct.json`.

## Parallel Scraping

### Overview

The parallel scraper uses ThreadPoolExecutor for concurrent scraping and stores data in DuckDB for better performance and queryability.

### Quick Start

```bash
# Scrape 1,000 properties with 10 workers
uv run python scripts/scrape_city.py \
    --city newhaven \
    --pid-min 1 \
    --pid-max 1000 \
    --workers 10 \
    --rate 5

# Export to CSV
uv run python scripts/scrape_city.py --export --output exports/
```

### Command-Line Options

```
# Scraping options
--city CITY              City name (default: newhaven)
--pid-min N              Starting PID (default: 1)
--pid-max N              Ending PID (REQUIRED)
--workers N              Concurrent threads (default: 10)
--rate N                 Requests per second (default: 5)
--db PATH                Database file (default: ctcityscraper.duckdb)
--resume                 Resume from last checkpoint
--checkpoint-every N     Save checkpoint every N properties (default: 100)
--batch-size N           Write to DB every N properties (default: 10)

# Export/query options
--export                 Export database to CSV files
--output DIR             CSV output directory (default: exports/)
--stats                  Show database statistics

# Logging
--log-level LEVEL        DEBUG, INFO, WARNING, ERROR (default: INFO)
```

### Examples

#### Small Test Scrape
```bash
# Test with 50 properties, 3 workers
uv run python scripts/scrape_city.py \
    --city newhaven \
    --pid-min 1 \
    --pid-max 50 \
    --workers 3 \
    --rate 2 \
    --db test.duckdb
```

#### Large Production Scrape
```bash
# Scrape 10,000 properties with resume capability
uv run python scripts/scrape_city.py \
    --city newhaven \
    --pid-min 1 \
    --pid-max 10000 \
    --workers 10 \
    --rate 5 \
    --resume \
    --db newhaven_full.duckdb
```

#### Resume After Interruption
```bash
# If scraping is interrupted (Ctrl+C), resume with:
uv run python scripts/scrape_city.py \
    --city newhaven \
    --pid-min 1 \
    --pid-max 10000 \
    --resume
```

### Performance Tuning

**Worker Count:**
- Start conservative: 5-10 workers
- Monitor CPU and network usage
- More workers ≠ always faster (rate limiting is the bottleneck)

**Rate Limiting:**
- Conservative: 2-3 req/sec (safest, prevents IP bans)
- Moderate: 5 req/sec (recommended for production)
- Aggressive: 10+ req/sec (use with caution, monitor closely)

**Batch Size:**
- Smaller (5-10): More frequent DB writes, safer but slower
- Larger (20-50): Fewer DB writes, faster but risk data loss on crash

### Monitoring

```bash
# Watch logs in real-time
tail -f logs/vgsi_scraper_*.log

# Check database stats
uv run python scripts/scrape_city.py --stats --db newhaven.duckdb

```
