# CT City Scraper

A web scraper for extracting property data from Connecticut cities and towns via the VGSI (Viewpoint GIS) online database.

## Features

- **Scrapes VGSI CT cities/towns** - Property records, ownership, valuations, building details
- **Uses duckdb, with a schema per city or town.**
- **Keeps it a bit simpler than before, but can scrape in parallel**

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

The scraper supports Connecticut cities and towns with VGSI databases. Common examples:

- New Haven (`newhaven`)
- Hartford (`hartford`)
- Stamford (`stamford`)
- Bridgeport (`bridgeport`)
- Greenwich (`greenwich`)

Full list available in `vgsi_cities_ct.json`.
