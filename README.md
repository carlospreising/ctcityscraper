# CT City Scraper

A web scraper for extracting property data from Connecticut cities and towns via the VGSI (Viewpoint GIS) online database.

## Features

- **Scrapes VGSI CT cities/towns** - Property records, ownership, valuations, building details

## Quick Start

### Installation

This project uses [uv](https://github.com/astral-sh/uv) for fast dependency management.

```bash
# Install dependencies
uv sync

# Run the test script
uv run python test_robustness.py
```

### Basic Usage

```python
from src.vgsi.logging_config import setup_logging
from src.vgsi.vgsi_utils import load_city

# Setup logging
logger = setup_logging(log_level="INFO")

# Scrape properties with rate limiting
properties, buildings, assessments, appraisals, ownership = load_city(
    city='newhaven',
    pid_min=1,
    pid_max=100,
    delay_seconds=2  # 2 second delay between requests (recommended)
)

# Check results
print(f"Scraped {len(properties)} properties")

# Check for errors
for prop in properties:
    if prop.get('parse_errors'):
        print(f"PID {prop['pid']}: {prop['parse_errors']}")
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

### Output Format

Data is returned as lists of dictionaries, one per property/building/assessment/etc.

```python
property = {
    'uuid': '...',
    'pid': 82,
    'address': '51 SOUTH END RD',
    'owner': 'CITY OF NEW HAVEN AIRPORT',
    'assessment_value': 1235010.0,
    'appraisal_value': 1764300.0,
    'land_size_acres': 3.8,
    'building_count': 1,
    'parse_errors': None,  # or "building_2,appraisals"
    'missing_fields': None,  # or "land_size_acres,zip_code"
    ...
}
```

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

The scraper supports all 169 Connecticut cities and towns with VGSI databases. Common examples:

- New Haven (`newhaven`)
- Hartford (`hartford`)
- Stamford (`stamford`)
- Bridgeport (`bridgeport`)
- Greenwich (`greenwich`)

Full list available in `vgsi_cities_ct.json`.
