#!/usr/bin/env python3
"""
Load VGSI cities from JSON file into DuckDB database.

This script loads city information from vgsi_cities_ct.json into the
cities table in the database. This allows the parallel scraper to
look up city URLs without relying on the JSON file.

Usage:
    python scripts/load_cities.py
    python scripts/load_cities.py --db /path/to/database.duckdb
    python scripts/load_cities.py --json /path/to/cities.json --list
"""

import argparse
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.vgsi.database import DuckDBWriter
from src.vgsi.logging_config import get_logger, setup_logging

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Load VGSI cities from JSON into DuckDB database"
    )

    parser.add_argument(
        "--db",
        default="ctcityscraper.duckdb",
        help="Path to DuckDB database (default: ctcityscraper.duckdb)",
    )

    parser.add_argument(
        "--json",
        default="vgsi_cities_ct.json",
        help="Path to cities JSON file (default: vgsi_cities_ct.json)",
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List all cities in database after loading",
    )

    parser.add_argument(
        "--state",
        help="Filter cities by state when listing (e.g., 'ct')",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)

    # Check if JSON file exists
    if not os.path.exists(args.json):
        logger.error(f"JSON file not found: {args.json}")
        return 1

    # Initialize database
    logger.info(f"Connecting to database: {args.db}")
    db_writer = DuckDBWriter(args.db)

    # Load cities from JSON
    logger.info(f"Loading cities from: {args.json}")
    count = db_writer.load_cities_from_json(args.json)
    logger.info(f"✅ Successfully loaded {count} cities")

    # List cities if requested
    if args.list:
        logger.info("=" * 60)
        cities = db_writer.list_cities(state=args.state)

        if args.state:
            logger.info(f"Cities in {args.state.upper()}:")
        else:
            logger.info("All cities:")

        logger.info("=" * 60)

        for city in cities:
            logger.info(
                f"  {city['city_key']:20} -> {city['city_name']:30} | {city['url']}"
            )

        logger.info("=" * 60)
        logger.info(f"Total: {len(cities)} cities")

    db_writer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
