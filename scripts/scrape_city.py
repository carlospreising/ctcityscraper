#!/usr/bin/env python3
"""
CLI tool for parallel scraping of CT property data.

Usage:
    python scripts/scrape_city.py --city newhaven --pid-min 1 --pid-max 100 --workers 10

    # Resume from checkpoint
    python scripts/scrape_city.py --city newhaven --pid-min 1 --pid-max 10000 --resume

    # Export to CSV
    python scripts/scrape_city.py --export --db mydata.duckdb --output exports/
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vgsi.export_utils import export_to_csv, get_database_stats
from vgsi.logging_config import setup_logging
from vgsi.parallel_scraper import load_city_parallel


def main():
    parser = argparse.ArgumentParser(
        description="Parallel scraper for CT property data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First, load cities into the database (required before scraping)
  python scripts/load_cities.py --list

  # Scrape properties 1-100 from New Haven with 10 workers
  python scripts/scrape_city.py --city newhaven --pid-min 1 --pid-max 100 --workers 10

  # Resume scraping from last checkpoint
  python scripts/scrape_city.py --city newhaven --pid-min 1 --pid-max 10000 --resume

  # Scrape with custom database path and rate limiting
  python scripts/scrape_city.py --city hartford --pid-min 1 --pid-max 500 \\
      --db hartford.duckdb --workers 5 --rate 3

  # Export database to CSV
  python scripts/scrape_city.py --export --db mydata.duckdb --output exports/

  # Show database statistics
  python scripts/scrape_city.py --stats --db mydata.duckdb

Note: Before scraping, make sure to load cities with:
      python scripts/load_cities.py
        """,
    )

    # Scraping options
    parser.add_argument(
        "--city", type=str, default="newhaven", help="City name (default: newhaven)"
    )
    parser.add_argument(
        "--pid-min", type=int, default=1, help="Starting PID (default: 1)"
    )
    parser.add_argument(
        "--pid-max", type=int, required=False, help="Ending PID (required for scraping)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of concurrent workers (default: 10)",
    )
    parser.add_argument(
        "--rate", type=int, default=5, help="Requests per second (default: 5)"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="ctcityscraper.duckdb",
        help="Database file path (default: ctcityscraper.duckdb)",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Save checkpoint every N properties (default: 100)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Write to DB every N properties (default: 10)",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume from last checkpoint"
    )
    parser.add_argument(
        "--no-progress", action="store_true", help="Disable progress bar"
    )

    # Export options
    parser.add_argument(
        "--export", action="store_true", help="Export database to CSV files"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="exports/",
        help="Output directory for CSV export (default: exports/)",
    )

    # Stats option
    parser.add_argument("--stats", action="store_true", help="Show database statistics")

    # Logging options
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        action="store_true",
        help="Enable file logging (default: True)",
        default=True,
    )

    args = parser.parse_args()

    # Setup logging
    log_level = getattr(__import__("logging"), args.log_level)
    logger = setup_logging(log_level=log_level, log_to_file=args.log_file)

    # Show statistics
    if args.stats:
        logger.info(f"Getting statistics for database: {args.db}")
        stats = get_database_stats(args.db)

        print("\n" + "=" * 60)
        print("DATABASE STATISTICS")
        print("=" * 60)
        for table, data in stats.items():
            if isinstance(data, dict):
                print(
                    f"{table:20s}: {data.get('count', 0):,} rows, {data.get('columns', 0)} columns"
                )
            else:
                print(f"{table:20s}: {data}")
        print("=" * 60 + "\n")
        return 0

    # Export to CSV
    if args.export:
        logger.info(f"Exporting database to CSV: {args.db} → {args.output}")
        exported_files = export_to_csv(args.db, args.output)

        print(f"\n✅ Exported {len(exported_files)} tables to {args.output}")
        for file in exported_files:
            print(f"  - {file}")
        return 0

    # Scraping mode
    if args.pid_max is None:
        parser.error("--pid-max is required for scraping mode")

    logger.info("=" * 60)
    logger.info("PARALLEL SCRAPER STARTING")
    logger.info("=" * 60)
    logger.info(f"City: {args.city}")
    logger.info(f"PID range: {args.pid_min} - {args.pid_max}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Rate limit: {args.rate} requests/sec")
    logger.info(f"Database: {args.db}")
    logger.info(f"Resume: {args.resume}")
    logger.info("=" * 60)

    try:
        count = load_city_parallel(
            city=args.city,
            db_path=args.db,
            pid_min=args.pid_min,
            pid_max=args.pid_max,
            max_workers=args.workers,
            requests_per_second=args.rate,
            checkpoint_every=args.checkpoint_every,
            resume_from_checkpoint=args.resume,
            batch_size=args.batch_size,
            show_progress=not args.no_progress,
        )

        print(f"\n✅ Scraping complete: {count} properties scraped successfully")
        print(f"   Database: {args.db}")
        print(f"   Use --export to export to CSV")
        print(f"   Use --stats to see database statistics")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nScraping interrupted by user (Ctrl+C)")
        logger.info("Progress has been saved to database")
        logger.info(
            f"Resume with: --resume --city {args.city} --pid-min {args.pid_min} --pid-max {args.pid_max}"
        )
        return 1

    except Exception as e:
        logger.error(f"Scraping failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
