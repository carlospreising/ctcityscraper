#!/usr/bin/env python3
"""
CLI entry point for scraping a VGSI city.

Usage:
    python scripts/scrape_city.py newhaven --pid-max 30000
    python scripts/scrape_city.py bridgeport --pid-min 1 --pid-max 50000 --workers 15
    python scripts/scrape_city.py --fetch-cities          # populate cities table
"""

import argparse
import logging
import sys

from src.vgsi import DuckDBWriter, fetch_vgsi_cities, load_city_parallel


def main():
    parser = argparse.ArgumentParser(description="Scrape VGSI property data")
    parser.add_argument("city", nargs="?", help="City key (e.g., newhaven)")
    parser.add_argument("--db", default="ctcityscraper.duckdb", help="Database path")
    parser.add_argument("--pid-min", type=int, default=1, help="Starting PID")
    parser.add_argument(
        "--pid-max", type=int, help="Ending PID (required for scraping)"
    )
    parser.add_argument(
        "--workers", type=int, default=10, help="Max concurrent workers"
    )
    parser.add_argument("--rate", type=float, default=5, help="Requests per second")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch write size")
    parser.add_argument(
        "--checkpoint-every", type=int, default=100, help="Checkpoint frequency"
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from checkpoint"
    )
    parser.add_argument("--base-url", help="Override VGSI base URL")
    parser.add_argument(
        "--download-photos", action="store_true", help="Download building photos"
    )
    parser.add_argument(
        "--photo-dir", default="photos", help="Photo download directory"
    )
    parser.add_argument(
        "--fetch-cities",
        action="store_true",
        help="Fetch and store city list from VGSI website",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.fetch_cities:
        cities = fetch_vgsi_cities()
        db = DuckDBWriter("main", args.db)
        count = db.store_cities(cities)
        print(f"Stored {count} cities in {args.db}")
        db.close()
        return

    if not args.city:
        parser.error("city is required (or use --fetch-cities)")

    if not args.pid_max and not args.base_url:
        parser.error("--pid-max is required for scraping")

    count = load_city_parallel(
        city=args.city,
        db_path=args.db,
        base_url=args.base_url,
        pid_min=args.pid_min,
        pid_max=args.pid_max,
        max_workers=args.workers,
        requests_per_second=args.rate,
        checkpoint_every=args.checkpoint_every,
        resume_from_checkpoint=not args.no_resume,
        batch_size=args.batch_size,
        download_photos=args.download_photos,
        photo_dir=args.photo_dir,
    )

    print(f"\nDone! Scraped {count} properties for {args.city}")


if __name__ == "__main__":
    main()
