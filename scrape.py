#!/usr/bin/env python3
"""
CLI entry point for the multi-source scraper engine.

Usage:
    scrape load vgsi newhaven --entry-id-max 30000
    scrape load ct_data --datasets enwv-52we
    scrape refresh vgsi newhaven
    scrape refresh-all --quiet
    scrape admin vgsi --fetch-cities
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from scrapers import REGISTRY
from src.engine import ParquetWriter, run_load, run_refresh

logger = logging.getLogger(__name__)


def cmd_load(args, config):
    params = config.resolve(args)
    if params.iter_entries_fn is None:
        print("Error: load requires iteration parameters", file=sys.stderr)
        return 1

    writer = ParquetWriter(params.scope_key, config.source, args.data_dir)
    count = run_load(
        scope_key=params.scope_key,
        writer=writer,
        source=config.source,
        base_url=params.base_url,
        iter_entries_fn=params.iter_entries_fn,
        max_workers=args.workers,
        requests_per_second=args.rate,
        checkpoint_every=args.checkpoint_every,
        resume_from_checkpoint=not args.no_resume,
        batch_size=args.batch_size,
        show_progress=not args.quiet,
        download_photos=getattr(args, "download_photos", False),
        photo_dir=getattr(args, "photo_dir", "photos"),
    )
    print(f"Done! Loaded {count} entries for {params.scope_key}")
    return 0


def cmd_refresh(args, config):
    params = config.resolve(args)
    writer = ParquetWriter(params.scope_key, config.source, args.data_dir)
    args._refresh_start_time = datetime.now()
    count = run_refresh(
        scope_key=params.scope_key,
        writer=writer,
        source=config.source,
        base_url=params.base_url,
        max_workers=args.workers,
        requests_per_second=args.rate,
        batch_size=args.batch_size,
        show_progress=not args.quiet,
        download_photos=getattr(args, "download_photos", False),
        photo_dir=getattr(args, "photo_dir", "photos"),
    )
    print(f"Done! Refreshed {count} entries for {params.scope_key}")
    config.post_refresh(args, params)
    return 0


def cmd_refresh_all(args):
    failures = 0
    total = 0

    for key, config in REGISTRY.items():
        scope_keys = config.get_all_scope_keys(args.data_dir)
        if not scope_keys:
            logger.info(f"No data found for source '{key}', skipping")
            continue

        for sk in scope_keys:
            try:
                # Build a minimal args namespace for resolve()
                resolve_args = _make_resolve_args(args, city=sk, refresh=True)
                params = config.resolve(resolve_args)

                writer = ParquetWriter(params.scope_key, config.source, args.data_dir)
                count = run_refresh(
                    scope_key=params.scope_key,
                    writer=writer,
                    source=config.source,
                    base_url=params.base_url,
                    max_workers=args.workers,
                    requests_per_second=args.rate,
                    batch_size=args.batch_size,
                    show_progress=not args.quiet,
                )
                total += count
                logger.info(f"Refreshed {count} entries for {key}/{sk}")
            except Exception as e:
                logger.error(f"Failed to refresh {key}/{sk}: {e}")
                failures += 1

    print(f"Refresh-all complete: {total} entries refreshed, {failures} failures")
    return 1 if failures else 0


def cmd_admin(args, config):
    if config.run_admin(args):
        return 0
    print(f"No admin command matched for source '{config.source_key}'", file=sys.stderr)
    return 1


def _make_resolve_args(args, **overrides):
    """Create a copy of args with overrides for resolve() calls."""
    import argparse

    d = vars(args).copy()
    d.update(overrides)
    return argparse.Namespace(**d)


def main():
    import argparse

    source_keys = list(REGISTRY.keys())

    # Shared args inherited by all subcommands
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--db", default="ctcityscraper.duckdb", help="DuckDB path (city lookup)")
    shared.add_argument("--data-dir", default="data", help="Parquet output directory")
    shared.add_argument("--workers", type=int, default=10, help="Concurrent threads")
    shared.add_argument("--rate", type=float, default=5, help="Requests per second")
    shared.add_argument("--batch-size", type=int, default=10, help="Batch write size")
    shared.add_argument(
        "--checkpoint-every", type=int, default=100, help="Checkpoint frequency"
    )
    shared.add_argument("--no-resume", action="store_true", help="Don't resume from checkpoint")
    shared.add_argument("--base-url", help="Override base URL for the source")
    shared.add_argument("--quiet", action="store_true", help="Suppress progress bars")
    shared.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    parser = argparse.ArgumentParser(description="CT City Scraper")
    sub = parser.add_subparsers(dest="command")

    # load <source> [city]
    load_p = sub.add_parser("load", parents=[shared], help="Load new entries")
    load_p.add_argument("source", choices=source_keys, help="Data source")
    load_p.add_argument("city", nargs="?", help="City/scope key")

    # refresh <source> [city]
    refresh_p = sub.add_parser("refresh", parents=[shared], help="Refresh known entries")
    refresh_p.add_argument("source", choices=source_keys, help="Data source")
    refresh_p.add_argument("city", nargs="?", help="City/scope key")

    # refresh-all
    sub.add_parser("refresh-all", parents=[shared], help="Refresh all known sources and scopes")

    # admin <source>
    admin_p = sub.add_parser("admin", parents=[shared], help="Source-specific admin commands")
    admin_p.add_argument("source", choices=source_keys, help="Data source")

    # Let each source register its args on all source-aware subparsers
    for config in REGISTRY.values():
        for sp in [load_p, refresh_p, admin_p]:
            config.add_args(sp)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Logging setup
    handlers = [logging.StreamHandler()]
    if args.quiet:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"scrape_{datetime.now().strftime('%Y%m%d')}.log"
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )

    if args.command == "refresh-all":
        return cmd_refresh_all(args)

    config = REGISTRY[args.source]
    args.refresh = args.command == "refresh"

    try:
        if args.command == "load":
            return cmd_load(args, config)
        elif args.command == "refresh":
            return cmd_refresh(args, config)
        elif args.command == "admin":
            return cmd_admin(args, config)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        return 1
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
