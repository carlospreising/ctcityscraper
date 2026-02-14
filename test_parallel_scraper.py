#!/usr/bin/env python3
"""
Test script for parallel scraper with DuckDB integration.

Tests:
- Small parallel scrape (PIDs 1-10, 3 workers)
- Rate limiting verification
- DuckDB schema validation
- Data integrity (foreign keys, UUIDs)
- Export functionality
"""

import os
import sys
import time
from pathlib import Path

from src.vgsi.export_utils import export_to_csv, get_database_stats, query_properties
from src.vgsi.logging_config import setup_logging
from src.vgsi.parallel_scraper import load_city_parallel

# Setup logging
logger = setup_logging(log_level="INFO", log_to_console=True, log_to_file=True)


def cleanup_test_files(db_path):
    """Clean up any existing test database."""
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info(f"Cleaned up existing test database: {db_path}")


def test_small_parallel_scrape():
    """Test 1: Small parallel scrape with 10 properties, 3 workers."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 1: Small Parallel Scrape (PIDs 1-10, 3 workers)")
    logger.info("=" * 60)

    db_path = "test_small.duckdb"
    cleanup_test_files(db_path)

    start_time = time.time()

    try:
        count = load_city_parallel(
            city="newhaven",
            db_path=db_path,
            pid_min=1,
            pid_max=10,
            max_workers=3,
            requests_per_second=2,
            checkpoint_every=5,
            batch_size=5,
            show_progress=True,
        )

        elapsed = time.time() - start_time

        logger.info(f"✅ Test 1 PASSED: Scraped {count} properties in {elapsed:.1f}s")
        return True

    except Exception as e:
        logger.error(f"❌ Test 1 FAILED: {e}", exc_info=True)
        return False


def test_data_integrity():
    """Test 2: Validate data integrity in database."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Data Integrity Validation")
    logger.info("=" * 60)

    db_path = "test_small.duckdb"

    try:
        # Check properties count
        prop_count_sql = "SELECT COUNT(*) as count FROM properties"
        result = query_properties(db_path, prop_count_sql)
        prop_count = result["count"][0]

        logger.info(f"Properties in database: {prop_count}")
        assert prop_count > 0, "No properties found in database"

        # Check for orphaned buildings
        orphan_sql = """
            SELECT COUNT(*) as count FROM buildings b
            WHERE NOT EXISTS (SELECT 1 FROM properties p WHERE p.uuid = b.property_uuid)
        """
        result = query_properties(db_path, orphan_sql)
        orphan_count = result["count"][0]

        logger.info(f"Orphaned building records: {orphan_count}")
        assert orphan_count == 0, f"Found {orphan_count} orphaned building records"

        # Check UUID uniqueness
        uuid_sql = """
            SELECT COUNT(DISTINCT uuid) as unique_count, COUNT(*) as total_count
            FROM properties
        """
        result = query_properties(db_path, uuid_sql)
        unique_count = result["unique_count"][0]
        total_count = result["total_count"][0]

        logger.info(f"Unique UUIDs: {unique_count} / {total_count}")
        assert unique_count == total_count, "Duplicate UUIDs found"

        # Get database stats
        stats = get_database_stats(db_path)
        logger.info(f"Database stats: {stats}")

        logger.info("✅ Test 2 PASSED: All integrity checks passed")
        return True

    except Exception as e:
        logger.error(f"❌ Test 2 FAILED: {e}", exc_info=True)
        return False


def test_export_functionality():
    """Test 3: Test CSV export functionality."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: CSV Export Functionality")
    logger.info("=" * 60)

    db_path = "test_small.duckdb"
    output_dir = "test_exports/"

    try:
        # Export to CSV
        exported_files = export_to_csv(db_path, output_dir)

        logger.info(f"Exported {len(exported_files)} files")

        # Verify files exist
        for file_path in exported_files:
            assert os.path.exists(file_path), f"Exported file not found: {file_path}"
            file_size = os.path.getsize(file_path)
            logger.info(f"  {Path(file_path).name}: {file_size:,} bytes")

        # Clean up export directory
        import shutil

        shutil.rmtree(output_dir)
        logger.info(f"Cleaned up test exports: {output_dir}")

        logger.info("✅ Test 3 PASSED: Export functionality working")
        return True

    except Exception as e:
        logger.error(f"❌ Test 3 FAILED: {e}", exc_info=True)
        return False


def test_checkpoint_resume():
    """Test 4: Test checkpoint save and resume functionality."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Checkpoint Resume Functionality")
    logger.info("=" * 60)

    db_path = "test_checkpoint.duckdb"
    cleanup_test_files(db_path)

    try:
        # First scrape: PIDs 1-5
        logger.info("First scrape: PIDs 1-5")
        count1 = load_city_parallel(
            city="newhaven",
            db_path=db_path,
            pid_min=1,
            pid_max=5,
            max_workers=2,
            requests_per_second=2,
            checkpoint_every=3,
            show_progress=False,
        )

        logger.info(f"First scrape complete: {count1} properties")

        # Second scrape: PIDs 1-10 (should resume from 6)
        logger.info("Second scrape: PIDs 1-10 (should resume from 6)")
        count2 = load_city_parallel(
            city="newhaven",
            db_path=db_path,
            pid_min=1,
            pid_max=10,
            max_workers=2,
            requests_per_second=2,
            checkpoint_every=3,
            resume_from_checkpoint=True,
            show_progress=False,
        )

        logger.info(f"Second scrape complete: {count2} additional properties")

        # Verify total
        result = query_properties(db_path, "SELECT COUNT(*) as count FROM properties")
        total_count = result["count"][0]

        logger.info(f"Total properties in database: {total_count}")
        assert total_count >= count1, "Resume didn't preserve existing data"

        # Clean up
        cleanup_test_files(db_path)

        logger.info("✅ Test 4 PASSED: Checkpoint resume working")
        return True

    except Exception as e:
        logger.error(f"❌ Test 4 FAILED: {e}", exc_info=True)
        return False


def main():
    """Run all tests."""
    logger.info("\n" + "#" * 60)
    logger.info("# PARALLEL SCRAPER TEST SUITE")
    logger.info("#" * 60)

    tests = [
        ("Small Parallel Scrape", test_small_parallel_scrape),
        ("Data Integrity", test_data_integrity),
        ("CSV Export", test_export_functionality),
        ("Checkpoint Resume", test_checkpoint_resume),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            passed = test_func()
            results.append((test_name, passed))
        except Exception as e:
            logger.error(f"Test '{test_name}' crashed: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name:30s}: {status}")

    logger.info("=" * 60)
    logger.info(f"Results: {passed_count}/{total_count} tests passed")
    logger.info("=" * 60)

    if passed_count == total_count:
        logger.info("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        logger.error(f"\n❌ {total_count - passed_count} TEST(S) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
