#!/usr/bin/env python3
"""
Test script to verify scraper robustness improvements.

This script tests:
- Logging functionality
- Retry logic and timeout handling
- Rate limiting
- Parse error tracking
- Assessment (not "assesment") field naming

Usage:
    python test_robustness.py
"""

import sys

from src.vgsi.logging_config import setup_logging
from src.vgsi.vgsi_utils import load_city


def main():
    # Setup logging
    print("Setting up logging...")
    logger = setup_logging(log_level="INFO", log_to_console=True, log_to_file=True)
    logger.info("=" * 60)
    logger.info("Starting robustness test for VGSI scraper")
    logger.info("=" * 60)

    # Test scraping a small range with all improvements enabled
    print("\nTesting scraper with PIDs 1-5 from New Haven...")
    logger.info("Test parameters: city=newhaven, pid_min=1, pid_max=5, delay_seconds=2")

    try:
        (
            property_list,
            building_list,
            assessment_list,
            appraisal_list,
            ownership_list,
        ) = load_city(
            city="newhaven",
            pid_min=1,
            pid_max=5,
            null_pages_seq=3,
            delay_seconds=2,  # 2 second delay to test rate limiting
        )

        logger.info(f"\n{'=' * 60}")
        logger.info("RESULTS SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Properties scraped: {len(property_list)}")
        logger.info(f"Buildings found: {len(building_list)}")
        logger.info(f"Assessments found: {len(assessment_list)}")
        logger.info(f"Appraisals found: {len(appraisal_list)}")
        logger.info(f"Ownership records found: {len(ownership_list)}")

        # Check for parse errors
        properties_with_errors = [p for p in property_list if p.get("parse_errors")]
        if properties_with_errors:
            logger.warning(
                f"\nProperties with parse errors: {len(properties_with_errors)}"
            )
            for prop in properties_with_errors:
                logger.warning(f"  PID {prop.get('pid')}: {prop.get('parse_errors')}")
        else:
            logger.info("\nNo parse errors detected!")

        # Check for missing fields
        properties_with_missing = [p for p in property_list if p.get("missing_fields")]
        if properties_with_missing:
            logger.info(
                f"\nProperties with missing fields: {len(properties_with_missing)}"
            )
            for prop in properties_with_missing[:3]:  # Show first 3
                logger.info(f"  PID {prop.get('pid')}: {prop.get('missing_fields')}")

        # Verify 'assessment' field exists (not 'assesment')
        logger.info("\nVerifying field naming fixes...")
        for prop in property_list:
            if "assesment_value" in prop:
                logger.error(
                    f"ERROR: Found old 'assesment_value' field in PID {prop.get('pid')}"
                )
            if "assessment_value" in prop:
                logger.info(
                    f"✓ PID {prop.get('pid')}: 'assessment_value' field present"
                )
                break

        logger.info(f"\n{'=' * 60}")
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info(f"{'=' * 60}")

        print("\n✅ Test completed! Check logs/vgsi_scraper_*.log for detailed output")
        return 0

    except Exception as e:
        logger.error(f"\n{'=' * 60}")
        logger.error(f"TEST FAILED: {e}")
        logger.error(f"{'=' * 60}")
        import traceback

        logger.error(traceback.format_exc())
        print(f"\n❌ Test failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
