"""
Test suite for ctcityscraper.

Tests cover:
- Database operations (DuckDB schema, writing, querying)
- Scraper functions (parsing, data extraction, type coercion)
- Parallel scraping (rate limiting, concurrency, checkpoints)
- Integration tests (end-to-end workflows)

Run tests with:
    pytest tests/
    pytest tests/test_database.py
    pytest tests/test_scraper.py -v
"""
