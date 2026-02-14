# Test Suite

Comprehensive tests for the CT City Scraper project.

## Installation

Install test dependencies:

```bash
uv sync --extra dev
# or
pip install -e ".[dev]"
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest tests/test_database.py
pytest tests/test_scraper.py
pytest tests/test_parallel.py
```

### Run with verbose output
```bash
pytest -v
```

### Run with coverage report
```bash
pytest --cov=src --cov-report=html
```

### Run specific test class or function
```bash
pytest tests/test_database.py::TestDuckDBWriter::test_write_property_only
pytest tests/test_scraper.py::TestTypeCoercion
```

## Test Organization

### `test_database.py` - Database Operations
Tests for DuckDB schema creation, data writing, and querying:
- Schema creation and table initialization
- Writing properties, buildings, ownership, appraisals, assessments
- Upsert operations
- Checkpoint save/load
- City management
- Batch operations
- Index creation
- Foreign key relationships

**Key tests:**
- `test_init_creates_schema` - Verify schema creation
- `test_write_complete_property` - Test writing all property fields
- `test_upsert_property` - Test update on conflict
- `test_write_buildings` - Test building records with construction details
- `test_checkpoint_save_and_load` - Test checkpoint functionality
- `test_store_cities` - Test city registry

### `test_scraper.py` - Scraper Functions
Tests for HTML parsing and data extraction:
- Type coercion (money, float, int, string)
- UUID generation
- Property parsing from HTML
- Building parsing with construction details and sub-areas
- Table parsing (ownership, appraisals, assessments, features)
- Edge cases and error handling

**Key tests:**
- `test_handle_money_valid` - Test money field parsing
- `test_parse_property_basic` - Test basic property field extraction
- `test_parse_buildings_multiple` - Test parsing multiple buildings
- `test_parse_table_rows_ownership` - Test ownership history parsing
- `test_parse_property_whitespace_handling` - Test edge cases

### `test_parallel.py` - Parallel Scraping
Tests for concurrent scraping and rate limiting:
- Rate limiter functionality
- Concurrent thread execution
- Integration with database
- Checkpoint and resume
- Error handling (invalid PIDs, network errors)
- Batch writing
- Photo downloading

**Key tests:**
- `test_rate_limiter_enforces_interval` - Test rate limiting
- `test_rate_limiter_concurrent` - Test thread safety
- `test_load_city_parallel_basic` - Test basic parallel scraping
- `test_load_city_parallel_handles_invalid_pid` - Test error recovery
- `test_load_city_parallel_checkpoints` - Test checkpoint creation
- `test_load_city_parallel_resume` - Test resume from checkpoint
- `test_full_workflow_integration` - End-to-end integration test

## Test Markers

Tests can be marked with categories:

```python
@pytest.mark.unit
def test_something():
    pass

@pytest.mark.integration
def test_database_workflow():
    pass

@pytest.mark.slow
def test_large_scrape():
    pass
```

Run specific markers:
```bash
pytest -m unit
pytest -m integration
pytest -m "not slow"
```

## Coverage

Generate coverage report:

```bash
# Terminal report
pytest --cov=src

# HTML report (opens in browser)
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

## Continuous Integration

These tests are designed to run in CI/CD pipelines. They:
- Use temporary databases (no cleanup required)
- Mock external network calls
- Complete quickly (< 30 seconds for full suite)
- Are deterministic (no flaky tests)

## Writing New Tests

### Test Structure
```python
import pytest
from src.vgsi.database import DuckDBWriter

class TestNewFeature:
    """Test description."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database."""
        # Setup
        yield db_path
        # Cleanup
    
    def test_something(self, temp_db):
        """Test specific behavior."""
        # Arrange
        writer = DuckDBWriter("city", temp_db)
        
        # Act
        result = writer.some_method()
        
        # Assert
        assert result == expected
```

### Best Practices
1. **Use fixtures** for shared setup/teardown
2. **Mock external calls** (network requests, file I/O)
3. **Test edge cases** (empty input, None values, errors)
4. **Use descriptive names** (`test_write_property_with_missing_fields`)
5. **One assertion focus** per test when possible
6. **Clean up resources** in fixtures

## Troubleshooting

### Tests fail with "module not found"
```bash
# Install in editable mode
pip install -e .
```

### Tests hang or timeout
```bash
# Run with timeout
pytest --timeout=30
```

### Database locked errors
```bash
# Tests create temporary databases, shouldn't conflict
# If issues persist, check for unclosed connections
```

### Import errors
```bash
# Ensure project root is in PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest
```
