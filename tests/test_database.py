"""
Tests for ParquetWriter and VGSI data writing.

Tests parquet file creation, checkpointing, and query-time versioning.
"""

import shutil
import tempfile
import time
from pathlib import Path

import duckdb
import pytest

from src.engine.database import ParquetWriter
from src.engine.hash import compute_row_hash
from scrapers.vgsi.source import (
    VGSI_SOURCE,
    get_changed_properties,
    get_known_entry_ids,
    get_property_history,
)


@pytest.fixture
def temp_dir():
    """Create a temporary data directory for parquet output."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def writer(temp_dir):
    """Create a ParquetWriter instance for testing."""
    w = ParquetWriter("testcity", VGSI_SOURCE, temp_dir)
    yield w
    w.close()


def _query_parquet(data_dir, scope_key, table_name, sql_suffix=""):
    """Helper to query parquet files via in-memory DuckDB."""
    pattern = f"{data_dir}/{scope_key}/{table_name}/*.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(
            f"SELECT * FROM read_parquet('{pattern}') {sql_suffix}"
        ).fetchall()
    finally:
        conn.close()


def _count_parquet(data_dir, scope_key, table_name):
    """Helper to count rows in parquet files."""
    pattern = f"{data_dir}/{scope_key}/{table_name}/*.parquet"
    conn = duckdb.connect()
    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{pattern}')"
        ).fetchone()
        return result[0] if result is not None else 0
    except Exception:
        return 0
    finally:
        conn.close()


class TestParquetWriter:
    """Test ParquetWriter class."""

    def test_write_creates_parquet_files(self, writer, temp_dir):
        """Writing a batch creates parquet files in the expected directory."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-001",
                    "pid": 123,
                    "town_name": "Test Town",
                }
            }
        ]
        writer.write_batch(result)

        parquet_dir = Path(temp_dir) / "testcity" / "properties"
        assert parquet_dir.exists()
        assert len(list(parquet_dir.glob("*.parquet"))) == 1

    def test_write_property_data(self, writer, temp_dir):
        """Property data is queryable from parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-001",
                    "pid": 123,
                    "town_name": "Test Town",
                    "account_number": "ACC123",
                    "address": "123 Main St",
                }
            }
        ]
        writer.write_batch(result)

        rows = _query_parquet(
            temp_dir, "testcity", "properties", "WHERE pid = 123"
        )
        assert len(rows) == 1

    def test_write_complete_property(self, writer, temp_dir):
        """All property fields are written to parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-002",
                    "pid": 456,
                    "account_number": "ACC456",
                    "town_name": "Test Town",
                    "address": "456 Oak Ave",
                    "owner": "John Doe",
                    "sale_price": 350000.0,
                    "assessment_value": 300000.0,
                    "building_count": 2,
                    "land_size_acres": 1.5,
                }
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT owner, sale_price, land_size_acres, building_count "
                f"FROM read_parquet('{temp_dir}/testcity/properties/*.parquet') "
                f"WHERE pid = 456"
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == "John Doe"
        assert row[1] == 350000.0
        assert row[2] == 1.5
        assert row[3] == 2

    def test_write_buildings(self, writer, temp_dir):
        """Building data is written to parquet with construction flattened."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-004",
                    "pid": 100,
                    "town_name": "Test Town",
                },
                "buildings": [
                    {
                        "property_uuid": "test-uuid-004",
                        "pid": 100,
                        "bid": 0,
                        "year_built": 1950,
                        "building_area": 2000.0,
                        "construction": {
                            "style": "Colonial",
                            "grade": "Good",
                            "stories": "2",
                        },
                        "sub_areas": [],
                    },
                    {
                        "property_uuid": "test-uuid-004",
                        "pid": 100,
                        "bid": 1,
                        "year_built": 1990,
                        "building_area": 500.0,
                        "construction": {},
                        "sub_areas": [],
                    },
                ],
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            rows = conn.execute(
                f"SELECT bid, year_built, building_area, style, grade "
                f"FROM read_parquet('{temp_dir}/testcity/buildings/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-004' ORDER BY bid"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2
        assert rows[0][0] == 0
        assert rows[0][1] == 1950
        assert rows[0][2] == 2000.0
        assert rows[0][3] == "Colonial"
        assert rows[0][4] == "Good"
        assert rows[1][0] == 1
        assert rows[1][1] == 1990

    def test_write_sub_areas(self, writer, temp_dir):
        """Sub-area data is written to parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-005",
                    "pid": 101,
                    "town_name": "Test Town",
                },
                "buildings": [
                    {
                        "property_uuid": "test-uuid-005",
                        "pid": 101,
                        "bid": 0,
                        "year_built": 1960,
                        "construction": {},
                        "sub_areas": [
                            {
                                "code": "BLA",
                                "description": "Basement Living Area",
                                "gross_area": 1000.0,
                                "living_area": 800.0,
                            },
                            {
                                "code": "FLA",
                                "description": "First Floor Living Area",
                                "gross_area": 1200.0,
                                "living_area": 1200.0,
                            },
                        ],
                    }
                ],
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            rows = conn.execute(
                f"SELECT code, gross_area, living_area "
                f"FROM read_parquet('{temp_dir}/testcity/sub_areas/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-005' ORDER BY code"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2
        assert rows[0][0] == "BLA"
        assert rows[0][1] == 1000.0
        assert rows[1][0] == "FLA"
        assert rows[1][2] == 1200.0

    def test_write_ownership(self, writer, temp_dir):
        """Ownership data is written to parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-006",
                    "pid": 102,
                    "town_name": "Test Town",
                },
                "ownership": [
                    {
                        "property_uuid": "test-uuid-006",
                        "pid": 102,
                        "owner": "John Smith",
                        "sale_price": 300000.0,
                        "sale_date": "2020-05-15",
                    },
                    {
                        "property_uuid": "test-uuid-006",
                        "pid": 102,
                        "owner": "Jane Smith",
                        "sale_price": 250000.0,
                        "sale_date": "2015-03-20",
                    },
                ],
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            rows = conn.execute(
                f"SELECT owner, sale_price "
                f"FROM read_parquet('{temp_dir}/testcity/ownership/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-006' ORDER BY sale_date DESC"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2
        assert rows[0][0] == "John Smith"
        assert rows[0][1] == 300000.0

    def test_write_appraisals_and_assessments(self, writer, temp_dir):
        """Appraisal and assessment data is written to parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-007",
                    "pid": 103,
                    "town_name": "Test Town",
                },
                "appraisals": [
                    {
                        "property_uuid": "test-uuid-007",
                        "pid": 103,
                        "valuation_year": "2023",
                        "total": 300000.0,
                    },
                ],
                "assessments": [
                    {
                        "property_uuid": "test-uuid-007",
                        "pid": 103,
                        "valuation_year": "2023",
                        "total": 210000.0,
                    },
                ],
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            appr = conn.execute(
                f"SELECT total FROM read_parquet('{temp_dir}/testcity/appraisals/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-007'"
            ).fetchone()
            assess = conn.execute(
                f"SELECT total FROM read_parquet('{temp_dir}/testcity/assessments/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-007'"
            ).fetchone()
        finally:
            conn.close()

        assert appr is not None
        assert appr[0] == 300000.0
        assert assess is not None
        assert assess[0] == 210000.0

    def test_write_extra_features_and_outbuildings(self, writer, temp_dir):
        """Extra features and outbuildings are written to parquet."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-008",
                    "pid": 104,
                    "town_name": "Test Town",
                },
                "extra_features": [
                    {
                        "property_uuid": "test-uuid-008",
                        "pid": 104,
                        "code": "POOL",
                        "description": "In-ground Pool",
                        "value": 25000.0,
                    },
                ],
                "outbuildings": [
                    {
                        "property_uuid": "test-uuid-008",
                        "pid": 104,
                        "code": "GAR",
                        "description": "Garage",
                        "sub_code": "DET",
                        "value": 15000.0,
                    },
                ],
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            ef = conn.execute(
                f"SELECT code, value FROM read_parquet('{temp_dir}/testcity/extra_features/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-008'"
            ).fetchone()
            ob = conn.execute(
                f"SELECT code, sub_code, value FROM read_parquet('{temp_dir}/testcity/outbuildings/*.parquet') "
                f"WHERE property_uuid = 'test-uuid-008'"
            ).fetchone()
        finally:
            conn.close()

        assert ef is not None
        assert ef[0] == "POOL"
        assert ef[1] == 25000.0
        assert ob is not None
        assert ob[0] == "GAR"
        assert ob[1] == "DET"
        assert ob[2] == 15000.0

    def test_batch_write_multiple_properties(self, writer, temp_dir):
        """Multiple properties in a single batch write."""
        batch = []
        for i in range(20):
            batch.append(
                {
                    "property": {
                        "uuid": f"test-uuid-{i:03d}",
                        "pid": 600 + i,
                        "town_name": "Test Town",
                        "address": f"{i} Test St",
                    }
                }
            )
        writer.write_batch(batch)

        count = _count_parquet(temp_dir, "testcity", "properties")
        assert count == 20

    def test_rows_have_scraped_at_and_hash(self, writer, temp_dir):
        """Each row gets scraped_at timestamp and row_hash."""
        result = [
            {
                "property": {
                    "uuid": "test-uuid-meta",
                    "pid": 999,
                    "town_name": "Test Town",
                }
            }
        ]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT scraped_at, row_hash "
                f"FROM read_parquet('{temp_dir}/testcity/properties/*.parquet') "
                f"WHERE pid = 999"
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] is not None  # scraped_at
        assert row[1] is not None  # row_hash
        assert len(row[1]) == 32  # MD5 hex

    def test_checkpoint_save_and_load(self, writer):
        """Test checkpoint save and retrieval."""
        writer.save_checkpoint("testcity", last_entry_id=500, total_scraped=450)

        last_entry_id, total = writer.get_last_checkpoint("testcity")
        assert last_entry_id == "500"
        assert total == 450

        writer.save_checkpoint("testcity", last_entry_id=1000, total_scraped=950)

        last_entry_id, total = writer.get_last_checkpoint("testcity")
        assert last_entry_id == "1000"
        assert total == 950

    def test_checkpoint_no_data(self, writer):
        """Test checkpoint retrieval when no checkpoint exists."""
        last_entry_id, total = writer.get_last_checkpoint("nonexistent_city")
        assert last_entry_id is None
        assert total == 0


class TestQueryTimeVersioning:
    """Test query-time SCD2 versioning over parquet files."""

    def test_append_only_creates_multiple_rows(self, writer, temp_dir):
        """Writing same UUID twice creates two rows (append-only)."""
        for value in [200000.0, 250000.0]:
            writer.write_batch(
                [
                    {
                        "property": {
                            "uuid": "test-uuid-ver",
                            "pid": 789,
                            "town_name": "Test Town",
                            "assessment_value": value,
                        }
                    }
                ]
            )

        count = _count_parquet(temp_dir, "testcity", "properties")
        assert count == 2

    def test_get_property_history(self, writer, temp_dir):
        """get_property_history derives versions via window functions."""
        for value in [150000.0, 160000.0, 170000.0]:
            writer.write_batch(
                [
                    {
                        "property": {
                            "uuid": "test-hist",
                            "pid": 1005,
                            "town_name": "Test Town",
                            "assessment_value": value,
                        }
                    }
                ]
            )

        history = get_property_history(temp_dir, "testcity", "test-hist")
        assert len(history) == 3
        assert list(history["version"]) == [1, 2, 3]

    def test_unchanged_data_still_appends(self, writer, temp_dir):
        """Identical data creates multiple rows (versioning filters at query time)."""
        for _ in range(2):
            writer.write_batch(
                [
                    {
                        "property": {
                            "uuid": "test-same",
                            "pid": 1003,
                            "town_name": "Test Town",
                            "assessment_value": 180000.0,
                        }
                    }
                ]
            )

        # Raw count: 2 rows appended
        count = _count_parquet(temp_dir, "testcity", "properties")
        assert count == 2

        # But get_property_history deduplicates by hash — only 1 unique version
        history = get_property_history(temp_dir, "testcity", "test-same")
        assert len(history) == 1

    def test_current_state_via_row_number(self, writer, temp_dir):
        """ROW_NUMBER query returns only the latest version per UUID."""
        for value in [100000.0, 110000.0, 120000.0]:
            writer.write_batch(
                [
                    {
                        "property": {
                            "uuid": "test-current",
                            "pid": 1004,
                            "town_name": "Test Town",
                            "assessment_value": value,
                        }
                    }
                ]
            )

        pattern = f"{temp_dir}/testcity/properties/*.parquet"
        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"""
                SELECT assessment_value FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY uuid ORDER BY scraped_at DESC
                    ) AS rn
                    FROM read_parquet('{pattern}')
                    WHERE uuid = 'test-current'
                ) WHERE rn = 1
                """
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == 120000.0


class TestHashFunction:
    """Test the shared compute_row_hash function."""

    def test_hash_excludes_metadata(self):
        """Metadata fields are excluded from hash computation."""
        data1 = {"uuid": "001", "pid": 1, "assessment_value": 100000.0}
        data2 = {
            "uuid": "001",
            "pid": 1,
            "assessment_value": 100000.0,
            "scraped_at": "2026-01-01",
            "row_hash": "abc",
            "city_id": 999,
        }
        assert compute_row_hash(data1) == compute_row_hash(data2)

    def test_hash_changes_on_data_change(self):
        """Hash changes when actual data changes."""
        data1 = {"pid": 1, "assessment_value": 100000.0}
        data2 = {"pid": 1, "assessment_value": 200000.0}
        assert compute_row_hash(data1) != compute_row_hash(data2)


class TestRefreshHelpers:
    """Tests for get_known_entry_ids and get_changed_properties."""

    def _make_result(self, uuid, pid, assessment_value=100000.0):
        return {
            "property": {
                "uuid": uuid,
                "pid": pid,
                "town_name": "Test Town",
                "assessment_value": assessment_value,
            },
        }

    def test_get_known_entry_ids_empty(self, temp_dir):
        """Returns empty list when no parquet files exist."""
        entry_ids = get_known_entry_ids(temp_dir, "testcity")
        assert entry_ids == []

    def test_get_known_entry_ids_returns_all_pids(self, writer, temp_dir):
        """Returns all distinct PIDs sorted."""
        for pid in [10, 5, 1, 20]:
            writer.write_batch([self._make_result(f"uuid-{pid}", pid)])

        entry_ids = get_known_entry_ids(temp_dir, "testcity")
        assert entry_ids == [1, 5, 10, 20]

    def test_get_known_entry_ids_deduplicates(self, writer, temp_dir):
        """Each PID appears once even after multiple writes."""
        writer.write_batch([self._make_result("uuid-42", 42, 100000.0)])
        writer.write_batch([self._make_result("uuid-42", 42, 200000.0)])

        entry_ids = get_known_entry_ids(temp_dir, "testcity")
        assert entry_ids.count(42) == 1

    def test_get_changed_properties_empty(self, temp_dir):
        """Returns empty DataFrame when no parquet files exist."""
        from datetime import datetime

        changed = get_changed_properties(temp_dir, "testcity", since=datetime.now())
        assert len(changed) == 0

    def test_get_changed_properties_detects_change(self, writer, temp_dir):
        """Returns properties whose hash changed after a cutoff time."""
        from datetime import datetime

        writer.write_batch([self._make_result("uuid-chg", 99, 100000.0)])
        cutoff = datetime.now()
        time.sleep(0.05)
        writer.write_batch([self._make_result("uuid-chg", 99, 200000.0)])

        changed = get_changed_properties(temp_dir, "testcity", since=cutoff)
        assert len(changed) == 1

    def test_get_changed_properties_ignores_first_version(self, writer, temp_dir):
        """First-time inserts are not returned as changes."""
        from datetime import datetime

        cutoff = datetime.now()
        writer.write_batch([self._make_result("uuid-new", 77, 100000.0)])

        changed = get_changed_properties(temp_dir, "testcity", since=cutoff)
        assert len(changed) == 0

    def test_get_changed_properties_unchanged_not_returned(self, writer, temp_dir):
        """Properties re-scraped with identical data don't appear."""
        from datetime import datetime

        writer.write_batch([self._make_result("uuid-same", 55, 100000.0)])
        cutoff = datetime.now()
        time.sleep(0.05)
        writer.write_batch([self._make_result("uuid-same", 55, 100000.0)])

        changed = get_changed_properties(temp_dir, "testcity", since=cutoff)
        assert len(changed) == 0
