"""
Tests for DuckDB database operations.

Tests schema creation, data writing, querying, and checkpoints.
"""

import tempfile
from pathlib import Path

import duckdb
import pytest

from src.vgsi.database import DuckDBWriter


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    # Create a temporary directory and file path (don't create the file yet)
    temp_dir = tempfile.mkdtemp()
    db_path = str(Path(temp_dir) / "test.duckdb")
    yield db_path
    # Cleanup
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def db_writer(temp_db):
    """Create a DuckDBWriter instance for testing."""
    writer = DuckDBWriter("testcity", temp_db)
    yield writer
    writer.close()


class TestDuckDBWriter:
    """Test DuckDBWriter class."""

    def test_init_creates_schema(self, temp_db):
        """Test that initialization creates the schema."""
        writer = DuckDBWriter("testcity", temp_db)

        # Check that schema exists
        result = writer.conn.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'testcity'"
        ).fetchone()
        assert result is not None

        # Check that tables exist
        tables = [
            "properties",
            "buildings",
            "sub_areas",
            "ownership",
            "appraisals",
            "assessments",
            "extra_features",
            "outbuildings",
        ]
        for table in tables:
            result = writer.conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = 'testcity' AND table_name = '{table}'"
            ).fetchone()
            assert result is not None, f"Table {table} not found"

        writer.close()

    def test_global_tables_created(self, temp_db):
        """Test that global tables (cities, checkpoints) are created."""
        writer = DuckDBWriter("testcity", temp_db)

        # Check cities table
        result = writer.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'cities'"
        ).fetchone()
        assert result is not None

        # Check checkpoints table
        result = writer.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'scrape_checkpoints'"
        ).fetchone()
        assert result is not None

        writer.close()

    def test_write_property_only(self, db_writer):
        """Test writing a minimal property record."""
        property_data = {
            "uuid": "test-uuid-001",
            "pid": 123,
            "town_name": "Test Town",
            "account_number": "ACC123",
            "address": "123 Main St",
        }

        result = [{"property": property_data}]
        db_writer.write_batch(result)

        # Verify data was written
        row = db_writer.conn.execute(
            "SELECT uuid, pid, town_name FROM testcity.properties WHERE pid = 123"
        ).fetchone()

        assert row is not None
        assert row[0] == "test-uuid-001"
        assert row[1] == 123
        assert row[2] == "Test Town"

    def test_write_complete_property(self, db_writer):
        """Test writing a property with all fields."""
        property_data = {
            "uuid": "test-uuid-002",
            "pid": 456,
            "account_number": "ACC456",
            "mblu": "MB123",
            "town_name": "Test Town",
            "address": "456 Oak Ave",
            "owner": "John Doe",
            "co_owner": "Jane Doe",
            "owner_address": "789 Elm St",
            "sale_price": 350000.0,
            "sale_date": "2024-01-15",
            "assessment_value": 300000.0,
            "appraisal_value": 320000.0,
            "building_count": 2,
            "building_use": "Residential",
            "land_use_code": "R1",
            "land_size_acres": 1.5,
            "land_frontage": 100.0,
            "land_depth": 200.0,
            "zip_code": "06511",
        }

        result = [{"property": property_data}]
        db_writer.write_batch(result)

        # Verify data
        row = db_writer.conn.execute(
            "SELECT owner, sale_price, land_size_acres, building_count "
            "FROM testcity.properties WHERE pid = 456"
        ).fetchone()

        assert row[0] == "John Doe"
        assert row[1] == 350000.0
        assert row[2] == 1.5
        assert row[3] == 2

    def test_upsert_property(self, db_writer):
        """Test that property upsert creates versions (SCD Type 2)."""
        property_v1 = {
            "uuid": "test-uuid-003",
            "pid": 789,
            "town_name": "Test Town",
            "assessment_value": 200000.0,
        }

        # Insert first version
        db_writer.write_batch([{"property": property_v1}])

        # Update with new assessment value
        property_v2 = {
            "uuid": "test-uuid-003",
            "pid": 789,
            "town_name": "Test Town",
            "assessment_value": 250000.0,
        }
        db_writer.write_batch([{"property": property_v2}])

        # With SCD Type 2, should have 2 versions
        all_rows = db_writer.conn.execute(
            "SELECT assessment_value, version, is_current FROM testcity.properties WHERE uuid = 'test-uuid-003' ORDER BY version"
        ).fetchall()

        assert len(all_rows) == 2
        assert all_rows[0][0] == 200000.0  # version 1
        assert all_rows[1][0] == 250000.0  # version 2
        assert all_rows[1][2] is True  # version 2 is current

        # Current view should show only the latest version
        current_rows = db_writer.conn.execute(
            "SELECT assessment_value FROM testcity.properties_current WHERE uuid = 'test-uuid-003'"
        ).fetchall()

        assert len(current_rows) == 1
        assert current_rows[0][0] == 250000.0

    def test_write_buildings(self, db_writer):
        """Test writing building records."""
        property_data = {
            "uuid": "test-uuid-004",
            "pid": 100,
            "town_name": "Test Town",
        }

        building1 = {
            "property_uuid": "test-uuid-004",
            "pid": 100,
            "bid": 0,
            "year_built": 1950,
            "building_area": 2000.0,
            "replacement_cost": 400000.0,
            "construction": {
                "style": "Colonial",
                "grade": "Good",
                "stories": "2",
            },
            "sub_areas": [],
        }

        building2 = {
            "property_uuid": "test-uuid-004",
            "pid": 100,
            "bid": 1,
            "year_built": 1990,
            "building_area": 500.0,
            "construction": {},
            "sub_areas": [],
        }

        result = [
            {
                "property": property_data,
                "buildings": [building1, building2],
            }
        ]

        db_writer.write_batch(result)

        # Verify buildings were written
        rows = db_writer.conn.execute(
            "SELECT bid, year_built, building_area, style, grade "
            "FROM testcity.buildings WHERE property_uuid = 'test-uuid-004' "
            "ORDER BY bid"
        ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 0  # bid
        assert rows[0][1] == 1950  # year_built
        assert rows[0][2] == 2000.0  # building_area
        assert rows[0][3] == "Colonial"  # style
        assert rows[0][4] == "Good"  # grade

        assert rows[1][0] == 1  # second building
        assert rows[1][1] == 1990

    def test_write_sub_areas(self, db_writer):
        """Test writing sub-areas for buildings."""
        property_data = {
            "uuid": "test-uuid-005",
            "pid": 101,
            "town_name": "Test Town",
        }

        building = {
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

        result = [{"property": property_data, "buildings": [building]}]
        db_writer.write_batch(result)

        # Verify sub-areas
        rows = db_writer.conn.execute(
            "SELECT code, description, gross_area, living_area "
            "FROM testcity.sub_areas WHERE property_uuid = 'test-uuid-005' "
            "ORDER BY code"
        ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "BLA"
        assert rows[0][2] == 1000.0
        assert rows[1][0] == "FLA"
        assert rows[1][3] == 1200.0

    def test_write_ownership(self, db_writer):
        """Test writing ownership/sales history."""
        property_data = {
            "uuid": "test-uuid-006",
            "pid": 102,
            "town_name": "Test Town",
        }

        ownership = [
            {
                "property_uuid": "test-uuid-006",
                "pid": 102,
                "owner": "John Smith",
                "sale_price": 300000.0,
                "sale_date": "2020-05-15",
                "book_and_page": "123/456",
            },
            {
                "property_uuid": "test-uuid-006",
                "pid": 102,
                "owner": "Jane Smith",
                "sale_price": 250000.0,
                "sale_date": "2015-03-20",
            },
        ]

        result = [{"property": property_data, "ownership": ownership}]
        db_writer.write_batch(result)

        # Verify ownership records
        rows = db_writer.conn.execute(
            "SELECT owner, sale_price, sale_date "
            "FROM testcity.ownership WHERE property_uuid = 'test-uuid-006' "
            "ORDER BY sale_date DESC"
        ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "John Smith"
        assert rows[0][1] == 300000.0
        assert rows[1][0] == "Jane Smith"

    def test_write_appraisals_and_assessments(self, db_writer):
        """Test writing appraisal and assessment history."""
        property_data = {
            "uuid": "test-uuid-007",
            "pid": 103,
            "town_name": "Test Town",
        }

        appraisals = [
            {
                "property_uuid": "test-uuid-007",
                "pid": 103,
                "valuation_year": "2023",
                "improvements": 200000.0,
                "land": 100000.0,
                "total": 300000.0,
            },
        ]

        assessments = [
            {
                "property_uuid": "test-uuid-007",
                "pid": 103,
                "valuation_year": "2023",
                "improvements": 140000.0,
                "land": 70000.0,
                "total": 210000.0,
            },
        ]

        result = [
            {
                "property": property_data,
                "appraisals": appraisals,
                "assessments": assessments,
            }
        ]

        db_writer.write_batch(result)

        # Verify appraisals
        row = db_writer.conn.execute(
            "SELECT total FROM testcity.appraisals WHERE property_uuid = 'test-uuid-007'"
        ).fetchone()
        assert row[0] == 300000.0

        # Verify assessments
        row = db_writer.conn.execute(
            "SELECT total FROM testcity.assessments WHERE property_uuid = 'test-uuid-007'"
        ).fetchone()
        assert row[0] == 210000.0

    def test_write_extra_features_and_outbuildings(self, db_writer):
        """Test writing extra features and outbuildings."""
        property_data = {
            "uuid": "test-uuid-008",
            "pid": 104,
            "town_name": "Test Town",
        }

        extra_features = [
            {
                "property_uuid": "test-uuid-008",
                "pid": 104,
                "code": "POOL",
                "description": "In-ground Pool",
                "size": "20x40",
                "value": 25000.0,
            },
        ]

        outbuildings = [
            {
                "property_uuid": "test-uuid-008",
                "pid": 104,
                "code": "GAR",
                "description": "Garage",
                "sub_code": "DET",
                "sub_description": "Detached",
                "size": "24x24",
                "value": 15000.0,
            },
        ]

        result = [
            {
                "property": property_data,
                "extra_features": extra_features,
                "outbuildings": outbuildings,
            }
        ]

        db_writer.write_batch(result)

        # Verify extra features
        row = db_writer.conn.execute(
            "SELECT code, value FROM testcity.extra_features "
            "WHERE property_uuid = 'test-uuid-008'"
        ).fetchone()
        assert row[0] == "POOL"
        assert row[1] == 25000.0

        # Verify outbuildings
        row = db_writer.conn.execute(
            "SELECT code, sub_code, value FROM testcity.outbuildings "
            "WHERE property_uuid = 'test-uuid-008'"
        ).fetchone()
        assert row[0] == "GAR"
        assert row[1] == "DET"
        assert row[2] == 15000.0

    def test_checkpoint_save_and_load(self, db_writer):
        """Test checkpoint save and retrieval."""
        # Save checkpoint
        db_writer.save_checkpoint("testcity", last_pid=500, total_scraped=450)

        # Retrieve checkpoint
        last_pid, total = db_writer.get_last_checkpoint("testcity")

        assert last_pid == 500
        assert total == 450

        # Save another checkpoint
        db_writer.save_checkpoint("testcity", last_pid=1000, total_scraped=950)

        # Should get the latest one
        last_pid, total = db_writer.get_last_checkpoint("testcity")

        assert last_pid == 1000
        assert total == 950

    def test_checkpoint_no_data(self, db_writer):
        """Test checkpoint retrieval when no checkpoint exists."""
        last_pid, total = db_writer.get_last_checkpoint("nonexistent_city")

        assert last_pid == 0
        assert total == 0

    def test_store_cities(self, db_writer):
        """Test storing city information."""
        cities_dict = {
            "newhaven": {
                "city_name": "New Haven",
                "state": "ct",
                "url": "https://gis.vgsi.com/newhavenct/",
                "type": "vgsi",
            },
            "hartford": {
                "city_name": "Hartford",
                "state": "ct",
                "url": "https://gis.vgsi.com/hartfordct/",
                "type": "vgsi",
            },
        }

        count = db_writer.store_cities(cities_dict)

        assert count == 2

        # Verify cities were stored
        rows = db_writer.conn.execute(
            "SELECT city_key, city_name, url FROM main.cities ORDER BY city_key"
        ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "hartford"
        assert rows[0][1] == "Hartford"
        assert rows[1][0] == "newhaven"

    def test_get_city_url(self, db_writer):
        """Test retrieving city URL."""
        cities_dict = {
            "newhaven": {
                "city_name": "New Haven",
                "state": "ct",
                "url": "https://gis.vgsi.com/newhavenct/",
                "type": "vgsi",
            },
        }

        db_writer.store_cities(cities_dict)

        url = db_writer.get_city_url("newhaven")
        assert url == "https://gis.vgsi.com/newhavenct/"

        # Non-existent city
        url = db_writer.get_city_url("nonexistent")
        assert url is None

    def test_batch_write_multiple_properties(self, db_writer):
        """Test writing multiple properties in a single batch."""
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

        db_writer.write_batch(batch)

        # Verify all properties were written
        count = db_writer.conn.execute(
            "SELECT COUNT(*) FROM testcity.properties WHERE pid >= 600 AND pid < 620"
        ).fetchone()[0]

        assert count == 20

    def test_indexes_created(self, db_writer):
        """Test that indexes were created on key columns."""
        # Check for property PID index
        indexes = db_writer.conn.execute(
            "SELECT index_name FROM duckdb_indexes() "
            "WHERE table_name = 'properties' AND schema_name = 'testcity'"
        ).fetchall()

        # Should have at least the PID index
        index_names = [idx[0] for idx in indexes]
        assert any("pid" in name.lower() for name in index_names)

    def test_foreign_key_relationships(self, db_writer):
        """Test that foreign key relationships work correctly."""
        # Write property
        property_data = {
            "uuid": "test-uuid-fk-001",
            "pid": 500,
            "town_name": "Test Town",
        }

        building = {
            "property_uuid": "test-uuid-fk-001",
            "pid": 500,
            "bid": 0,
            "year_built": 2000,
            "construction": {},
            "sub_areas": [],
        }

        db_writer.write_batch([{"property": property_data, "buildings": [building]}])

        # Query with JOIN
        result = db_writer.conn.execute(
            """
            SELECT p.address, b.year_built
            FROM testcity.properties p
            JOIN testcity.buildings b ON p.uuid = b.property_uuid
            WHERE p.uuid = 'test-uuid-fk-001'
            """
        ).fetchone()

        assert result is not None
        assert result[1] == 2000


class TestSCDType2:
    """Test SCD Type 2 historical tracking."""

    def test_initial_version_is_current(self, db_writer):
        """First insert creates version 1 marked as current."""
        property_data = {
            "uuid": "test-scd-001",
            "pid": 1001,
            "account_number": "ACC1001",
            "town_name": "Test Town",
            "owner": "John Doe",
            "assessment_value": 250000.0,
        }

        result = [
            {
                "property": property_data,
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }
        ]
        db_writer.write_batch(result)

        # Check version 1 is current
        row = db_writer.conn.execute("""
            SELECT version, is_current, effective_from, effective_to
            FROM testcity.properties
            WHERE uuid = 'test-scd-001'
        """).fetchone()

        assert row is not None
        assert row[0] == 1  # version
        assert row[1] is True  # is_current
        assert row[2] is not None  # effective_from
        assert row[3] is None  # effective_to

    def test_data_change_creates_new_version(self, db_writer):
        """Changing data closes old version and creates new one."""
        # Insert version 1
        property_v1 = {
            "uuid": "test-scd-002",
            "pid": 1002,
            "account_number": "ACC1002",
            "town_name": "Test Town",
            "owner": "Jane Smith",
            "assessment_value": 200000.0,
        }

        result1 = [
            {
                "property": property_v1,
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }
        ]
        db_writer.write_batch(result1)

        # Update assessment value (version 2)
        property_v2 = property_v1.copy()
        property_v2["assessment_value"] = 225000.0

        result2 = [
            {
                "property": property_v2,
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }
        ]
        db_writer.write_batch(result2)

        # Check we have 2 versions
        rows = db_writer.conn.execute("""
            SELECT version, is_current, assessment_value, effective_to
            FROM testcity.properties
            WHERE uuid = 'test-scd-002'
            ORDER BY version
        """).fetchall()

        assert len(rows) == 2

        # Version 1 should be expired
        assert rows[0][0] == 1
        assert rows[0][1] is False
        assert rows[0][2] == 200000.0
        assert rows[0][3] is not None  # has end date

        # Version 2 should be current
        assert rows[1][0] == 2
        assert rows[1][1] is True
        assert rows[1][2] == 225000.0
        assert rows[1][3] is None  # no end date

    def test_no_change_preserves_version(self, db_writer):
        """Identical data doesn't create new version."""
        property_data = {
            "uuid": "test-scd-003",
            "pid": 1003,
            "account_number": "ACC1003",
            "town_name": "Test Town",
            "owner": "Bob Johnson",
            "assessment_value": 180000.0,
        }

        result = [
            {
                "property": property_data,
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }
        ]

        # Write twice with same data
        db_writer.write_batch(result)
        db_writer.write_batch(result)

        # Should still have only 1 version
        count = db_writer.conn.execute("""
            SELECT COUNT(*) FROM testcity.properties WHERE uuid = 'test-scd-003'
        """).fetchone()[0]

        assert count == 1

    def test_current_view_shows_latest_only(self, db_writer):
        """Current view excludes historical versions."""
        # Create property with multiple versions
        property_base = {
            "uuid": "test-scd-004",
            "pid": 1004,
            "account_number": "ACC1004",
            "town_name": "Test Town",
            "owner": "Alice Brown",
        }

        # Version 1
        prop_v1 = property_base.copy()
        prop_v1["assessment_value"] = 100000.0
        db_writer.write_batch(
            [
                {
                    "property": prop_v1,
                    "buildings": [],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Version 2
        prop_v2 = property_base.copy()
        prop_v2["assessment_value"] = 110000.0
        db_writer.write_batch(
            [
                {
                    "property": prop_v2,
                    "buildings": [],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Version 3
        prop_v3 = property_base.copy()
        prop_v3["assessment_value"] = 120000.0
        db_writer.write_batch(
            [
                {
                    "property": prop_v3,
                    "buildings": [],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Base table should have 3 versions
        total_count = db_writer.conn.execute("""
            SELECT COUNT(*) FROM testcity.properties WHERE uuid = 'test-scd-004'
        """).fetchone()[0]
        assert total_count == 3

        # Current view should have only 1
        current_count = db_writer.conn.execute("""
            SELECT COUNT(*) FROM testcity.properties_current WHERE uuid = 'test-scd-004'
        """).fetchone()[0]
        assert current_count == 1

        # Current view should show version 3
        current_value = db_writer.conn.execute("""
            SELECT assessment_value FROM testcity.properties_current WHERE uuid = 'test-scd-004'
        """).fetchone()[0]
        assert current_value == 120000.0

    def test_property_history_query(self, db_writer):
        """Can retrieve all versions of a property."""
        property_base = {
            "uuid": "test-scd-005",
            "pid": 1005,
            "account_number": "ACC1005",
            "town_name": "Test Town",
        }

        # Create 3 versions
        for i, value in enumerate([150000.0, 160000.0, 170000.0], 1):
            prop = property_base.copy()
            prop["assessment_value"] = value
            db_writer.write_batch(
                [
                    {
                        "property": prop,
                        "buildings": [],
                        "ownership": [],
                        "appraisals": [],
                        "assessments": [],
                        "extra_features": [],
                        "outbuildings": [],
                    }
                ]
            )

        # Query history
        history = db_writer.get_property_history("test-scd-005")

        assert len(history) == 3
        assert list(history["version"]) == [1, 2, 3]
        assert list(history["assessment_value"]) == [150000.0, 160000.0, 170000.0]
        assert (
            history["is_current"].iloc[-1] == True
        )  # Use == for numpy bool comparison
        assert all(~history["is_current"].iloc[:-1])

    def test_building_versioning(self, db_writer):
        """Buildings track changes via SCD Type 2."""
        property_data = {
            "uuid": "test-scd-006",
            "pid": 1006,
            "account_number": "ACC1006",
            "town_name": "Test Town",
        }

        # Version 1 of building
        building_v1 = {
            "property_uuid": "test-scd-006",
            "pid": 1006,
            "bid": 0,
            "year_built": 1950,
            "building_area": 2000.0,
            "construction": {},
            "sub_areas": [],
        }

        db_writer.write_batch(
            [
                {
                    "property": property_data,
                    "buildings": [building_v1],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Version 2 - change building area
        building_v2 = building_v1.copy()
        building_v2["building_area"] = 2200.0

        db_writer.write_batch(
            [
                {
                    "property": property_data,
                    "buildings": [building_v2],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Should have 2 versions
        rows = db_writer.conn.execute("""
            SELECT version, is_current, building_area
            FROM testcity.buildings
            WHERE property_uuid = 'test-scd-006' AND bid = 0
            ORDER BY version
        """).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] is False
        assert rows[0][2] == 2000.0
        assert rows[1][0] == 2
        assert rows[1][1] is True
        assert rows[1][2] == 2200.0

    def test_version_numbering(self, db_writer):
        """Versions increment correctly (1, 2, 3...)."""
        property_base = {
            "uuid": "test-scd-007",
            "pid": 1007,
            "account_number": "ACC1007",
            "town_name": "Test Town",
        }

        # Create 5 versions
        for i in range(5):
            prop = property_base.copy()
            prop["assessment_value"] = 100000.0 + (i * 10000.0)
            db_writer.write_batch(
                [
                    {
                        "property": prop,
                        "buildings": [],
                        "ownership": [],
                        "appraisals": [],
                        "assessments": [],
                        "extra_features": [],
                        "outbuildings": [],
                    }
                ]
            )

        versions = db_writer.conn.execute("""
            SELECT version FROM testcity.properties
            WHERE uuid = 'test-scd-007'
            ORDER BY version
        """).fetchall()

        assert len(versions) == 5
        assert [v[0] for v in versions] == [1, 2, 3, 4, 5]

    def test_effective_dates(self, db_writer):
        """effective_from and effective_to track correctly."""
        import time

        property_base = {
            "uuid": "test-scd-008",
            "pid": 1008,
            "account_number": "ACC1008",
            "town_name": "Test Town",
        }

        # Version 1
        prop1 = property_base.copy()
        prop1["assessment_value"] = 100000.0
        db_writer.write_batch(
            [
                {
                    "property": prop1,
                    "buildings": [],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        time.sleep(0.1)  # Ensure different timestamps

        # Version 2
        prop2 = property_base.copy()
        prop2["assessment_value"] = 110000.0
        db_writer.write_batch(
            [
                {
                    "property": prop2,
                    "buildings": [],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        rows = db_writer.conn.execute("""
            SELECT version, effective_from, effective_to
            FROM testcity.properties
            WHERE uuid = 'test-scd-008'
            ORDER BY version
        """).fetchall()

        # Version 1 should have end date
        assert rows[0][1] is not None  # effective_from
        assert rows[0][2] is not None  # effective_to

        # Version 2 should not have end date
        assert rows[1][1] is not None  # effective_from
        assert rows[1][2] is None  # effective_to

        # Version 1's effective_to should equal version 2's effective_from
        assert rows[0][2] == rows[1][1]

    def test_is_current_flag(self, db_writer):
        """Only latest version has is_current=TRUE."""
        property_base = {
            "uuid": "test-scd-009",
            "pid": 1009,
            "account_number": "ACC1009",
            "town_name": "Test Town",
        }

        # Create 3 versions
        for i in range(3):
            prop = property_base.copy()
            prop["assessment_value"] = 100000.0 + (i * 10000.0)
            db_writer.write_batch(
                [
                    {
                        "property": prop,
                        "buildings": [],
                        "ownership": [],
                        "appraisals": [],
                        "assessments": [],
                        "extra_features": [],
                        "outbuildings": [],
                    }
                ]
            )

        current_rows = db_writer.conn.execute("""
            SELECT version FROM testcity.properties
            WHERE uuid = 'test-scd-009' AND is_current = TRUE
        """).fetchall()

        # Only version 3 should be current
        assert len(current_rows) == 1
        assert current_rows[0][0] == 3

        # Versions 1 and 2 should not be current
        non_current_rows = db_writer.conn.execute("""
            SELECT version FROM testcity.properties
            WHERE uuid = 'test-scd-009' AND is_current = FALSE
            ORDER BY version
        """).fetchall()

        assert len(non_current_rows) == 2
        assert [v[0] for v in non_current_rows] == [1, 2]

    def test_sub_area_versioning(self, db_writer):
        """Sub-areas track changes via SCD Type 2."""
        property_data = {
            "uuid": "test-scd-010",
            "pid": 1010,
            "account_number": "ACC1010",
            "town_name": "Test Town",
        }

        building_base = {
            "property_uuid": "test-scd-010",
            "pid": 1010,
            "bid": 0,
            "year_built": 1970,
            "construction": {},
        }

        # Version 1 of sub-area
        sub_area_v1 = {
            "code": "FLA",
            "description": "First Floor",
            "gross_area": 1200.0,
            "living_area": 1200.0,
        }

        building1 = building_base.copy()
        building1["sub_areas"] = [sub_area_v1]

        db_writer.write_batch(
            [
                {
                    "property": property_data,
                    "buildings": [building1],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Version 2 - change living area
        sub_area_v2 = sub_area_v1.copy()
        sub_area_v2["living_area"] = 1100.0  # Reduced

        building2 = building_base.copy()
        building2["sub_areas"] = [sub_area_v2]

        db_writer.write_batch(
            [
                {
                    "property": property_data,
                    "buildings": [building2],
                    "ownership": [],
                    "appraisals": [],
                    "assessments": [],
                    "extra_features": [],
                    "outbuildings": [],
                }
            ]
        )

        # Should have 2 versions of sub-area
        rows = db_writer.conn.execute("""
            SELECT version, is_current, living_area
            FROM testcity.sub_areas
            WHERE property_uuid = 'test-scd-010' AND code = 'FLA'
            ORDER BY version
        """).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] is False
        assert rows[0][2] == 1200.0
        assert rows[1][0] == 2
        assert rows[1][1] is True
        assert rows[1][2] == 1100.0
