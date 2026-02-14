"""
- Per-city schema creation
- Thread-safe batch writing with generic insert helper
- Checkpoint management for resume capability
- City metadata storage
- SCD Type 2 (Slowly Changing Dimension) for historical tracking
"""

import hashlib
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)


class DuckDBWriter:
    """
    Thread-safe DuckDB writer for property scraping data.

    Usage:
        writer = DuckDBWriter('newhaven', 'properties.duckdb')
        writer.write_batch([result1, result2, ...])
        writer.save_checkpoint('newhaven', last_pid=100, total=95)
        writer.close()
    """

    def __init__(self, city, db_path="ctcityscraper.duckdb"):
        self.db_path = db_path
        self.city = city
        self.conn = duckdb.connect(db_path)
        self.lock = threading.Lock()
        logger.info(f"Connected to database: {db_path}")
        self._create_schema()

    def _create_schema(self):
        with self.lock:
            c = self.conn
            city = self.city

            c.execute(f"CREATE SCHEMA IF NOT EXISTS {city}")

            # Create sequences first (before tables that reference them)
            for table in [
                "properties",
                "buildings",
                "sub_areas",
                "ownership",
                "appraisals",
                "assessments",
                "extra_features",
                "outbuildings",
            ]:
                c.execute(f"CREATE SEQUENCE IF NOT EXISTS {city}.{table}_seq")

            # Properties (SCD Type 2)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.properties (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.properties_seq'),
                    uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    account_number VARCHAR,
                    mblu VARCHAR,
                    town_name VARCHAR,
                    address VARCHAR,
                    owner VARCHAR,
                    co_owner VARCHAR,
                    owner_address VARCHAR,
                    sale_price DECIMAL(12,2),
                    sale_date VARCHAR,
                    certificate VARCHAR,
                    book VARCHAR,
                    page VARCHAR,
                    book_page VARCHAR,
                    label_instrument VARCHAR,
                    book_label VARCHAR,
                    page_label VARCHAR,
                    assessment_value DECIMAL(12,2),
                    appraisal_value DECIMAL(12,2),
                    building_count INTEGER,
                    building_use VARCHAR,
                    land_use_code VARCHAR,
                    land_zone VARCHAR,
                    land_neighborhood_code VARCHAR,
                    land_alt_approved VARCHAR,
                    land_size_acres DECIMAL(10,4),
                    land_frontage DECIMAL(10,2),
                    land_depth DECIMAL(10,2),
                    land_assessed_value DECIMAL(12,2),
                    land_appraised_value DECIMAL(12,2),
                    zip_code VARCHAR,

                    -- SCD Type 2 fields
                    version INTEGER NOT NULL DEFAULT 1,
                    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE,
                    row_hash VARCHAR NOT NULL,

                    updated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE(uuid, effective_from)
                )
            """)

            # Buildings (with construction details inlined, SCD Type 2)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.buildings (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.buildings_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    bid INTEGER,
                    year_built INTEGER,
                    building_area DECIMAL(10,2),
                    replacement_cost DECIMAL(12,2),
                    less_depreciation DECIMAL(12,2),
                    pct_good INTEGER,
                    photo_url VARCHAR,
                    photo_local_path VARCHAR,
                    sketch_url VARCHAR,
                    style VARCHAR,
                    model VARCHAR,
                    grade VARCHAR,
                    stories VARCHAR,
                    occupancy VARCHAR,
                    exterior_wall_1 VARCHAR,
                    exterior_wall_2 VARCHAR,
                    roof_structure VARCHAR,
                    roof_cover VARCHAR,
                    interior_wall_1 VARCHAR,
                    interior_wall_2 VARCHAR,
                    interior_floor_1 VARCHAR,
                    interior_floor_2 VARCHAR,
                    heat_fuel VARCHAR,
                    heat_type VARCHAR,
                    ac_type VARCHAR,
                    total_bedrooms VARCHAR,
                    total_bthrms VARCHAR,
                    total_half_baths VARCHAR,
                    total_xtra_fixtrs VARCHAR,
                    total_rooms VARCHAR,
                    bath_style VARCHAR,
                    kitchen_style VARCHAR,
                    interior_condition VARCHAR,
                    fin_bsmnt_area VARCHAR,
                    fin_bsmnt_qual VARCHAR,
                    nbhd_code VARCHAR,

                    -- SCD Type 2 fields
                    version INTEGER NOT NULL DEFAULT 1,
                    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE,
                    row_hash VARCHAR NOT NULL,

                    updated_at TIMESTAMP,

                    UNIQUE(property_uuid, bid, effective_from)
                )
            """)

            # Sub-areas (per building, SCD Type 2)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.sub_areas (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.sub_areas_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    bid INTEGER,
                    code VARCHAR,
                    description VARCHAR,
                    gross_area DECIMAL(10,2),
                    living_area DECIMAL(10,2),

                    -- SCD Type 2 fields
                    version INTEGER NOT NULL DEFAULT 1,
                    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE,
                    row_hash VARCHAR NOT NULL,

                )
            """)

            # Ownership / sales history
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.ownership (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.ownership_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    owner VARCHAR,
                    sale_price DECIMAL(12,2),
                    certificate VARCHAR,
                    book_and_page VARCHAR,
                    instrument VARCHAR,
                    sale_date VARCHAR,
                    updated_at TIMESTAMP
                )
            """)

            # Appraisals (history)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.appraisals (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.appraisals_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    valuation_year VARCHAR,
                    improvements DECIMAL(12,2),
                    land DECIMAL(12,2),
                    total DECIMAL(12,2),
                    updated_at TIMESTAMP
                )
            """)

            # Assessments (history)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.assessments (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.assessments_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    valuation_year VARCHAR,
                    improvements DECIMAL(12,2),
                    land DECIMAL(12,2),
                    total DECIMAL(12,2),
                    updated_at TIMESTAMP
                )
            """)

            # Extra features (SCD Type 2)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.extra_features (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.extra_features_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    code VARCHAR,
                    description VARCHAR,
                    size VARCHAR,
                    value DECIMAL(12,2),
                    assessed_value DECIMAL(12,2),
                    bldg_num VARCHAR,

                    -- SCD Type 2 fields
                    version INTEGER NOT NULL DEFAULT 1,
                    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE,
                    row_hash VARCHAR NOT NULL,

                    UNIQUE(id, effective_from)
                )
            """)

            # Outbuildings (SCD Type 2)
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}.outbuildings (
                    id INTEGER PRIMARY KEY DEFAULT nextval('{city}.outbuildings_seq'),
                    property_uuid VARCHAR NOT NULL,
                    pid INTEGER NOT NULL,
                    code VARCHAR,
                    description VARCHAR,
                    sub_code VARCHAR,
                    sub_description VARCHAR,
                    size VARCHAR,
                    value DECIMAL(12,2),
                    assessed_value DECIMAL(12,2),
                    bldg_num VARCHAR,

                    -- SCD Type 2 fields
                    version INTEGER NOT NULL DEFAULT 1,
                    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE,
                    row_hash VARCHAR NOT NULL,

                    UNIQUE(id, effective_from)
                )
            """)

            # Cities table (global)
            c.execute("""
                CREATE TABLE IF NOT EXISTS main.cities (
                    city_key VARCHAR PRIMARY KEY,
                    city_name VARCHAR NOT NULL,
                    state VARCHAR NOT NULL,
                    url VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Checkpoints (global)
            c.execute("""
                CREATE TABLE IF NOT EXISTS main.scrape_checkpoints (
                    id INTEGER,
                    city VARCHAR NOT NULL,
                    last_pid INTEGER NOT NULL,
                    total_scraped INTEGER NOT NULL,
                    checkpoint_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create sequences for auto-increment IDs
            for table in [
                "buildings",
                "sub_areas",
                "ownership",
                "appraisals",
                "assessments",
                "extra_features",
                "outbuildings",
            ]:
                try:
                    c.execute(f"CREATE SEQUENCE IF NOT EXISTS {city}.{table}_seq")
                except Exception:
                    pass  # sequence already exists

            # Indexes (including SCD Type 2 indexes)
            for idx_sql in [
                # Basic lookup indexes
                f"CREATE INDEX IF NOT EXISTS idx_{city}_prop_pid ON {city}.properties(pid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_bldg_uuid ON {city}.buildings(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_own_uuid ON {city}.ownership(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_appr_uuid ON {city}.appraisals(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_asmt_uuid ON {city}.assessments(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_xf_uuid ON {city}.extra_features(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_ob_uuid ON {city}.outbuildings(property_uuid)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_sub_uuid ON {city}.sub_areas(property_uuid)",
                # SCD Type 2 indexes - current records (no partial indexes in DuckDB)
                f"CREATE INDEX IF NOT EXISTS idx_{city}_prop_current ON {city}.properties(uuid, is_current)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_bldg_current ON {city}.buildings(property_uuid, bid, is_current)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_sub_current ON {city}.sub_areas(property_uuid, is_current)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_xf_current ON {city}.extra_features(property_uuid, is_current)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_ob_current ON {city}.outbuildings(property_uuid, is_current)",
                # SCD Type 2 indexes - temporal queries
                f"CREATE INDEX IF NOT EXISTS idx_{city}_prop_dates ON {city}.properties(effective_from, effective_to)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_bldg_dates ON {city}.buildings(effective_from, effective_to)",
                # SCD Type 2 indexes - hash lookups
                f"CREATE INDEX IF NOT EXISTS idx_{city}_prop_hash ON {city}.properties(row_hash)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_bldg_hash ON {city}.buildings(row_hash)",
                # SCD Type 2 indexes - version queries
                f"CREATE INDEX IF NOT EXISTS idx_{city}_prop_version ON {city}.properties(uuid, version)",
                f"CREATE INDEX IF NOT EXISTS idx_{city}_bldg_version ON {city}.buildings(property_uuid, bid, version)",
            ]:
                try:
                    c.execute(idx_sql)
                except Exception as e:
                    logger.debug(f"Index creation note: {e}")

            # Create current state views
            self._create_current_views()

            logger.info(f"Schema created for city: {city}")

    def _create_current_views(self):
        """Create views for current (is_current=TRUE) records."""
        # Note: This method is called from _create_schema which already holds the lock
        tables = [
            "properties",
            "buildings",
            "sub_areas",
            "extra_features",
            "outbuildings",
        ]

        for table in tables:
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {self.city}.{table}_current AS
                    SELECT * FROM {self.city}.{table}
                    WHERE is_current = TRUE
                """)
            except Exception as e:
                logger.debug(f"View creation note for {table}: {e}")

        logger.info(f"Created current views for {self.city}")

    def _compute_row_hash(self, data: Dict, exclude_fields: List[str] = None) -> str:
        """Compute MD5 hash of data for change detection."""
        exclude = set(exclude_fields or [])
        exclude.update(
            [
                "version",
                "effective_from",
                "effective_to",
                "is_current",
                "row_hash",
                "updated_at",
                "created_at",
                "id",
            ]
        )

        # Sort keys for consistent hashing
        hash_data = {
            k: str(v)
            for k, v in sorted(data.items())
            if k not in exclude and v is not None
        }
        hash_string = str(hash_data)
        return hashlib.md5(hash_string.encode("utf-8")).hexdigest()

    def _insert_row(self, table, data, columns):
        """Generic insert helper. Only inserts columns that exist in data."""
        vals = {}
        cols_present = []
        for col in columns:
            if col in data and data[col] is not None:
                vals[col] = data[col]
                cols_present.append(col)

        if not cols_present:
            return

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)
        self.conn.execute(
            f"INSERT INTO {self.city}.{table} ({col_names}) VALUES ({placeholders})",
            vals,
        )

    def write_batch(self, results: List[Dict]):
        """Write a batch of scraping results to database."""
        if not results:
            return

        now = datetime.now()

        with self.lock:
            try:
                self.conn.begin()

                seen_uuids = set()

                for result in results:
                    prop = result.get("property")
                    if prop and prop["uuid"] in seen_uuids:
                        continue
                    if prop:
                        seen_uuids.add(prop["uuid"])

                    if prop is None:
                        continue

                    prop["updated_at"] = now
                    self._upsert_property(prop)

                    for building in result.get("buildings", []):
                        building["updated_at"] = now
                        self._insert_building(building)

                    for o in result.get("ownership", []):
                        o["updated_at"] = now
                        self._insert_row(
                            "ownership",
                            o,
                            [
                                "property_uuid",
                                "pid",
                                "owner",
                                "sale_price",
                                "certificate",
                                "book_and_page",
                                "instrument",
                                "sale_date",
                                "updated_at",
                            ],
                        )

                    for a in result.get("appraisals", []):
                        a["updated_at"] = now
                        self._insert_row(
                            "appraisals",
                            a,
                            [
                                "property_uuid",
                                "pid",
                                "valuation_year",
                                "improvements",
                                "land",
                                "total",
                                "updated_at",
                            ],
                        )

                    for a in result.get("assessments", []):
                        a["updated_at"] = now
                        self._insert_row(
                            "assessments",
                            a,
                            [
                                "property_uuid",
                                "pid",
                                "valuation_year",
                                "improvements",
                                "land",
                                "total",
                                "updated_at",
                            ],
                        )

                    for xf in result.get("extra_features", []):
                        self._upsert_extra_feature(xf, now)

                    for ob in result.get("outbuildings", []):
                        self._upsert_outbuilding(ob, now)

                self.conn.commit()
                logger.debug(f"Wrote batch of {len(results)} properties")

            except Exception as e:
                self.conn.rollback()
                logger.error(f"Failed to write batch: {e}, for {prop['pid']}")
                raise

    def _upsert_property(self, data):
        """Insert new version or update existing if unchanged (SCD Type 2)."""
        uuid = data["uuid"]
        now = datetime.now()

        # Compute hash of new data
        new_hash = self._compute_row_hash(data)

        existing = self.conn.execute(
            f"""
            SELECT id FROM {self.city}.properties
            WHERE uuid = ? AND effective_from = ?
        """,
            [uuid, now],
        ).fetchone()

        if existing:
            return

        # Check for current version
        current = self.conn.execute(
            f"""
            SELECT id, row_hash, version
            FROM {self.city}.properties
            WHERE uuid = ? AND is_current = TRUE
        """,
            [uuid],
        ).fetchone()

        if current:
            current_id, current_hash, current_version = current

            # If unchanged, just update timestamp
            if current_hash == new_hash:
                self.conn.execute(
                    f"""
                    UPDATE {self.city}.properties
                    SET updated_at = ?
                    WHERE id = ?
                """,
                    [now, current_id],
                )
                return

            # Data changed - expire current version
            self.conn.execute(
                f"""
                UPDATE {self.city}.properties
                SET effective_to = ?, is_current = FALSE
                WHERE id = ?
            """,
                [now, current_id],
            )

            # Set next version number
            data["version"] = current_version + 1
        else:
            data["version"] = 1

        # Insert new version
        data["row_hash"] = new_hash
        data["effective_from"] = now
        data["effective_to"] = None
        data["is_current"] = True
        data["created_at"] = now
        data["updated_at"] = now

        # Define columns for insert
        cols = [
            "uuid",
            "pid",
            "account_number",
            "mblu",
            "town_name",
            "address",
            "owner",
            "co_owner",
            "owner_address",
            "sale_price",
            "sale_date",
            "certificate",
            "book",
            "page",
            "book_page",
            "label_instrument",
            "book_label",
            "page_label",
            "assessment_value",
            "appraisal_value",
            "building_count",
            "building_use",
            "land_use_code",
            "land_zone",
            "land_neighborhood_code",
            "land_alt_approved",
            "land_size_acres",
            "land_frontage",
            "land_depth",
            "land_assessed_value",
            "land_appraised_value",
            "zip_code",
            "version",
            "effective_from",
            "effective_to",
            "is_current",
            "row_hash",
            "created_at",
            "updated_at",
        ]

        vals = {c: data.get(c) for c in cols}
        cols_present = [c for c in cols if vals[c] is not None]

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)

        self.conn.execute(
            f"INSERT INTO {self.city}.properties ({col_names}) VALUES ({placeholders})",
            {c: vals[c] for c in cols_present},
        )

    def _insert_building(self, data):
        """Insert building with SCD Type 2 versioning."""
        # Flatten construction details into the building dict
        construction = data.pop("construction", {})
        sub_areas = data.pop("sub_areas", [])
        now = datetime.now()

        # Map construction keys to DB column names
        cns_mapping = {
            "style": "style",
            "model": "model",
            "grade": "grade",
            "stories": "stories",
            "occupancy": "occupancy",
            "exterior_wall_1": "exterior_wall_1",
            "exterior_wall_2": "exterior_wall_2",
            "roof_structure": "roof_structure",
            "roof_cover": "roof_cover",
            "interior_wall_1": "interior_wall_1",
            "interior_wall_2": "interior_wall_2",
            "interior_flr_1": "interior_floor_1",
            "interior_flr_2": "interior_floor_2",
            "interior_floor_1": "interior_floor_1",
            "interior_floor_2": "interior_floor_2",
            "heat_fuel": "heat_fuel",
            "heat_type": "heat_type",
            "ac_type": "ac_type",
            "total_bedrooms": "total_bedrooms",
            "total_bthrms": "total_bthrms",
            "total_half_baths": "total_half_baths",
            "total_xtra_fixtrs": "total_xtra_fixtrs",
            "total_rooms": "total_rooms",
            "bath_style": "bath_style",
            "kitchen_style": "kitchen_style",
            "interior_condition": "interior_condition",
            "fin_bsmnt_area": "fin_bsmnt_area",
            "fin_bsmnt_qual": "fin_bsmnt_qual",
            "nbhd_code": "nbhd_code",
        }

        for src_key, dst_col in cns_mapping.items():
            if src_key in construction:
                data[dst_col] = construction[src_key]

        # Compute hash
        new_hash = self._compute_row_hash(data)

        # Check for current version
        current = self.conn.execute(
            f"""
            SELECT id, row_hash, version
            FROM {self.city}.buildings
            WHERE property_uuid = ? AND bid = ? AND is_current = TRUE
        """,
            [data["property_uuid"], data.get("bid")],
        ).fetchone()

        if current:
            current_id, current_hash, current_version = current

            if current_hash == new_hash:
                self.conn.execute(
                    f"""
                    UPDATE {self.city}.buildings SET updated_at = ? WHERE id = ?
                """,
                    [now, current_id],
                )
                # Still process sub-areas
                for sa in sub_areas:
                    sa["property_uuid"] = data["property_uuid"]
                    sa["pid"] = data["pid"]
                    sa["bid"] = data.get("bid")
                    self._upsert_sub_area(sa, now)
                return

            # Expire current
            self.conn.execute(
                f"""
                UPDATE {self.city}.buildings SET effective_to = ?, is_current = FALSE WHERE id = ?
            """,
                [now, current_id],
            )
            data["version"] = current_version + 1
        else:
            data["version"] = 1

        data["row_hash"] = new_hash
        data["effective_from"] = now
        data["effective_to"] = None
        data["is_current"] = True
        data["updated_at"] = now

        # Insert building
        cols = [
            "property_uuid",
            "pid",
            "bid",
            "year_built",
            "building_area",
            "replacement_cost",
            "less_depreciation",
            "pct_good",
            "photo_url",
            "photo_local_path",
            "sketch_url",
            "style",
            "model",
            "grade",
            "stories",
            "occupancy",
            "exterior_wall_1",
            "exterior_wall_2",
            "roof_structure",
            "roof_cover",
            "interior_wall_1",
            "interior_wall_2",
            "interior_floor_1",
            "interior_floor_2",
            "heat_fuel",
            "heat_type",
            "ac_type",
            "total_bedrooms",
            "total_bthrms",
            "total_half_baths",
            "total_xtra_fixtrs",
            "total_rooms",
            "bath_style",
            "kitchen_style",
            "interior_condition",
            "fin_bsmnt_area",
            "fin_bsmnt_qual",
            "nbhd_code",
            "version",
            "effective_from",
            "effective_to",
            "is_current",
            "row_hash",
            "updated_at",
        ]

        vals = {c: data.get(c) for c in cols}
        cols_present = [c for c in cols if vals[c] is not None]

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)

        self.conn.execute(
            f"INSERT INTO {self.city}.buildings ({col_names}) VALUES ({placeholders})",
            {c: vals[c] for c in cols_present},
        )

        # Insert sub-areas with SCD Type 2
        for sa in sub_areas:
            sa["property_uuid"] = data["property_uuid"]
            sa["pid"] = data["pid"]
            sa["bid"] = data.get("bid")
            self._upsert_sub_area(sa, now)

    def _upsert_sub_area(self, data: Dict, now: datetime):
        """Insert or update sub-area with SCD Type 2."""
        new_hash = self._compute_row_hash(data)

        current = self.conn.execute(
            f"""
            SELECT id, row_hash, version
            FROM {self.city}.sub_areas
            WHERE property_uuid = ? AND bid = ? AND code = ? AND is_current = TRUE
        """,
            [data["property_uuid"], data.get("bid"), data.get("code")],
        ).fetchone()

        if current:
            current_id, current_hash, current_version = current
            if current_hash == new_hash:
                return

            self.conn.execute(
                f"""
                UPDATE {self.city}.sub_areas SET effective_to = ?, is_current = FALSE WHERE id = ?
            """,
                [now, current_id],
            )
            data["version"] = current_version + 1
        else:
            data["version"] = 1

        data["row_hash"] = new_hash
        data["effective_from"] = now
        data["effective_to"] = None
        data["is_current"] = True

        cols = [
            "property_uuid",
            "pid",
            "bid",
            "code",
            "description",
            "gross_area",
            "living_area",
            "version",
            "effective_from",
            "effective_to",
            "is_current",
            "row_hash",
        ]

        vals = {c: data.get(c) for c in cols}
        cols_present = [c for c in cols if vals[c] is not None]

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)

        self.conn.execute(
            f"INSERT INTO {self.city}.sub_areas ({col_names}) VALUES ({placeholders})",
            {c: vals[c] for c in cols_present},
        )

    def _upsert_extra_feature(self, data: Dict, now: datetime):
        """Insert or update extra feature with SCD Type 2."""
        new_hash = self._compute_row_hash(data)

        current = self.conn.execute(
            f"""
            SELECT id, row_hash, version
            FROM {self.city}.extra_features
            WHERE property_uuid = ? AND code = ? AND is_current = TRUE
        """,
            [data["property_uuid"], data.get("code")],
        ).fetchone()

        if current:
            current_id, current_hash, current_version = current
            if current_hash == new_hash:
                return

            self.conn.execute(
                f"""
                UPDATE {self.city}.extra_features SET effective_to = ?, is_current = FALSE WHERE id = ?
            """,
                [now, current_id],
            )
            data["version"] = current_version + 1
        else:
            data["version"] = 1

        data["row_hash"] = new_hash
        data["effective_from"] = now
        data["effective_to"] = None
        data["is_current"] = True

        cols = [
            "property_uuid",
            "pid",
            "code",
            "description",
            "size",
            "value",
            "assessed_value",
            "bldg_num",
            "version",
            "effective_from",
            "effective_to",
            "is_current",
            "row_hash",
        ]

        vals = {c: data.get(c) for c in cols}
        cols_present = [c for c in cols if vals[c] is not None]

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)

        self.conn.execute(
            f"INSERT INTO {self.city}.extra_features ({col_names}) VALUES ({placeholders})",
            {c: vals[c] for c in cols_present},
        )

    def _upsert_outbuilding(self, data: Dict, now: datetime):
        """Insert or update outbuilding with SCD Type 2."""
        new_hash = self._compute_row_hash(data)

        current = self.conn.execute(
            f"""
            SELECT id, row_hash, version
            FROM {self.city}.outbuildings
            WHERE property_uuid = ? AND code = ? AND is_current = TRUE
        """,
            [data["property_uuid"], data.get("code")],
        ).fetchone()

        if current:
            current_id, current_hash, current_version = current
            if current_hash == new_hash:
                return

            self.conn.execute(
                f"""
                UPDATE {self.city}.outbuildings SET effective_to = ?, is_current = FALSE WHERE id = ?
            """,
                [now, current_id],
            )
            data["version"] = current_version + 1
        else:
            data["version"] = 1

        data["row_hash"] = new_hash
        data["effective_from"] = now
        data["effective_to"] = None
        data["is_current"] = True

        cols = [
            "property_uuid",
            "pid",
            "code",
            "description",
            "sub_code",
            "sub_description",
            "size",
            "value",
            "assessed_value",
            "bldg_num",
            "version",
            "effective_from",
            "effective_to",
            "is_current",
            "row_hash",
        ]

        vals = {c: data.get(c) for c in cols}
        cols_present = [c for c in cols if vals[c] is not None]

        col_names = ", ".join(cols_present)
        placeholders = ", ".join(f"${c}" for c in cols_present)

        self.conn.execute(
            f"INSERT INTO {self.city}.outbuildings ({col_names}) VALUES ({placeholders})",
            {c: vals[c] for c in cols_present},
        )

    # --- Checkpoint ---

    def save_checkpoint(self, city: str, last_pid: int, total_scraped: int):
        with self.lock:
            self.conn.execute(
                "INSERT INTO main.scrape_checkpoints (city, last_pid, total_scraped) VALUES (?, ?, ?)",
                [city, last_pid, total_scraped],
            )
            self.conn.commit()
            logger.info(
                f"Checkpoint: city={city}, last_pid={last_pid}, total={total_scraped}"
            )

    def get_last_checkpoint(self, city: str) -> Tuple[int, int]:
        with self.lock:
            result = self.conn.execute(
                "SELECT last_pid, total_scraped FROM main.scrape_checkpoints "
                "WHERE city = ? ORDER BY checkpoint_time DESC LIMIT 1",
                [city],
            ).fetchone()
            if result:
                logger.info(
                    f"Resuming: city={city}, last_pid={result[0]}, total={result[1]}"
                )
                return result[0], result[1]
            return 0, 0

    # --- City management ---

    def store_cities(self, cities_dict: Dict):
        """Upsert cities from fetch_vgsi_cities() into main.cities table."""
        with self.lock:
            count = 0
            for city_key, info in cities_dict.items():
                self.conn.execute(
                    """
                    INSERT INTO main.cities (city_key, city_name, state, url, type)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (city_key) DO UPDATE SET
                        city_name = EXCLUDED.city_name,
                        url = EXCLUDED.url,
                        type = EXCLUDED.type
                """,
                    [
                        city_key,
                        info["city_name"],
                        info["state"],
                        info["url"],
                        info["type"],
                    ],
                )
                count += 1
            self.conn.commit()
            logger.info(f"Stored {count} cities in database")
        return count

    def get_city_url(self, city_key: str) -> Optional[str]:
        """Look up a city's VGSI URL from the database."""
        with self.lock:
            result = self.conn.execute(
                "SELECT url FROM main.cities WHERE city_key = ?",
                [city_key],
            ).fetchone()
            return result[0] if result else None

    # --- SCD Type 2 Query Helpers ---

    def get_current_properties(self, limit: Optional[int] = None):
        """Get current versions of all properties."""
        with self.lock:
            query = f"SELECT * FROM {self.city}.properties_current"
            if limit:
                query += f" LIMIT {limit}"
            return self.conn.execute(query).df()

    def get_property_history(self, uuid: str):
        """Get all versions of a property."""
        with self.lock:
            return self.conn.execute(
                f"""
                SELECT version, effective_from, effective_to, is_current,
                       owner, assessment_value, appraisal_value, building_count
                FROM {self.city}.properties
                WHERE uuid = ?
                ORDER BY version
            """,
                [uuid],
            ).df()

    def get_property_at_date(self, uuid: str, as_of_date: datetime):
        """Get property state at a specific date."""
        with self.lock:
            return self.conn.execute(
                f"""
                SELECT * FROM {self.city}.properties
                WHERE uuid = ?
                  AND effective_from <= ?
                  AND (effective_to IS NULL OR effective_to > ?)
            """,
                [uuid, as_of_date, as_of_date],
            ).df()

    def close(self):
        self.conn.close()
        logger.info(f"Database closed: {self.db_path}")
