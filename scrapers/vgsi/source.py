"""
VGSI source definition — scraping, parsing, city management, and the
VGSI_SOURCE SourceDefinition instance.

Write logic has been replaced by append-only parquet via the engine's
ParquetWriter. SCD2 versioning is derived at query time.
"""

import hashlib
import json
import logging
import re
import time
import uuid
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import duckdb
import requests
import urllib3
from bs4 import BeautifulSoup

from src.engine.base import ResolvedParams, SourceConfig, SourceDefinition

logger = logging.getLogger(__name__)

# VGSI uses self-signed certs; create a dedicated session so we don't
# need to disable SSL warnings process-wide.
_session = requests.Session()
_session.verify = False


# ============================================================================
# Constants & Exceptions
# ============================================================================

_VGSI_CITIES_URL = "https://www.vgsi.com/connecticut-online-database/"
_ERROR_ACTION = "./Error.aspx?Message=There+was+an+error+loading+the+parcel."

PROPERTY_TAGS = {
    "MainContent_lblPid": "pid",
    "MainContent_lblAcctNum": "account_number",
    "MainContent_lblMblu": "mblu",
    "lblTownName": "town_name",
    "MainContent_lblLocation": "address",
    "MainContent_lblGenOwner": "owner",
    "MainContent_lblAddr1": "owner_address",
    "MainContent_lblCoOwner": "co_owner",
    "MainContent_lblPrice": "sale_price",
    "MainContent_lblCertificate": "certificate",
    "MainContent_lblSaleDate": "sale_date",
    "MainContent_lblBp": "book_page",
    "MainContent_lblBookLabel": "book_label",
    "MainContent_lblBook": "book",
    "MainContent_lblPageLabel": "page_label",
    "MainContent_lblPage": "page",
    "MainContent_lblInstrument": "label_instrument",
    "MainContent_lblGenAssessment": "assessment_value",
    "MainContent_lblGenAppraisal": "appraisal_value",
    "MainContent_lblBldCount": "building_count",
    "MainContent_lblUseCode": "land_use_code",
    "MainContent_lblUseCodeDescription": "building_use",
    "MainContent_lblAltApproved": "land_alt_approved",
    "MainContent_lblZone": "land_zone",
    "MainContent_lblNbhd": "land_neighborhood_code",
    "MainContent_lblLndFront": "land_frontage",
    "MainContent_lblDepth": "land_depth",
    "MainContent_lblLndAsmt": "land_assessed_value",
    "MainContent_lblLndAppr": "land_appraised_value",
    "MainContent_lblZip": "zip_code",
}

_LAND_SIZE_IDS = ["MainContent_lblLndSize", "MainContent_lblLndAcres"]

# All known property-level span IDs (for detecting new/extra fields)
_KNOWN_PROPERTY_SPAN_IDS = frozenset(PROPERTY_TAGS.keys()) | frozenset(_LAND_SIZE_IDS)

PROPERTY_MONEY_FIELDS = {
    "sale_price",
    "assessment_value",
    "appraisal_value",
    "land_assessed_value",
    "land_appraised_value",
}

PROPERTY_INT_FIELDS = {"building_count"}

# Construction detail field mapping: parsed name -> column name
CNS_MAPPING = {
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


class InvalidEntryException(Exception):
    pass


# ============================================================================
# Type coercion helpers
# ============================================================================


def _handle_money(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("$", "").replace(",", "")
        if cleaned == "":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _handle_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _handle_int(value):
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _clean_string(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


# ============================================================================
# UUID generation
# ============================================================================


def generate_uuid(pid, data):
    """Generate a deterministic UUID from PID and data dict.

    Uses JSON with sorted keys for canonical serialization, ensuring
    the UUID is stable regardless of dict insertion order.
    """
    if isinstance(data, dict):
        canonical = json.dumps(data, sort_keys=True, default=str)
    else:
        canonical = str(data)
    uuid_str = f"{pid}{canonical}"
    hex_string = hashlib.md5(uuid_str.encode("UTF-8")).hexdigest()
    return str(uuid.UUID(hex=hex_string))


# ============================================================================
# HTTP fetching
# ============================================================================


def fetch_page(
    base_url, pid, max_retries=3, initial_delay=1, backoff_factor=2, timeout=30
):
    """
    Fetch a VGSI property page with retry and exponential backoff.

    Returns BeautifulSoup object.
    Raises InvalidEntryException if the PID doesn't exist.
    """
    url = f"{base_url}Parcel.aspx?pid={pid}"
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
                response = _session.get(url, timeout=timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            form = soup.find(id="form1")
            if form:
                action = form.get("action", "")
                if action == _ERROR_ACTION:
                    raise InvalidEntryException(f"PID {pid} doesn't exist")

            if attempt > 0:
                logger.info(f"Request succeeded on attempt {attempt + 1}")

            return soup

        except InvalidEntryException:
            raise
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        ) as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"Request failed after {max_retries + 1} attempts: {e}")

    raise last_exception or RuntimeError("Request failed with no recorded exception")


# ============================================================================
# Parsing functions
# ============================================================================


def _extract_spans(soup, tag_mapping):
    """Extract text from span elements by ID mapping."""
    result = {}
    for tag in soup.find_all("span"):
        tag_id = tag.get("id")
        if tag_id and tag_id in tag_mapping:
            field_name = tag_mapping[tag_id]
            result[field_name] = tag.get_text(separator=" ", strip=True)
    return result


def parse_property(soup, pid):
    """Parse property-level fields from a VGSI page.

    Any MainContent_lbl* spans not in the known mapping are captured
    in an ``extra_fields`` column as a JSON string so new fields that
    VGSI adds later are never silently lost.
    """
    data = _extract_spans(soup, PROPERTY_TAGS)

    for land_id in _LAND_SIZE_IDS:
        el = soup.find("span", id=land_id)
        if el:
            data["land_size_acres"] = el.get_text(strip=True)
            break
    if "land_size_acres" not in data:
        data["land_size_acres"] = None

    # Capture any MainContent_lbl* spans we don't have a mapping for
    extra = {}
    for tag in soup.find_all("span"):
        tag_id = tag.get("id")
        if (
            tag_id
            and tag_id.startswith("MainContent_lbl")
            and tag_id not in _KNOWN_PROPERTY_SPAN_IDS
        ):
            text = tag.get_text(separator=" ", strip=True)
            if text:
                extra[tag_id] = text
    if extra:
        logger.debug(f"PID {pid}: captured {len(extra)} extra property fields")

    for field in PROPERTY_MONEY_FIELDS:
        if field in data:
            data[field] = _handle_money(data[field])

    for field in PROPERTY_INT_FIELDS:
        if field in data:
            data[field] = _handle_int(data[field])

    data["land_size_acres"] = _handle_float(data["land_size_acres"])
    data["land_frontage"] = _handle_float(data.get("land_frontage"))
    data["land_depth"] = _handle_float(data.get("land_depth"))

    for key, value in data.items():
        if isinstance(value, str):
            data[key] = _clean_string(value)

    data["extra_fields"] = json.dumps(extra, sort_keys=True) if extra else None

    property_uuid = generate_uuid(pid, data)
    data["uuid"] = property_uuid
    data["pid"] = pid

    return data


def parse_buildings(soup, building_count, pid):
    """Parse all building data from a VGSI page."""
    buildings = []
    if building_count is None:
        building_count = 0

    for bid in range(building_count + 3):
        try:
            prefix = f"MainContent_ctl0{bid + 2}"

            year_el = soup.find("span", id=f"{prefix}_lblYearBuilt")
            area_el = soup.find("span", id=f"{prefix}_lblBldArea")
            rcn_el = soup.find("span", id=f"{prefix}_lblRcn")
            rcnld_el = soup.find("span", id=f"{prefix}_lblRcnld")
            pctgood_el = soup.find("span", id=f"{prefix}_lblPctGood")

            if year_el is None and area_el is None:
                if bid < building_count:
                    logger.warning(f"PID {pid}: Building {bid} not found on page")
                continue

            building = {
                "bid": bid,
                "year_built": _handle_int(year_el.get_text(strip=True))
                if year_el
                else None,
                "building_area": _handle_float(
                    area_el.get_text(strip=True).replace(",", "")
                )
                if area_el
                else None,
                "replacement_cost": _handle_money(rcn_el.get_text(strip=True))
                if rcn_el
                else None,
                "less_depreciation": _handle_money(rcnld_el.get_text(strip=True))
                if rcnld_el
                else None,
                "pct_good": _handle_int(pctgood_el.get_text(strip=True))
                if pctgood_el
                else None,
            }

            photo_el = soup.find("img", id=f"{prefix}_imgPhoto")
            if photo_el:
                photo_url = photo_el.get("src", "")
                if photo_url and "default.jpg" not in photo_url.lower():
                    building["photo_url"] = photo_url
                else:
                    building["photo_url"] = None
            else:
                building["photo_url"] = None

            sketch_el = soup.find("img", alt="Building Layout")
            if sketch_el:
                building["sketch_url"] = sketch_el.get("src")
            else:
                building["sketch_url"] = None

            building["construction"] = _parse_construction_details(soup, prefix)
            building["sub_areas"] = _parse_sub_areas(soup, prefix)

            buildings.append(building)

        except Exception as e:
            if bid < building_count:
                logger.warning(f"PID {pid}: Failed to parse building {bid}: {e}")

    return buildings


def _parse_construction_details(soup, prefix):
    """Parse the grdCns construction details table for a building."""
    table = soup.find("table", id=f"{prefix}_grdCns")
    if not table:
        return {}

    details = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            key = tds[0].get_text(strip=True).lower().rstrip(":")
            key = key.replace(" ", "_").replace("&", "and")
            value = tds[1].get_text(strip=True)
            if value:
                details[key] = value
    return details


def _parse_sub_areas(soup, prefix):
    """Parse the grdSub sub-areas table for a building."""
    table = soup.find("table", id=f"{prefix}_grdSub")
    if not table:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    sub_areas = []
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) >= 4:
            code = tds[0].get_text(strip=True)
            if not code:
                continue
            sub_areas.append(
                {
                    "code": code,
                    "description": tds[1].get_text(strip=True),
                    "gross_area": _handle_float(
                        tds[2].get_text(strip=True).replace(",", "")
                    ),
                    "living_area": _handle_float(
                        tds[3].get_text(strip=True).replace(",", "")
                    ),
                }
            )
    return sub_areas


def parse_table_rows(soup, table_id, money_fields=None):
    """Generic parser for VGSI tables (sales, appraisals, assessments, etc.)."""
    table = soup.find("table", id=table_id)
    if not table:
        return []

    if "No Data" in table.get_text(strip=True):
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    headers = []
    for th in rows[0].find_all(["th", "td"]):
        key = th.get_text(strip=True)
        key = key.lower().replace(" ", "_").replace("&", "and")
        headers.append(key)

    if not headers:
        return []

    money_fields = set(money_fields or [])
    results = []

    for tr in rows[1:]:
        tds = tr.find_all("td")
        values = [td.get_text(strip=True) for td in tds]

        row_dict = {}
        for i, header in enumerate(headers):
            if i < len(values):
                val = values[i]
                if header in money_fields:
                    row_dict[header] = _handle_money(val)
                else:
                    row_dict[header] = _clean_string(val)

        if any(v is not None for v in row_dict.values()):
            results.append(row_dict)

    return results


# ============================================================================
# Main scrape function
# ============================================================================


def scrape_property(base_url, pid):
    """
    Scrape all data for a single VGSI property.

    Returns dict with keys: property, buildings, assessments, appraisals,
    ownership, extra_features, outbuildings.
    """
    soup = fetch_page(base_url, pid)

    prop = parse_property(soup, pid)
    property_uuid = prop["uuid"]
    building_count = prop.get("building_count") or 0

    prop["vgsi_url"] = f"{base_url}Parcel.aspx?pid={pid}"

    buildings = parse_buildings(soup, building_count, pid)
    for b in buildings:
        b["property_uuid"] = property_uuid
        b["pid"] = pid

    ownership = parse_table_rows(
        soup, "MainContent_grdSales", money_fields=["sale_price"]
    )
    for o in ownership:
        o["property_uuid"] = property_uuid
        o["pid"] = pid

    appraisals = parse_table_rows(
        soup,
        "MainContent_grdHistoryValuesAppr",
        money_fields=["improvements", "land", "total"],
    )
    for a in appraisals:
        a["property_uuid"] = property_uuid
        a["pid"] = pid

    assessments = parse_table_rows(
        soup,
        "MainContent_grdHistoryValuesAsmt",
        money_fields=["improvements", "land", "total"],
    )
    for a in assessments:
        a["property_uuid"] = property_uuid
        a["pid"] = pid

    extra_features = parse_table_rows(
        soup, "MainContent_grdXf", money_fields=["value", "assessed_value"]
    )
    for xf in extra_features:
        xf["property_uuid"] = property_uuid
        xf["pid"] = pid

    outbuildings = parse_table_rows(
        soup, "MainContent_grdOb", money_fields=["value", "assessed_value"]
    )
    for ob in outbuildings:
        ob["property_uuid"] = property_uuid
        ob["pid"] = pid

    return {
        "property": prop,
        "buildings": buildings,
        "assessments": assessments,
        "appraisals": appraisals,
        "ownership": ownership,
        "extra_features": extra_features,
        "outbuildings": outbuildings,
    }


# ============================================================================
# Flatten — extract per-table rows from scrape results
# ============================================================================


def flatten_vgsi(results: List[dict]) -> dict[str, list[dict]]:
    """
    Extract per-table flat rows from VGSI scrape results.

    Flattens building construction details and sub_areas into their own tables.
    Returns {table_name: [row_dict, ...]}.
    """
    tables: dict[str, list[dict]] = defaultdict(list)
    seen_uuids: set[str] = set()

    for result in results:
        prop = result.get("property")
        if not prop or prop.get("uuid") in seen_uuids:
            continue
        seen_uuids.add(prop["uuid"])

        tables["properties"].append(prop)

        for building in result.get("buildings", []):
            construction = building.get("construction", {})
            sub_areas = building.get("sub_areas", [])

            # Build a flat row without mutating the original
            flat_building = {
                k: v for k, v in building.items()
                if k not in ("construction", "sub_areas")
            }
            # Map known construction keys to columns; capture unknown ones
            cns_known_keys = set(CNS_MAPPING.keys())
            cns_extra = {}
            for src_key, dst_col in CNS_MAPPING.items():
                if src_key in construction:
                    flat_building[dst_col] = construction[src_key]
            for cns_key, cns_val in construction.items():
                if cns_key not in cns_known_keys:
                    cns_extra[cns_key] = cns_val
            if cns_extra:
                flat_building["extra_fields"] = json.dumps(cns_extra, sort_keys=True)

            tables["buildings"].append(flat_building)

            for sa in sub_areas:
                tables["sub_areas"].append({
                    **sa,
                    "property_uuid": building["property_uuid"],
                    "pid": building["pid"],
                    "bid": building.get("bid"),
                })

        for key in ("ownership", "appraisals", "assessments", "extra_features", "outbuildings"):
            tables[key].extend(result.get(key, []))

    return dict(tables)


# ============================================================================
# Photo download
# ============================================================================


def download_photo(photo_url, city, pid, output_dir="photos"):
    """Download a building photo to local disk."""
    if not photo_url or "default.jpg" in photo_url.lower():
        return None

    city_dir = Path(output_dir) / city
    city_dir.mkdir(parents=True, exist_ok=True)
    local_path = city_dir / f"{pid}.jpg"

    if local_path.exists():
        return str(local_path)

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
            response = _session.get(photo_url, timeout=30)
        response.raise_for_status()
        local_path.write_bytes(response.content)
        logger.debug(f"Downloaded photo for PID {pid}: {local_path}")
        return str(local_path)
    except Exception as e:
        logger.warning(f"Failed to download photo for PID {pid}: {e}")
        return None


def _get_photo_items(result, city, entry_id):
    """Extract photo download items from a VGSI scrape result."""
    items = []
    for b in result.get("buildings", []):
        url = b.get("photo_url")
        if url:
            items.append((url, city, entry_id))
    return items


# ============================================================================
# City fetching (VGSI-specific global data)
# ============================================================================


_CITIES_DDL = """
    CREATE TABLE IF NOT EXISTS main.cities (
        city_id INTEGER PRIMARY KEY DEFAULT nextval('main.cities_seq'),
        city_key VARCHAR UNIQUE NOT NULL,
        city_name VARCHAR NOT NULL,
        state VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        type VARCHAR NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""


def _ensure_cities_table(conn):
    """Create the cities table and sequence if they don't exist."""
    conn.execute("CREATE SEQUENCE IF NOT EXISTS main.cities_seq")
    conn.execute(_CITIES_DDL)


def fetch_vgsi_cities(url=_VGSI_CITIES_URL, state="ct", timeout=30):
    """
    Scrape the VGSI website for all cities in a state.

    Returns dict of {city_key: {city_name, state, url, type}}.
    """
    logger.info(f"Fetching VGSI cities from {url}")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        response = _session.get(url, timeout=timeout)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    cities = {}

    for link in soup.find_all(href=re.compile(r"https://gis\.vgsi\.com/")):
        href = str(link.get("href", ""))
        match = re.search(r"([\w]{2,}([cC][Tt])+)", href)
        if match:
            location = match.group(1).lower()
            city_key = location[:-2]
            city_state = location[-2:]
            cities[city_key] = {
                "city_name": link.get_text(strip=True),
                "state": city_state,
                "url": href,
                "type": "vgsi",
            }

    logger.info(f"Found {len(cities)} VGSI cities for state={state}")
    return cities


def store_cities(conn, lock, cities_dict: Dict):
    """Upsert VGSI cities into main.cities table."""
    with lock:
        _ensure_cities_table(conn)

        count = 0
        for city_key, info in cities_dict.items():
            conn.execute(
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
        conn.commit()
        logger.info(f"Stored {count} cities in database")
    return count


def get_city_url(conn, lock, city_key: str) -> Optional[str]:
    """Look up a city's VGSI URL from the database."""
    with lock:
        try:
            result = conn.execute(
                "SELECT url FROM main.cities WHERE city_key = ?",
                [city_key],
            ).fetchone()
            return result[0] if result else None
        except Exception:
            return None


# ============================================================================
# Iteration & entry ID queries
# ============================================================================


def make_load_iter(entry_id_min: int = 1, entry_id_max: int | None = None):
    """Returns an iter_entries_fn for loading a PID range."""

    def iter_entries(base_url: str, data_dir: str, city: str) -> Iterator[int]:
        if entry_id_max is None:
            raise ValueError("entry_id_max is required for VGSI load")
        yield from range(entry_id_min, entry_id_max + 1)

    return iter_entries


def get_known_entry_ids(data_dir: str, scope_key: str) -> list[int]:
    """Return all PIDs from existing parquet files."""
    pattern = f"{data_dir}/{scope_key}/properties/*.parquet"
    glob_results = list(Path(data_dir).glob(f"{scope_key}/properties/*.parquet"))
    if not glob_results:
        return []

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT pid FROM read_parquet('{pattern}') ORDER BY pid"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


# ============================================================================
# SCD Type 2 Query Helpers (for use after data is written)
# ============================================================================


def get_property_history(data_dir: str, scope_key: str, uuid_val: str):
    """Get all versions of a property from parquet files."""
    import pandas as pd

    glob_results = list(Path(data_dir).glob(f"{scope_key}/properties/*.parquet"))
    if not glob_results:
        return pd.DataFrame()

    pattern = f"{data_dir}/{scope_key}/properties/*.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(
            f"""
            SELECT * FROM (
                SELECT *,
                    LAG(row_hash) OVER (PARTITION BY uuid ORDER BY scraped_at) AS prev_hash,
                    ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY scraped_at) AS version
                FROM read_parquet('{pattern}')
                WHERE uuid = ?
            )
            WHERE row_hash != prev_hash OR prev_hash IS NULL
            ORDER BY version
        """,
            [uuid_val],
        ).df()
    finally:
        conn.close()


def get_changed_properties(data_dir: str, scope_key: str, since):
    """Return properties that have changed versions after `since`."""
    import pandas as pd

    glob_results = list(Path(data_dir).glob(f"{scope_key}/properties/*.parquet"))
    if not glob_results:
        return pd.DataFrame()

    pattern = f"{data_dir}/{scope_key}/properties/*.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(
            f"""
            SELECT * FROM (
                SELECT *,
                    LAG(row_hash) OVER (PARTITION BY uuid ORDER BY scraped_at) AS prev_hash
                FROM read_parquet('{pattern}')
            )
            WHERE scraped_at >= ?
              AND prev_hash IS NOT NULL
              AND row_hash != prev_hash
            ORDER BY scraped_at DESC
        """,
            [since],
        ).df()
    finally:
        conn.close()


# ============================================================================
# Source Definition
# ============================================================================


VGSI_SOURCE = SourceDefinition(
    source_key="vgsi",
    scrape_fn=scrape_property,
    flatten_fn=flatten_vgsi,
    get_known_entry_ids_fn=get_known_entry_ids,
    invalid_entry_exception=InvalidEntryException,
    get_photo_items_fn=_get_photo_items,
    download_fn=download_photo,
)


# ============================================================================
# Source Config (CLI / orchestration)
# ============================================================================


class VGSIConfig(SourceConfig):
    def __init__(self):
        super().__init__(source=VGSI_SOURCE)

    def add_args(self, parser):
        parser.add_argument(
            "--entry-id-min", type=int, default=1, help="Starting entry ID"
        )
        parser.add_argument(
            "--entry-id-max", type=int, help="Ending entry ID (required for load)"
        )
        parser.add_argument(
            "--fetch-cities",
            action="store_true",
            help="Fetch and store city list from VGSI website",
        )
        parser.add_argument(
            "--download-photos", action="store_true", help="Download building photos"
        )
        parser.add_argument(
            "--photo-dir", default="photos", help="Photo download directory"
        )

    def resolve(self, args):
        import threading

        if not args.city:
            raise ValueError(
                "city is required for VGSI source (or use: scrape admin vgsi --fetch-cities)"
            )

        base_url = args.base_url
        if base_url is None:
            conn = duckdb.connect(args.db)
            lock = threading.Lock()
            base_url = get_city_url(conn, lock, args.city)
            conn.close()
            if not base_url:
                raise ValueError(
                    f"City '{args.city}' not found. "
                    f"Run 'scrape admin vgsi --fetch-cities' first, or provide --base-url."
                )
        if not base_url.endswith("/"):
            base_url += "/"

        iter_fn = None
        if not getattr(args, "refresh", False):
            if not args.entry_id_max:
                raise ValueError("--entry-id-max is required for load scraping")
            iter_fn = make_load_iter(args.entry_id_min, args.entry_id_max)

        return ResolvedParams(
            base_url=base_url,
            scope_key=args.city,
            iter_entries_fn=iter_fn,
        )

    def get_all_scope_keys(self, data_dir):
        base = Path(data_dir)
        if not base.exists():
            return []
        return [
            d.name
            for d in sorted(base.iterdir())
            if d.is_dir()
            and not d.name.startswith("_")
            and (d / "properties").exists()
        ]

    def post_refresh(self, args, params):

        start_time = getattr(args, "_refresh_start_time", None)
        if start_time is None:
            return

        changed = get_changed_properties(args.data_dir, params.scope_key, since=start_time)
        print(f"Changed entries detected: {len(changed)}")
        if not changed.empty:
            cols = [c for c in ["pid", "address", "owner", "assessment_value", "scraped_at"]
                    if c in changed.columns]
            print(changed[cols].to_string(index=False))

    def run_admin(self, args):
        import threading

        if getattr(args, "fetch_cities", False):
            cities = fetch_vgsi_cities()
            conn = duckdb.connect(args.db)
            lock = threading.Lock()
            count = store_cities(conn, lock, cities)
            conn.close()
            print(f"Stored {count} cities in {args.db}")
            return True
        return False


VGSI_CONFIG = VGSIConfig()
