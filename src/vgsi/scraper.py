"""
VGSI property scraper — pure functions that fetch and parse property data.

Replaces the old class hierarchy (Base/Property/Table/Building/etc.) with
simple functions that return plain dicts.
"""

import hashlib
import logging
import os
import re
import time
import uuid
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_VGSI_CITIES_URL = "https://www.vgsi.com/connecticut-online-database/"
_ERROR_ACTION = "./Error.aspx?Message=There+was+an+error+loading+the+parcel."

# --- Tag mappings (HTML element IDs → field names) ---

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

# Land size has different IDs depending on the city
_LAND_SIZE_IDS = ["MainContent_lblLndSize", "MainContent_lblLndAcres"]

PROPERTY_MONEY_FIELDS = {
    "sale_price",
    "assessment_value",
    "appraisal_value",
    "land_assessed_value",
    "land_appraised_value",
}

PROPERTY_INT_FIELDS = {"building_count"}


class InvalidPIDException(Exception):
    pass


# --- Type coercion helpers ---


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


# --- UUID generation ---


def generate_uuid(pid, data):
    """Generate a deterministic UUID from PID and data dict."""
    uuid_str = f"{pid}{data}"
    hex_string = hashlib.md5(uuid_str.encode("UTF-8")).hexdigest()
    return str(uuid.UUID(hex=hex_string))


# --- HTTP fetching ---


def fetch_page(
    base_url, pid, max_retries=3, initial_delay=1, backoff_factor=2, timeout=30
):
    """
    Fetch a VGSI property page with retry and exponential backoff.

    Returns BeautifulSoup object.
    Raises InvalidPIDException if the PID doesn't exist.
    Raises the last exception if all retries fail.
    """
    url = f"{base_url}Parcel.aspx?pid={pid}"
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, verify=False, timeout=timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Check for error/invalid PID page
            form = soup.find(id="form1")
            if form:
                action = form.get("action", "")
                if action == _ERROR_ACTION:
                    raise InvalidPIDException(f"PID {pid} doesn't exist")

            if attempt > 0:
                logger.info(f"Request succeeded on attempt {attempt + 1}")

            return soup

        except InvalidPIDException:
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

    raise last_exception


# --- Parsing functions ---


def _extract_spans(soup, tag_mapping):
    """Extract text from span elements by ID mapping. Returns dict."""
    result = {}
    for tag in soup.find_all("span"):
        tag_id = tag.get("id")
        if tag_id and tag_id in tag_mapping:
            field_name = tag_mapping[tag_id]
            result[field_name] = tag.get_text(separator=" ", strip=True)
    return result


def parse_property(soup, pid):
    """
    Parse property-level fields from a VGSI page.

    Returns a flat dict with all property fields, type-coerced.
    """
    data = _extract_spans(soup, PROPERTY_TAGS)

    # Handle land size (different ID depending on city)
    for land_id in _LAND_SIZE_IDS:
        el = soup.find("span", id=land_id)
        if el:
            data["land_size_acres"] = el.get_text(strip=True)
            break
    if "land_size_acres" not in data:
        data["land_size_acres"] = None

    # Type coercion
    for field in PROPERTY_MONEY_FIELDS:
        if field in data:
            data[field] = _handle_money(data[field])

    for field in PROPERTY_INT_FIELDS:
        if field in data:
            data[field] = _handle_int(data[field])

    data["land_size_acres"] = _handle_float(data["land_size_acres"])
    data["land_frontage"] = _handle_float(data.get("land_frontage"))
    data["land_depth"] = _handle_float(data.get("land_depth"))

    # Clean remaining string fields
    for key, value in data.items():
        if isinstance(value, str):
            data[key] = _clean_string(value)

    # Generate UUID
    property_uuid = generate_uuid(pid, data)
    data["uuid"] = property_uuid
    data["pid"] = pid

    return data


def parse_buildings(soup, building_count, pid):
    """
    Parse all building data from a VGSI page.

    Returns list of dicts, each with:
    - Core fields: bid, year_built, building_area, replacement_cost, less_depreciation, pct_good
    - photo_url, sketch_url
    - construction: dict of construction detail fields (from grdCns)
    - sub_areas: list of dicts (from grdSub)
    """
    buildings = []
    if building_count is None:
        building_count = 0

    # Try up to building_count + 3 to handle edge cases
    for bid in range(building_count + 3):
        try:
            prefix = f"MainContent_ctl0{bid + 2}"

            # Core span fields
            year_el = soup.find("span", id=f"{prefix}_lblYearBuilt")
            area_el = soup.find("span", id=f"{prefix}_lblBldArea")
            rcn_el = soup.find("span", id=f"{prefix}_lblRcn")
            rcnld_el = soup.find("span", id=f"{prefix}_lblRcnld")
            pctgood_el = soup.find("span", id=f"{prefix}_lblPctGood")

            # If we can't find the year element, this building doesn't exist
            # (only log warning if within expected count)
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

            # Photo URL
            photo_el = soup.find("img", id=f"{prefix}_imgPhoto")
            if photo_el:
                photo_url = photo_el.get("src", "")
                # Skip default placeholder
                if photo_url and "default.jpg" not in photo_url.lower():
                    building["photo_url"] = photo_url
                else:
                    building["photo_url"] = None
            else:
                building["photo_url"] = None

            # Sketch URL
            sketch_el = soup.find("img", alt="Building Layout")
            if sketch_el:
                building["sketch_url"] = sketch_el.get("src")
            else:
                building["sketch_url"] = None

            # Construction details from grdCns table
            building["construction"] = _parse_construction_details(soup, prefix)

            # Sub-areas from grdSub table
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
    for tr in rows[1:]:  # skip header
        tds = tr.find_all("td")
        if len(tds) >= 4:
            code = tds[0].get_text(strip=True)
            if not code:  # skip totals row
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
    """
    Generic parser for VGSI tables (sales, appraisals, assessments,
    extra features, outbuildings).

    Reads headers from first row, values from subsequent rows.
    Returns list of dicts.
    """
    table = soup.find("table", id=table_id)
    if not table:
        return []

    # Check for "No Data" message
    if "No Data" in table.get_text(strip=True):
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    # Extract headers from first row
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


# --- Main scrape function ---


def scrape_property(base_url, pid):
    """
    Scrape all data for a single property.

    Returns dict with keys:
        property, buildings, assessments, appraisals, ownership,
        extra_features, outbuildings
    Or None if the PID is invalid.
    """
    soup = fetch_page(base_url, pid)

    # Parse property
    prop = parse_property(soup, pid)
    property_uuid = prop["uuid"]
    building_count = prop.get("building_count") or 0

    # Parse buildings (with construction details and sub-areas)
    buildings = parse_buildings(soup, building_count, pid)
    for b in buildings:
        b["property_uuid"] = property_uuid
        b["pid"] = pid

    # Parse sales/ownership
    ownership = parse_table_rows(
        soup,
        "MainContent_grdSales",
        money_fields=["sale_price"],
    )
    for o in ownership:
        o["property_uuid"] = property_uuid
        o["pid"] = pid

    # Parse appraisals (history)
    appraisals = parse_table_rows(
        soup,
        "MainContent_grdHistoryValuesAppr",
        money_fields=["improvements", "land", "total"],
    )
    for a in appraisals:
        a["property_uuid"] = property_uuid
        a["pid"] = pid

    # Parse assessments (history)
    assessments = parse_table_rows(
        soup,
        "MainContent_grdHistoryValuesAsmt",
        money_fields=["improvements", "land", "total"],
    )
    for a in assessments:
        a["property_uuid"] = property_uuid
        a["pid"] = pid

    # Parse extra features
    extra_features = parse_table_rows(
        soup,
        "MainContent_grdXf",
        money_fields=["value", "assessed_value"],
    )
    for xf in extra_features:
        xf["property_uuid"] = property_uuid
        xf["pid"] = pid

    # Parse outbuildings
    outbuildings = parse_table_rows(
        soup,
        "MainContent_grdOb",
        money_fields=["value", "assessed_value"],
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


# --- Photo download ---


def download_photo(photo_url, city, pid, output_dir="photos"):
    """
    Download a building photo to local disk.

    Returns local file path on success, None on skip/failure.
    Skips default.jpg placeholders.
    """
    if not photo_url or "default.jpg" in photo_url.lower():
        return None

    city_dir = Path(output_dir) / city
    city_dir.mkdir(parents=True, exist_ok=True)
    local_path = city_dir / f"{pid}.jpg"

    # Skip if already downloaded
    if local_path.exists():
        return str(local_path)

    try:
        response = requests.get(photo_url, verify=False, timeout=30)
        response.raise_for_status()
        local_path.write_bytes(response.content)
        logger.debug(f"Downloaded photo for PID {pid}: {local_path}")
        return str(local_path)
    except Exception as e:
        logger.warning(f"Failed to download photo for PID {pid}: {e}")
        return None


# --- City fetching ---


def fetch_vgsi_cities(url=_VGSI_CITIES_URL, state="ct"):
    """
    Scrape the VGSI website for all cities in a state.

    Returns dict of {city_key: {city_name, state, url, type}}.
    """
    logger.info(f"Fetching VGSI cities from {url}")
    response = requests.get(url, verify=False, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    cities = {}

    for link in soup.find_all(href=re.compile(r"https://gis\.vgsi\.com/")):
        href = link.get("href", "")
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
