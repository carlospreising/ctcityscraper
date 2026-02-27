"""
Tests for VGSI scraper functions.

Tests HTML parsing, data extraction, type coercion, and error handling.
"""

import json

from bs4 import BeautifulSoup

from scrapers.vgsi.source import (
    _clean_string,
    _handle_float,
    _handle_int,
    _handle_money,
    flatten_vgsi,
    generate_uuid,
    parse_buildings,
    parse_property,
    parse_table_rows,
)


class TestTypeCoercion:
    """Test type coercion helper functions."""

    def test_handle_money_valid(self):
        """Test money parsing with valid inputs."""
        assert _handle_money("$100,000.00") == 100000.0
        assert _handle_money("$1,234.56") == 1234.56
        assert _handle_money("500") == 500.0
        assert _handle_money(1000) == 1000.0
        assert _handle_money(1000.5) == 1000.5

    def test_handle_money_invalid(self):
        """Test money parsing with invalid inputs."""
        assert _handle_money("") is None
        assert _handle_money("   ") is None
        assert _handle_money("N/A") is None
        assert _handle_money(None) is None

    def test_handle_float_valid(self):
        """Test float parsing with valid inputs."""
        assert _handle_float("123.45") == 123.45
        assert _handle_float("1000") == 1000.0
        assert _handle_float(500) == 500.0
        assert _handle_float(123.456) == 123.456

    def test_handle_float_invalid(self):
        """Test float parsing with invalid inputs."""
        assert _handle_float("") is None
        assert _handle_float("N/A") is None
        assert _handle_float(None) is None

    def test_handle_int_valid(self):
        """Test integer parsing with valid inputs."""
        assert _handle_int("123") == 123
        assert _handle_int("0") == 0
        assert _handle_int(456) == 456
        assert _handle_int(789.0) == 789

    def test_handle_int_invalid(self):
        """Test integer parsing with invalid inputs."""
        assert _handle_int("") is None
        assert _handle_int("N/A") is None
        assert _handle_int(None) is None

    def test_clean_string(self):
        """Test string cleaning."""
        assert _clean_string("  hello  ") == "hello"
        assert _clean_string("") is None
        assert _clean_string("   ") is None
        assert _clean_string(None) is None
        assert _clean_string("test") == "test"


class TestUUIDGeneration:
    """Test UUID generation."""

    def test_generate_uuid_deterministic(self):
        """Test that UUID generation is deterministic."""
        uuid1 = generate_uuid(123, "testdata")
        uuid2 = generate_uuid(123, "testdata")
        assert uuid1 == uuid2

    def test_generate_uuid_different_inputs(self):
        """Test that different inputs produce different UUIDs."""
        uuid1 = generate_uuid(123, "data1")
        uuid2 = generate_uuid(123, "data2")
        uuid3 = generate_uuid(456, "data1")

        assert uuid1 != uuid2
        assert uuid1 != uuid3
        assert uuid2 != uuid3

    def test_generate_uuid_format(self):
        """Test that generated UUID has correct format."""
        uuid = generate_uuid(123, "test")
        assert len(uuid) == 36  # Standard UUID format with hyphens
        assert uuid.count("-") == 4

    def test_generate_uuid_dict_order_independent(self):
        """Test that UUID is stable regardless of dict insertion order."""
        data1 = {"address": "100 Main St", "owner": "John", "town": "Hartford"}
        data2 = {"town": "Hartford", "address": "100 Main St", "owner": "John"}
        assert generate_uuid(123, data1) == generate_uuid(123, data2)

    def test_generate_uuid_dict_deterministic(self):
        """Test that UUID from dict is deterministic across calls."""
        data = {"pid": "123", "address": "100 Main St", "owner": "John"}
        assert generate_uuid(123, data) == generate_uuid(123, data)


class TestParseProperty:
    """Test property parsing from HTML."""

    def test_parse_property_basic(self):
        """Test parsing basic property fields."""
        html = """
        <html>
            <span id="MainContent_lblPid">123</span>
            <span id="MainContent_lblAcctNum">ACC123</span>
            <span id="lblTownName">New Haven</span>
            <span id="MainContent_lblLocation">100 Main St</span>
            <span id="MainContent_lblGenOwner">John Doe</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        assert result["pid"] == 123
        assert result["account_number"] == "ACC123"
        assert result["town_name"] == "New Haven"
        assert result["address"] == "100 Main St"
        assert result["owner"] == "John Doe"
        assert "uuid" in result

    def test_parse_property_money_fields(self):
        """Test parsing money fields with proper type coercion."""
        html = """
        <html>
            <span id="MainContent_lblPid">456</span>
            <span id="MainContent_lblAcctNum">ACC456</span>
            <span id="lblTownName">Hartford</span>
            <span id="MainContent_lblPrice">$350,000.00</span>
            <span id="MainContent_lblGenAssessment">$300,000</span>
            <span id="MainContent_lblGenAppraisal">$320,000.50</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 456)

        assert result["sale_price"] == 350000.0
        assert result["assessment_value"] == 300000.0
        assert result["appraisal_value"] == 320000.5

    def test_parse_property_numeric_fields(self):
        """Test parsing numeric fields."""
        html = """
        <html>
            <span id="MainContent_lblPid">789</span>
            <span id="MainContent_lblAcctNum">ACC789</span>
            <span id="lblTownName">Stamford</span>
            <span id="MainContent_lblBldCount">3</span>
            <span id="MainContent_lblLndSize">2.5</span>
            <span id="MainContent_lblLndFront">100.5</span>
            <span id="MainContent_lblDepth">200.75</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 789)

        assert result["building_count"] == 3
        assert result["land_size_acres"] == 2.5
        assert result["land_frontage"] == 100.5
        assert result["land_depth"] == 200.75

    def test_parse_property_missing_fields(self):
        """Test that missing fields don't cause errors."""
        html = """
        <html>
            <span id="MainContent_lblPid">100</span>
            <span id="MainContent_lblAcctNum">ACC100</span>
            <span id="lblTownName">Bridgeport</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 100)

        assert result["pid"] == 100
        assert result["town_name"] == "Bridgeport"
        # Missing fields should not be in result or be None
        assert result.get("sale_price") is None
        assert result.get("owner") is None


class TestParseBuildings:
    """Test building parsing from HTML."""

    def test_parse_buildings_single(self):
        """Test parsing a single building."""
        html = """
        <html>
            <span id="MainContent_ctl02_lblYearBuilt">1950</span>
            <span id="MainContent_ctl02_lblBldArea">2,000</span>
            <span id="MainContent_ctl02_lblRcn">$400,000</span>
            <span id="MainContent_ctl02_lblRcnld">$350,000</span>
            <span id="MainContent_ctl02_lblPctGood">85</span>
            <img id="MainContent_ctl02_imgPhoto" src="photo1.jpg" />
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=1, pid=123)

        assert len(buildings) == 1
        assert buildings[0]["bid"] == 0
        assert buildings[0]["year_built"] == 1950
        assert buildings[0]["building_area"] == 2000.0
        assert buildings[0]["replacement_cost"] == 400000.0
        assert buildings[0]["less_depreciation"] == 350000.0
        assert buildings[0]["pct_good"] == 85
        assert buildings[0]["photo_url"] == "photo1.jpg"

    def test_parse_buildings_multiple(self):
        """Test parsing multiple buildings."""
        html = """
        <html>
            <span id="MainContent_ctl02_lblYearBuilt">1950</span>
            <span id="MainContent_ctl02_lblBldArea">2,000</span>
            <span id="MainContent_ctl03_lblYearBuilt">1990</span>
            <span id="MainContent_ctl03_lblBldArea">500</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=2, pid=456)

        assert len(buildings) == 2
        assert buildings[0]["bid"] == 0
        assert buildings[0]["year_built"] == 1950
        assert buildings[1]["bid"] == 1
        assert buildings[1]["year_built"] == 1990

    def test_parse_buildings_skip_default_photo(self):
        """Test that default placeholder photos are skipped."""
        html = """
        <html>
            <span id="MainContent_ctl02_lblYearBuilt">1950</span>
            <img id="MainContent_ctl02_imgPhoto" src="images/default.jpg" />
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=1, pid=123)

        assert buildings[0]["photo_url"] is None

    def test_parse_buildings_construction_details(self):
        """Test parsing construction details table."""
        html = """
        <html>
            <span id="MainContent_ctl02_lblYearBuilt">1960</span>
            <table id="MainContent_ctl02_grdCns">
                <tr>
                    <td>Style:</td>
                    <td>Colonial</td>
                </tr>
                <tr>
                    <td>Grade:</td>
                    <td>Good</td>
                </tr>
                <tr>
                    <td>Stories:</td>
                    <td>2</td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=1, pid=789)

        construction = buildings[0]["construction"]
        assert construction["style"] == "Colonial"
        assert construction["grade"] == "Good"
        assert construction["stories"] == "2"

    def test_parse_buildings_sub_areas(self):
        """Test parsing sub-areas table."""
        html = """
        <html>
            <span id="MainContent_ctl02_lblYearBuilt">1970</span>
            <table id="MainContent_ctl02_grdSub">
                <tr>
                    <th>Code</th>
                    <th>Description</th>
                    <th>Gross Area</th>
                    <th>Living Area</th>
                </tr>
                <tr>
                    <td>BLA</td>
                    <td>Basement Living Area</td>
                    <td>1,000</td>
                    <td>800</td>
                </tr>
                <tr>
                    <td>FLA</td>
                    <td>First Floor</td>
                    <td>1,200</td>
                    <td>1,200</td>
                </tr>
                <tr>
                    <td></td>
                    <td>Total</td>
                    <td>2,200</td>
                    <td>2,000</td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=1, pid=100)

        sub_areas = buildings[0]["sub_areas"]
        assert len(sub_areas) == 2  # Total row should be skipped
        assert sub_areas[0]["code"] == "BLA"
        assert sub_areas[0]["gross_area"] == 1000.0
        assert sub_areas[0]["living_area"] == 800.0
        assert sub_areas[1]["code"] == "FLA"


class TestParseTableRows:
    """Test generic table parsing."""

    def test_parse_table_rows_basic(self):
        """Test parsing a simple table."""
        html = """
        <html>
            <table id="test_table">
                <tr>
                    <th>Name</th>
                    <th>Value</th>
                    <th>Amount</th>
                </tr>
                <tr>
                    <td>Item 1</td>
                    <td>100</td>
                    <td>$50.00</td>
                </tr>
                <tr>
                    <td>Item 2</td>
                    <td>200</td>
                    <td>$75.50</td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(soup, "test_table", money_fields=["amount"])

        assert len(result) == 2
        assert result[0]["name"] == "Item 1"
        assert result[0]["value"] == "100"
        assert result[0]["amount"] == 50.0
        assert result[1]["amount"] == 75.5

    def test_parse_table_rows_no_data(self):
        """Test parsing table with 'No Data' message."""
        html = """
        <html>
            <table id="test_table">
                <tr><td>No Data</td></tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(soup, "test_table")

        assert result == []

    def test_parse_table_rows_missing_table(self):
        """Test parsing when table doesn't exist."""
        html = "<html></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(soup, "nonexistent_table")

        assert result == []

    def test_parse_table_rows_ownership(self):
        """Test parsing ownership/sales table."""
        html = """
        <html>
            <table id="MainContent_grdSales">
                <tr>
                    <th>Owner</th>
                    <th>Sale Price</th>
                    <th>Sale Date</th>
                    <th>Book and Page</th>
                </tr>
                <tr>
                    <td>John Smith</td>
                    <td>$300,000</td>
                    <td>2020-05-15</td>
                    <td>123/456</td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(
            soup, "MainContent_grdSales", money_fields=["sale_price"]
        )

        assert len(result) == 1
        assert result[0]["owner"] == "John Smith"
        assert result[0]["sale_price"] == 300000.0
        assert result[0]["sale_date"] == "2020-05-15"

    def test_parse_table_rows_appraisals(self):
        """Test parsing appraisal history table."""
        html = """
        <html>
            <table id="MainContent_grdHistoryValuesAppr">
                <tr>
                    <th>Valuation Year</th>
                    <th>Improvements</th>
                    <th>Land</th>
                    <th>Total</th>
                </tr>
                <tr>
                    <td>2023</td>
                    <td>$200,000</td>
                    <td>$100,000</td>
                    <td>$300,000</td>
                </tr>
                <tr>
                    <td>2022</td>
                    <td>$190,000</td>
                    <td>$95,000</td>
                    <td>$285,000</td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(
            soup,
            "MainContent_grdHistoryValuesAppr",
            money_fields=["improvements", "land", "total"],
        )

        assert len(result) == 2
        assert result[0]["valuation_year"] == "2023"
        assert result[0]["improvements"] == 200000.0
        assert result[0]["land"] == 100000.0
        assert result[0]["total"] == 300000.0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_parse_property_whitespace_handling(self):
        """Test that whitespace is properly handled."""
        html = """
        <html>
            <span id="MainContent_lblPid">  123  </span>
            <span id="MainContent_lblAcctNum">  ACC123  </span>
            <span id="lblTownName">  New Haven  </span>
            <span id="MainContent_lblLocation">
                100 Main St
            </span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        # Whitespace should be stripped
        assert result["town_name"] == "New Haven"
        assert result["address"] == "100 Main St"

    def test_parse_buildings_zero_count(self):
        """Test parsing when building_count is 0."""
        html = "<html></html>"
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=0, pid=123)

        # Should return empty list or handle gracefully
        assert isinstance(buildings, list)

    def test_parse_buildings_none_count(self):
        """Test parsing when building_count is None."""
        html = "<html></html>"
        soup = BeautifulSoup(html, "html.parser")
        buildings = parse_buildings(soup, building_count=None, pid=123)

        # Should handle None gracefully
        assert isinstance(buildings, list)

    def test_parse_table_rows_empty_cells(self):
        """Test parsing table with empty cells."""
        html = """
        <html>
            <table id="test_table">
                <tr>
                    <th>Name</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td></td>
                    <td>100</td>
                </tr>
                <tr>
                    <td>Item 2</td>
                    <td></td>
                </tr>
            </table>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_table_rows(soup, "test_table")

        assert len(result) == 2
        # Empty cells should be cleaned to None
        assert result[0]["name"] is None
        assert result[1]["value"] is None

    def test_parse_property_land_size_alternate_id(self):
        """Test that alternate land size ID is handled."""
        html = """
        <html>
            <span id="MainContent_lblPid">123</span>
            <span id="MainContent_lblAcctNum">ACC123</span>
            <span id="lblTownName">Test</span>
            <span id="MainContent_lblLndAcres">5.25</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        assert result["land_size_acres"] == 5.25


class TestExtraFields:
    """Test extra_fields capture for unknown spans and construction keys."""

    def test_parse_property_captures_unknown_spans(self):
        """Unknown MainContent_lbl* spans are captured in extra_fields."""
        html = """
        <html>
            <span id="MainContent_lblPid">123</span>
            <span id="MainContent_lblAcctNum">ACC123</span>
            <span id="lblTownName">Test Town</span>
            <span id="MainContent_lblNewField">surprise value</span>
            <span id="MainContent_lblAnotherNew">another</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        assert result["extra_fields"] is not None
        extra = json.loads(result["extra_fields"])
        assert extra["MainContent_lblNewField"] == "surprise value"
        assert extra["MainContent_lblAnotherNew"] == "another"

    def test_parse_property_no_extra_fields_when_all_known(self):
        """extra_fields is None when no unknown spans are present."""
        html = """
        <html>
            <span id="MainContent_lblPid">123</span>
            <span id="MainContent_lblAcctNum">ACC123</span>
            <span id="lblTownName">Test Town</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        assert result["extra_fields"] is None

    def test_parse_property_ignores_empty_unknown_spans(self):
        """Empty unknown spans are not included in extra_fields."""
        html = """
        <html>
            <span id="MainContent_lblPid">123</span>
            <span id="MainContent_lblAcctNum">ACC123</span>
            <span id="lblTownName">Test Town</span>
            <span id="MainContent_lblEmptyNew"></span>
            <span id="MainContent_lblRealNew">has value</span>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = parse_property(soup, 123)

        extra = json.loads(result["extra_fields"])
        assert "MainContent_lblEmptyNew" not in extra
        assert extra["MainContent_lblRealNew"] == "has value"

    def test_flatten_building_captures_unknown_construction_keys(self):
        """Unknown construction detail keys go into building extra_fields."""
        results = [
            {
                "property": {
                    "uuid": "test-ef",
                    "pid": 1,
                    "town_name": "Test",
                },
                "buildings": [
                    {
                        "property_uuid": "test-ef",
                        "pid": 1,
                        "bid": 0,
                        "year_built": 1950,
                        "construction": {
                            "style": "Colonial",
                            "brand_new_field": "unknown_value",
                            "another_new": "42",
                        },
                        "sub_areas": [],
                    }
                ],
            }
        ]
        tables = flatten_vgsi(results)
        building = tables["buildings"][0]

        assert building["style"] == "Colonial"
        assert "extra_fields" in building
        extra = json.loads(building["extra_fields"])
        assert extra["brand_new_field"] == "unknown_value"
        assert extra["another_new"] == "42"

    def test_flatten_building_no_extra_when_all_known(self):
        """No extra_fields when all construction keys are in CNS_MAPPING."""
        results = [
            {
                "property": {
                    "uuid": "test-no-ef",
                    "pid": 2,
                    "town_name": "Test",
                },
                "buildings": [
                    {
                        "property_uuid": "test-no-ef",
                        "pid": 2,
                        "bid": 0,
                        "year_built": 1960,
                        "construction": {"style": "Ranch"},
                        "sub_areas": [],
                    }
                ],
            }
        ]
        tables = flatten_vgsi(results)
        building = tables["buildings"][0]

        assert building.get("extra_fields") is None
