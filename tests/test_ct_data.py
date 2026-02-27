"""
Tests for the CT Data source (Connecticut Business Registry).

Tests flatten logic, iteration, fetch logic with mocked HTTP,
and integration with ParquetWriter.
"""

import shutil
import tempfile
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from scrapers.ct_data.source import (
    CT_DATA_SOURCE,
    DATASETS,
    InvalidDatasetException,
    flatten_ct_data,
    get_known_entry_ids,
    make_load_iter,
)
from src.engine.database import ParquetWriter
from src.engine.hash import compute_row_hash


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def writer(temp_dir):
    w = ParquetWriter("ct_data", CT_DATA_SOURCE, temp_dir)
    yield w
    w.close()


class TestFlattenCtData:
    """Test the flatten function."""

    def test_flatten_single_table(self):
        results = [
            {
                "dataset_id": "n7gp-d28j",
                "table_name": "businesses",
                "rows": [
                    {"business_id": "001", "name": "Test Corp"},
                    {"business_id": "002", "name": "Test Inc"},
                ],
            }
        ]
        tables = flatten_ct_data(results)
        assert "businesses" in tables
        assert len(tables["businesses"]) == 2

    def test_flatten_multiple_tables(self):
        results = [
            {
                "dataset_id": "n7gp-d28j",
                "table_name": "businesses",
                "rows": [{"business_id": "001", "name": "Test"}],
            },
            {
                "dataset_id": "enwv-52we",
                "table_name": "name_changes",
                "rows": [{"unique_key": "UK1", "business_name_old": "Old"}],
            },
        ]
        tables = flatten_ct_data(results)
        assert "businesses" in tables
        assert "name_changes" in tables

    def test_flatten_empty_rows(self):
        results = [
            {"dataset_id": "n7gp-d28j", "table_name": "businesses", "rows": []}
        ]
        tables = flatten_ct_data(results)
        assert "businesses" not in tables

    def test_flatten_merges_same_table(self):
        results = [
            {
                "dataset_id": "n7gp-d28j",
                "table_name": "businesses",
                "rows": [{"business_id": "001"}],
            },
            {
                "dataset_id": "n7gp-d28j",
                "table_name": "businesses",
                "rows": [{"business_id": "002"}],
            },
        ]
        tables = flatten_ct_data(results)
        assert len(tables["businesses"]) == 2


class TestParquetWrite:
    """Test writing CT Data through ParquetWriter."""

    def test_write_businesses(self, writer, temp_dir):
        rows = [
            {"business_id": "001", "name": "Test Corp", "status": "Active"},
            {"business_id": "002", "name": "Beta Inc", "status": "Forfeited"},
        ]
        result = [{"dataset_id": "n7gp-d28j", "table_name": "businesses", "rows": rows}]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/ct_data/businesses/*.parquet')"
            ).fetchone()
        finally:
            conn.close()
        assert result is not None
        count = result[0]
        assert count == 2

    def test_write_name_changes(self, writer, temp_dir):
        rows = [
            {
                "unique_key": "UK1",
                "business_name_old": "Old1",
                "business_name_new": "New1",
            },
        ]
        result = [{"dataset_id": "enwv-52we", "table_name": "name_changes", "rows": rows}]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT unique_key, business_name_new "
                f"FROM read_parquet('{temp_dir}/ct_data/name_changes/*.parquet')"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] == "UK1"
        assert row[1] == "New1"

    def test_rows_have_metadata(self, writer, temp_dir):
        """Each row gets scraped_at and row_hash."""
        rows = [{"business_id": "001", "name": "Test"}]
        result = [{"dataset_id": "n7gp-d28j", "table_name": "businesses", "rows": rows}]
        writer.write_batch(result)

        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT scraped_at, row_hash "
                f"FROM read_parquet('{temp_dir}/ct_data/businesses/*.parquet')"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] is not None
        assert row[1] is not None


class TestRowHash:
    """Test row hash computation."""

    def test_hash_excludes_metadata(self):
        data1 = {"business_id": "001", "name": "Test"}
        data2 = {
            "business_id": "001",
            "name": "Test",
            "scraped_at": "2025-01-01",
            "row_hash": "abc",
        }
        assert compute_row_hash(data1) == compute_row_hash(data2)

    def test_hash_changes_on_data_change(self):
        data1 = {"business_id": "001", "name": "Old Name"}
        data2 = {"business_id": "001", "name": "New Name"}
        assert compute_row_hash(data1) != compute_row_hash(data2)


class TestIteration:
    """Test make_load_iter and get_known_entry_ids."""

    def test_make_load_iter_all(self):
        iter_fn = make_load_iter()
        ids = list(iter_fn("http://example.com/", "somedir", "ct_data"))
        assert set(ids) == set(DATASETS.keys())

    def test_make_load_iter_specific(self):
        iter_fn = make_load_iter(["n7gp-d28j", "enwv-52we"])
        ids = list(iter_fn("http://example.com/", "somedir", "ct_data"))
        assert ids == ["n7gp-d28j", "enwv-52we"]

    def test_get_known_entry_ids_empty(self, temp_dir):
        ids = get_known_entry_ids(temp_dir, "ct_data")
        assert ids == []

    def test_get_known_entry_ids_with_data(self, writer, temp_dir):
        rows = [{"business_id": "001", "name": "Test", "status": "Active"}]
        writer.write_batch(
            [{"dataset_id": "n7gp-d28j", "table_name": "businesses", "rows": rows}]
        )

        ids = get_known_entry_ids(temp_dir, "ct_data")
        assert "n7gp-d28j" in ids


class TestFetchDataset:
    """Test fetch_dataset with mocked HTTP."""

    @patch("scrapers.ct_data.source.curl_requests")
    def test_fetch_single_page(self, mock_curl):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"business_id": "001", "name": "Test"},
            {"business_id": "002", "name": "Test2"},
        ]
        mock_curl.get.return_value = mock_response

        from scrapers.ct_data.source import fetch_dataset

        result = fetch_dataset("https://data.ct.gov/resource/", "n7gp-d28j")

        assert result["dataset_id"] == "n7gp-d28j"
        assert result["table_name"] == "businesses"
        assert len(result["rows"]) == 2

    @patch("scrapers.ct_data.source.curl_requests")
    def test_fetch_paginates(self, mock_curl):
        from scrapers.ct_data.source import PAGE_SIZE, fetch_dataset

        page1 = [{"business_id": str(i)} for i in range(PAGE_SIZE)]
        page2 = [{"business_id": "last"}]

        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = page1

        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = page2

        mock_curl.get.side_effect = [mock_response1, mock_response2]

        result = fetch_dataset("https://data.ct.gov/resource/", "n7gp-d28j")
        assert len(result["rows"]) == PAGE_SIZE + 1
        assert mock_curl.get.call_count == 2

    @patch("scrapers.ct_data.source.curl_requests")
    def test_fetch_unknown_dataset_raises(self, mock_curl):
        from scrapers.ct_data.source import fetch_dataset

        with pytest.raises(InvalidDatasetException, match="Unknown dataset"):
            fetch_dataset("https://data.ct.gov/resource/", "invalid-id")

    @patch("scrapers.ct_data.source.curl_requests")
    def test_fetch_api_error_raises(self, mock_curl):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_curl.get.return_value = mock_response

        from scrapers.ct_data.source import fetch_dataset

        with pytest.raises(InvalidDatasetException, match="HTTP 500"):
            fetch_dataset("https://data.ct.gov/resource/", "n7gp-d28j")


class TestFetchRetry:
    """Test retry logic in _fetch_page_with_retry."""

    @patch("scrapers.ct_data.source.time.sleep")
    @patch("scrapers.ct_data.source.curl_requests")
    def test_retries_on_connection_error(self, mock_curl, mock_sleep):
        from scrapers.ct_data.source import fetch_dataset

        error_response = MagicMock()
        error_response.side_effect = ConnectionError("connection refused")

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = [{"business_id": "001"}]

        mock_curl.get.side_effect = [
            ConnectionError("connection refused"),
            success_response,
        ]

        result = fetch_dataset("https://data.ct.gov/resource/", "n7gp-d28j")
        assert len(result["rows"]) == 1
        assert mock_curl.get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("scrapers.ct_data.source.time.sleep")
    @patch("scrapers.ct_data.source.curl_requests")
    def test_raises_after_max_retries(self, mock_curl, mock_sleep):
        from scrapers.ct_data.source import fetch_dataset

        mock_curl.get.side_effect = ConnectionError("connection refused")

        with pytest.raises(ConnectionError):
            fetch_dataset("https://data.ct.gov/resource/", "n7gp-d28j")

        assert mock_curl.get.call_count == 4  # 1 initial + 3 retries

    @patch("scrapers.ct_data.source.curl_requests")
    def test_no_retry_on_invalid_dataset(self, mock_curl):
        """InvalidDatasetException should not be retried."""
        from scrapers.ct_data.source import fetch_dataset

        with pytest.raises(InvalidDatasetException):
            fetch_dataset("https://data.ct.gov/resource/", "bad-id")

        mock_curl.get.assert_not_called()


class TestFetchDatasetIncremental:
    """Test incremental fetching."""

    @patch("scrapers.ct_data.source.curl_requests")
    def test_incremental_adds_where_clause(self, mock_curl):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"business_id": "001"}]
        mock_curl.get.return_value = mock_response

        from scrapers.ct_data.source import fetch_dataset_incremental

        fetch_dataset_incremental(
            "https://data.ct.gov/resource/", "n7gp-d28j", "2026-01-01"
        )

        call_args = mock_curl.get.call_args
        params = call_args[1]["params"]
        assert "$where" in params
        assert "create_dt > '2026-01-01'" in params["$where"]

    @patch("scrapers.ct_data.source.curl_requests")
    def test_incremental_name_changes_does_full_fetch(self, mock_curl):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"unique_key": "UK1"}]
        mock_curl.get.return_value = mock_response

        from scrapers.ct_data.source import fetch_dataset_incremental

        fetch_dataset_incremental(
            "https://data.ct.gov/resource/", "enwv-52we", "2026-01-01"
        )

        call_args = mock_curl.get.call_args
        params = call_args[1]["params"]
        assert "$where" not in params


class TestIntegration:
    """Integration tests: scrape -> write -> query via parquet."""

    @patch.object(CT_DATA_SOURCE, "scrape_fn")
    def test_full_load_workflow(self, mock_scrape, temp_dir):
        from src.engine import run_load

        def fake_fetch(base_url, dataset_id):
            table_name = DATASETS[dataset_id]
            if table_name == "businesses":
                return {
                    "dataset_id": dataset_id,
                    "table_name": "businesses",
                    "rows": [
                        {"business_id": "B1", "name": "Alpha Corp", "status": "Active"},
                        {"business_id": "B2", "name": "Beta Inc", "status": "Forfeited"},
                    ],
                }
            return {"dataset_id": dataset_id, "table_name": table_name, "rows": []}

        mock_scrape.side_effect = fake_fetch

        writer = ParquetWriter("ct_data", CT_DATA_SOURCE, temp_dir)
        iter_fn = make_load_iter(["n7gp-d28j"])
        count = run_load(
            scope_key="ct_data",
            writer=writer,
            source=CT_DATA_SOURCE,
            base_url="https://data.ct.gov/resource/",
            iter_entries_fn=iter_fn,
            max_workers=1,
            show_progress=False,
        )

        assert count == 1

        conn = duckdb.connect()
        try:
            biz_count_row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/ct_data/businesses/*.parquet')"
            ).fetchone()
            assert biz_count_row is not None
            biz_count = biz_count_row[0]
            assert biz_count == 2

            active = conn.execute(
                f"SELECT name FROM read_parquet('{temp_dir}/ct_data/businesses/*.parquet') "
                f"WHERE status = 'Active'"
            ).fetchone()
            assert active is not None
            assert active[0] == "Alpha Corp"
        finally:
            conn.close()

    @patch.object(CT_DATA_SOURCE, "scrape_fn")
    def test_refresh_appends_new_data(self, mock_scrape, temp_dir):
        """Refresh appends new rows; changes detected at query time."""
        from src.engine import run_load, run_refresh

        mock_scrape.side_effect = lambda url, did: {
            "dataset_id": did,
            "table_name": DATASETS[did],
            "rows": [{"business_id": "R1", "name": "Original", "status": "Active"}]
            if DATASETS[did] == "businesses"
            else [],
        }

        writer = ParquetWriter("ct_data", CT_DATA_SOURCE, temp_dir)
        iter_fn = make_load_iter(["n7gp-d28j"])
        run_load(
            scope_key="ct_data",
            writer=writer,
            source=CT_DATA_SOURCE,
            base_url="https://data.ct.gov/resource/",
            iter_entries_fn=iter_fn,
            max_workers=1,
            show_progress=False,
        )

        # Refresh with changed name
        mock_scrape.side_effect = lambda url, did: {
            "dataset_id": did,
            "table_name": DATASETS[did],
            "rows": [{"business_id": "R1", "name": "Updated", "status": "Active"}]
            if DATASETS[did] == "businesses"
            else [],
        }

        writer = ParquetWriter("ct_data", CT_DATA_SOURCE, temp_dir)
        run_refresh(
            scope_key="ct_data",
            writer=writer,
            source=CT_DATA_SOURCE,
            base_url="https://data.ct.gov/resource/",
            max_workers=1,
            show_progress=False,
        )

        # Both versions should be in parquet (append-only)
        conn = duckdb.connect()
        try:
            rows = conn.execute(
                f"SELECT name FROM read_parquet('{temp_dir}/ct_data/businesses/*.parquet') "
                f"WHERE business_id = 'R1' ORDER BY scraped_at"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2
        assert rows[0][0] == "Original"
        assert rows[1][0] == "Updated"
