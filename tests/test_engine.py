"""
Tests for the generic parallel scraping engine.

Tests rate limiting, concurrent execution, and integration with ParquetWriter.
"""

import shutil
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from src.engine import ParquetWriter, RateLimiter, run_load, run_refresh
from scrapers.vgsi.source import (
    VGSI_SOURCE,
    InvalidEntryException,
    get_property_history,
    make_load_iter,
)


class TestRateLimiter:
    """Test the RateLimiter class."""

    def test_rate_limiter_basic(self):
        limiter = RateLimiter(max_workers=2, requests_per_second=10)
        start = time.time()
        with limiter.acquire():
            pass
        assert time.time() - start < 0.5

    def test_rate_limiter_enforces_interval(self):
        limiter = RateLimiter(max_workers=5, requests_per_second=2)
        times = []
        for _ in range(3):
            start = time.time()
            with limiter.acquire():
                times.append(time.time() - start)
        assert times[0] < 0.1
        assert times[1] >= 0.4
        assert times[2] >= 0.4

    def test_rate_limiter_concurrent(self):
        limiter = RateLimiter(max_workers=2, requests_per_second=10)
        counter = {"value": 0}
        lock = threading.Lock()

        def worker():
            with limiter.acquire():
                with lock:
                    counter["value"] += 1
                time.sleep(0.1)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start

        assert counter["value"] == 5
        assert elapsed >= 0.2

    def test_rate_limiter_stats(self):
        limiter = RateLimiter(max_workers=5, requests_per_second=10)
        for _ in range(5):
            with limiter.acquire():
                pass
        stats = limiter.get_stats()
        assert stats["total_requests"] == 5
        assert stats["total_wait_time"] >= 0

    def test_rate_limiter_zero_rps(self):
        limiter = RateLimiter(max_workers=5, requests_per_second=0)
        start = time.time()
        for _ in range(3):
            with limiter.acquire():
                pass
        assert time.time() - start < 0.5


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _make_writer(temp_dir):
    return ParquetWriter("testcity", VGSI_SOURCE, temp_dir)


class TestRunLoad:
    """Test the parallel loading function."""

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_basic(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 5)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            requests_per_second=10,
            checkpoint_every=2,
            batch_size=2,
            show_progress=False,
        )

        assert count == 5
        assert mock_scrape.call_count == 5

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_handles_invalid_entry(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            if pid == 3:
                raise InvalidEntryException(f"PID {pid} doesn't exist")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 5)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            batch_size=10,
            show_progress=False,
        )

        assert count == 4

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_handles_errors(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            if pid == 2:
                raise Exception("Network error")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 4)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            batch_size=10,
            show_progress=False,
        )

        assert count == 3

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_checkpoints(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 10)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            checkpoint_every=3,
            batch_size=5,
            show_progress=False,
        )

        assert count == 10

        # Verify checkpoint was saved (JSON file)
        checkpoint_path = Path(temp_dir) / "_checkpoints" / "testcity.json"
        assert checkpoint_path.exists()

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_resume(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = fake_scrape

        # Set up a checkpoint
        writer = _make_writer(temp_dir)
        writer.save_checkpoint("testcity", last_entry_id=5, total_scraped=5)
        writer.close()

        # Resume from checkpoint
        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 10)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            resume_from_checkpoint=True,
            batch_size=10,
            show_progress=False,
        )

        # Should only scrape entry IDs 6-10
        assert count == 5

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_run_load_batching(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                    "address": f"{pid} Main St",
                },
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 10)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            batch_size=3,
            show_progress=False,
        )

        assert count == 10

        # Verify all data was written to parquet
        conn = duckdb.connect()
        try:
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/testcity/properties/*.parquet')"
            ).fetchone()
        finally:
            conn.close()
        assert result is not None
        assert result[0] == 10

    @patch.object(VGSI_SOURCE, "scrape_fn")
    @patch.object(VGSI_SOURCE, "download_fn")
    def test_run_load_with_photos(self, mock_download, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [
                    {
                        "property_uuid": f"test-{pid}",
                        "pid": pid,
                        "bid": 0,
                        "photo_url": f"https://example.com/photo{pid}.jpg",
                        "construction": {},
                        "sub_areas": [],
                    }
                ],
            }

        mock_scrape.side_effect = fake_scrape
        mock_download.return_value = "/tmp/photo.jpg"

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 3)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=2,
            download_photos=True,
            photo_dir="/tmp/photos",
            show_progress=False,
        )

        assert count == 3
        assert mock_download.call_count == 3


class TestIntegration:
    """Integration tests with real parquet operations."""

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_full_workflow_integration(self, mock_scrape, temp_dir):
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "New Haven",
                    "address": f"{pid} Main St",
                    "assessment_value": 100000.0 + (pid * 10000.0),
                },
                "buildings": [
                    {
                        "property_uuid": f"test-{pid}",
                        "pid": pid,
                        "bid": 0,
                        "year_built": 1950 + pid,
                        "building_area": 2000.0,
                        "construction": {},
                        "sub_areas": [],
                    }
                ],
            }

        mock_scrape.side_effect = fake_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 10)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=3,
            show_progress=False,
        )

        assert count == 10

        conn = duckdb.connect()
        try:
            prop_count_row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/testcity/properties/*.parquet')"
            ).fetchone()
            assert prop_count_row is not None
            prop_count = prop_count_row[0]
            assert prop_count == 10

            bldg_count_row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/testcity/buildings/*.parquet')"
            ).fetchone()
            assert bldg_count_row is not None
            bldg_count = bldg_count_row[0]
            assert bldg_count == 10

            # Test query
            row = conn.execute(
                f"""
                SELECT p.pid, p.address, b.year_built
                FROM read_parquet('{temp_dir}/testcity/properties/*.parquet') p
                JOIN read_parquet('{temp_dir}/testcity/buildings/*.parquet') b
                    ON p.uuid = b.property_uuid
                WHERE p.pid = 5
                """
            ).fetchone()
            assert row is not None
            assert row[0] == 5
            assert row[1] == "5 Main St"
            assert row[2] == 1955

            # Test aggregation
            avg_row = conn.execute(
                f"SELECT AVG(assessment_value) FROM read_parquet('{temp_dir}/testcity/properties/*.parquet')"
            ).fetchone()
            assert avg_row is not None
            avg = avg_row[0]
            assert abs(avg - 155000.0) < 1.0
        finally:
            conn.close()


class TestRunRefresh:
    """Tests for run_refresh."""

    def _fake_result(self, pid, assessment_value=100000.0):
        return {
            "property": {
                "uuid": f"test-{pid}",
                "pid": pid,
                "town_name": "Test Town",
                "assessment_value": assessment_value,
            },
        }

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_scrapes_known_entry_ids(self, mock_scrape, temp_dir):
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid)

        # Seed with PIDs 1-3
        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 3)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        mock_scrape.reset_mock()

        writer = _make_writer(temp_dir)
        count = run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        assert count == 3
        scraped_pids = sorted(call.args[1] for call in mock_scrape.call_args_list)
        assert scraped_pids == [1, 2, 3]

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_empty_returns_zero(self, mock_scrape, temp_dir):
        writer = _make_writer(temp_dir)
        count = run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        assert count == 0
        mock_scrape.assert_not_called()

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_detects_changes(self, mock_scrape, temp_dir):
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid, 100000.0)
        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 2)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        # Refresh with changed value for PID 1
        def changed_scrape(url, pid):
            value = 999000.0 if pid == 1 else 100000.0
            return self._fake_result(pid, value)

        mock_scrape.side_effect = changed_scrape
        writer = _make_writer(temp_dir)
        run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        history = get_property_history(temp_dir, "testcity", "test-1")
        assert len(history) == 2
        assert list(history["version"]) == [1, 2]

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_no_change_no_new_version(self, mock_scrape, temp_dir):
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid, 100000.0)

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 2)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        writer = _make_writer(temp_dir)
        run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        history = get_property_history(temp_dir, "testcity", "test-1")
        assert len(history) == 1


class TestRefreshDedup:
    """Tests for refresh hash-based deduplication."""

    def _fake_result(self, pid, assessment_value=100000.0):
        return {
            "property": {
                "uuid": f"test-{pid}",
                "pid": pid,
                "town_name": "Test Town",
                "assessment_value": assessment_value,
            },
        }

    def _count_rows(self, temp_dir, table="properties"):
        conn = duckdb.connect()
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{temp_dir}/testcity/{table}/*.parquet')"
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_skips_unchanged_rows(self, mock_scrape, temp_dir):
        """Refresh with no changes should write zero new rows."""
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid)

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 3)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        initial_count = self._count_rows(temp_dir)
        assert initial_count == 3

        writer = _make_writer(temp_dir)
        run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        assert self._count_rows(temp_dir) == initial_count

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_writes_only_changed_rows(self, mock_scrape, temp_dir):
        """Refresh should only write rows that actually changed."""
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid, 100000.0)

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 3)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        initial_count = self._count_rows(temp_dir)

        # Change only PID 2
        def changed_scrape(url, pid):
            value = 999000.0 if pid == 2 else 100000.0
            return self._fake_result(pid, value)

        mock_scrape.side_effect = changed_scrape
        writer = _make_writer(temp_dir)
        run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        # Only 1 new row (changed PID 2)
        assert self._count_rows(temp_dir) == initial_count + 1

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_refresh_write_stats(self, mock_scrape, temp_dir):
        """Writer tracks rows written vs skipped."""
        mock_scrape.side_effect = lambda url, pid: self._fake_result(pid, 100000.0)

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 3)
        run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            show_progress=False,
        )

        # Refresh with 1 change
        def changed_scrape(url, pid):
            value = 999000.0 if pid == 1 else 100000.0
            return self._fake_result(pid, value)

        mock_scrape.side_effect = changed_scrape
        writer = _make_writer(temp_dir)
        run_refresh(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            show_progress=False,
        )

        stats = writer.get_write_stats()
        assert stats["rows_written"] == 1
        assert stats["rows_skipped"] == 2

    def test_preload_hashes_empty_scope(self, temp_dir):
        """preload_hashes on empty dir doesn't crash."""
        writer = _make_writer(temp_dir)
        writer.preload_hashes()
        assert writer._existing_hashes == {}


class TestErrorThreshold:
    """Tests for consecutive error abort behavior."""

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_load_aborts_on_consecutive_errors(self, mock_scrape, temp_dir):
        """run_load stops early when consecutive error threshold is hit."""
        mock_scrape.side_effect = Exception("network down")

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 20)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=1,
            batch_size=10,
            show_progress=False,
            max_consecutive_errors=5,
        )

        assert count == 0

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_load_resets_consecutive_on_success(self, mock_scrape, temp_dir):
        """Consecutive error counter resets when a scrape succeeds."""
        call_count = {"n": 0}

        def flaky_scrape(base_url, pid):
            call_count["n"] += 1
            # Fail every other call
            if call_count["n"] % 2 == 0:
                raise Exception("transient error")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
            }

        mock_scrape.side_effect = flaky_scrape

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 10)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=1,
            batch_size=10,
            show_progress=False,
            max_consecutive_errors=3,
        )

        # With alternating success/fail and single worker, consecutive never hits 3
        assert count > 0

    @patch.object(VGSI_SOURCE, "scrape_fn")
    def test_load_no_threshold_when_zero(self, mock_scrape, temp_dir):
        """max_consecutive_errors=0 disables the threshold."""
        mock_scrape.side_effect = Exception("network down")

        writer = _make_writer(temp_dir)
        iter_fn = make_load_iter(1, 5)
        count = run_load(
            scope_key="testcity",
            writer=writer,
            source=VGSI_SOURCE,
            base_url="https://example.com/",
            iter_entries_fn=iter_fn,
            max_workers=1,
            batch_size=10,
            show_progress=False,
            max_consecutive_errors=0,
        )

        # All entries attempted, all failed
        assert count == 0
        assert mock_scrape.call_count == 5
