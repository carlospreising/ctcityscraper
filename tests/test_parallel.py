"""
Tests for parallel scraping functionality.

Tests rate limiting, concurrent execution, and integration with database.
"""

import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.vgsi.parallel import RateLimiter, load_city_parallel


class TestRateLimiter:
    """Test the RateLimiter class."""

    def test_rate_limiter_basic(self):
        """Test basic rate limiter functionality."""
        limiter = RateLimiter(max_workers=2, requests_per_second=10)

        start = time.time()
        with limiter.acquire():
            pass
        elapsed = time.time() - start

        # Should acquire immediately on first request
        assert elapsed < 0.5

    def test_rate_limiter_enforces_interval(self):
        """Test that rate limiter enforces minimum interval."""
        limiter = RateLimiter(max_workers=5, requests_per_second=2)  # 0.5s interval

        times = []
        for _ in range(3):
            start = time.time()
            with limiter.acquire():
                times.append(time.time() - start)

        # First request should be immediate
        assert times[0] < 0.1

        # Subsequent requests should wait ~0.5s each
        # Allow some tolerance for timing variations
        assert times[1] >= 0.4
        assert times[2] >= 0.4

    def test_rate_limiter_concurrent(self):
        """Test rate limiter with concurrent threads."""
        limiter = RateLimiter(max_workers=2, requests_per_second=10)

        counter = {"value": 0}
        lock = threading.Lock()

        def worker():
            with limiter.acquire():
                with lock:
                    counter["value"] += 1
                time.sleep(0.1)  # Simulate work

        threads = [threading.Thread(target=worker) for _ in range(5)]
        start = time.time()

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        elapsed = time.time() - start

        # All workers should complete
        assert counter["value"] == 5

        # With 2 max workers and 0.1s work, should take at least 0.2s
        # (5 tasks / 2 workers = 3 batches, 3 * 0.1s = 0.3s minimum)
        assert elapsed >= 0.2

    def test_rate_limiter_stats(self):
        """Test rate limiter statistics tracking."""
        limiter = RateLimiter(max_workers=5, requests_per_second=10)

        # Make several requests
        for _ in range(5):
            with limiter.acquire():
                pass

        stats = limiter.get_stats()

        assert stats["total_requests"] == 5
        assert stats["total_wait_time"] >= 0
        assert stats["avg_wait_time"] >= 0

    def test_rate_limiter_zero_rps(self):
        """Test rate limiter with no rate limit (0 requests per second)."""
        limiter = RateLimiter(max_workers=5, requests_per_second=0)

        start = time.time()
        for _ in range(3):
            with limiter.acquire():
                pass
        elapsed = time.time() - start

        # With no rate limit, should complete very quickly
        assert elapsed < 0.5


class TestLoadCityParallel:
    """Test the parallel city loading function."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database."""
        temp_dir = tempfile.mkdtemp()
        db_path = str(Path(temp_dir) / "test.duckdb")
        yield db_path
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_load_city_parallel_requires_pid_max(self, temp_db):
        """Test that pid_max is required."""
        with pytest.raises(ValueError, match="pid_max is required"):
            load_city_parallel(
                city="testcity",
                db_path=temp_db,
                base_url="https://example.com/",
                pid_min=1,
                pid_max=None,
            )

    def test_load_city_parallel_validates_city(self, temp_db):
        """Test that city must exist in database or base_url provided."""
        with pytest.raises(ValueError, match="not found in database"):
            load_city_parallel(
                city="nonexistent",
                db_path=temp_db,
                base_url=None,
                pid_min=1,
                pid_max=10,
            )

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_basic(self, mock_scrape, temp_db):
        """Test basic parallel scraping with mocked scrape_property."""

        # Mock scrape_property to return fake data
        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Run parallel scraper
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=5,
            max_workers=2,
            requests_per_second=10,
            checkpoint_every=2,
            batch_size=2,
            show_progress=False,
        )

        # Should scrape all 5 properties
        assert count == 5

        # Verify scrape_property was called 5 times
        assert mock_scrape.call_count == 5

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_handles_invalid_pid(self, mock_scrape, temp_db):
        """Test handling of invalid PIDs."""
        from src.vgsi.scraper import InvalidPIDException

        def fake_scrape(base_url, pid):
            if pid == 3:
                raise InvalidPIDException(f"PID {pid} doesn't exist")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Should continue despite invalid PID
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=5,
            max_workers=2,
            batch_size=10,
            show_progress=False,
        )

        # Should scrape 4 valid properties (1-2, 4-5)
        assert count == 4

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_handles_errors(self, mock_scrape, temp_db):
        """Test handling of scraping errors."""

        def fake_scrape(base_url, pid):
            if pid == 2:
                raise Exception("Network error")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Should continue despite error
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=4,
            max_workers=2,
            batch_size=10,
            show_progress=False,
        )

        # Should scrape 3 valid properties (1, 3, 4)
        assert count == 3

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_checkpoints(self, mock_scrape, temp_db):
        """Test checkpoint functionality."""

        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # First run: scrape 1-10 with checkpoint every 3
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=10,
            max_workers=2,
            checkpoint_every=3,
            batch_size=5,
            show_progress=False,
        )

        assert count == 10

        # Verify checkpoint was saved
        import duckdb

        conn = duckdb.connect(temp_db)
        result = conn.execute(
            "SELECT last_pid, total_scraped FROM main.scrape_checkpoints "
            "WHERE city = 'testcity' ORDER BY checkpoint_time DESC LIMIT 1"
        ).fetchone()
        conn.close()

        assert result is not None
        assert result[0] == 10  # last_pid
        assert result[1] == 10  # total_scraped

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_resume(self, mock_scrape, temp_db):
        """Test resume from checkpoint."""
        from src.vgsi.database import DuckDBWriter

        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Set up a checkpoint manually
        db_writer = DuckDBWriter("testcity", temp_db)
        db_writer.save_checkpoint("testcity", last_pid=5, total_scraped=5)
        db_writer.close()

        # Resume from checkpoint
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,  # Will be overridden by checkpoint
            pid_max=10,
            max_workers=2,
            resume_from_checkpoint=True,
            batch_size=10,
            show_progress=False,
        )

        # Should only scrape PIDs 6-10 (5 properties)
        assert count == 5

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_batching(self, mock_scrape, temp_db):
        """Test that batching works correctly."""

        def fake_scrape(base_url, pid):
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                    "address": f"{pid} Main St",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Scrape with small batch size
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=10,
            max_workers=2,
            batch_size=3,  # Small batch size
            show_progress=False,
        )

        assert count == 10

        # Verify all data was written
        import duckdb

        conn = duckdb.connect(temp_db)
        result = conn.execute("SELECT COUNT(*) FROM testcity.properties").fetchone()
        conn.close()

        assert result[0] == 10

    @patch("src.vgsi.parallel.scrape_property")
    def test_load_city_parallel_url_normalization(self, mock_scrape, temp_db):
        """Test that URLs are normalized (trailing slash added)."""

        def fake_scrape(base_url, pid):
            # Check that URL has trailing slash
            assert base_url.endswith("/")
            return {
                "property": {
                    "uuid": f"test-{pid}",
                    "pid": pid,
                    "town_name": "Test Town",
                },
                "buildings": [],
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Provide URL without trailing slash
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com",  # No trailing slash
            pid_min=1,
            pid_max=3,
            max_workers=2,
            show_progress=False,
        )

        assert count == 3

    @patch("src.vgsi.parallel.scrape_property")
    @patch("src.vgsi.parallel.download_photo")
    def test_load_city_parallel_with_photos(self, mock_download, mock_scrape, temp_db):
        """Test photo downloading functionality."""

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
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape
        mock_download.return_value = "/tmp/photo.jpg"

        # Run with photo downloading enabled
        count = load_city_parallel(
            city="testcity",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=3,
            max_workers=2,
            download_photos=True,
            photo_dir="/tmp/photos",
            show_progress=False,
        )

        assert count == 3

        # Verify download_photo was called
        assert mock_download.call_count == 3


class TestIntegration:
    """Integration tests with real database operations."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database."""
        temp_dir = tempfile.mkdtemp()
        db_path = str(Path(temp_dir) / "test.duckdb")
        yield db_path
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    @patch("src.vgsi.parallel.scrape_property")
    def test_full_workflow_integration(self, mock_scrape, temp_db):
        """Test complete workflow: scrape, write, query."""

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
                "ownership": [],
                "appraisals": [],
                "assessments": [],
                "extra_features": [],
                "outbuildings": [],
            }

        mock_scrape.side_effect = fake_scrape

        # Scrape data
        count = load_city_parallel(
            city="newhaven",
            db_path=temp_db,
            base_url="https://example.com/",
            pid_min=1,
            pid_max=10,
            max_workers=3,
            show_progress=False,
        )

        assert count == 10

        # Query the database
        import duckdb

        conn = duckdb.connect(temp_db)

        # Check property count
        result = conn.execute("SELECT COUNT(*) FROM newhaven.properties").fetchone()
        assert result[0] == 10

        # Check building count
        result = conn.execute("SELECT COUNT(*) FROM newhaven.buildings").fetchone()
        assert result[0] == 10

        # Test JOIN query
        result = conn.execute(
            """
            SELECT p.pid, p.address, b.year_built
            FROM newhaven.properties p
            JOIN newhaven.buildings b ON p.uuid = b.property_uuid
            WHERE p.pid = 5
            """
        ).fetchone()

        assert result[0] == 5
        assert result[1] == "5 Main St"
        assert result[2] == 1955

        # Test aggregation
        result = conn.execute(
            """
            SELECT AVG(assessment_value) as avg_value
            FROM newhaven.properties
            """
        ).fetchone()

        # Average should be 155000 (100000 + 10000*5.5)
        assert abs(result[0] - 155000.0) < 1.0

        conn.close()
