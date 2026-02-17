"""
Parallel scraper with integrated rate limiting.

Merges the previous parallel_scraper.py and rate_limiter.py into a single
module. Uses ThreadPoolExecutor for concurrency and a semaphore + token
bucket for rate control.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

from tqdm import tqdm

from .database import DuckDBWriter
from .scraper import InvalidPIDException, download_photo, scrape_property

logger = logging.getLogger(__name__)


# --- Rate Limiter ---


class RateLimiter:
    """
    Thread-safe rate limiter using semaphore + token bucket.

    Controls:
    1. Max concurrent requests (semaphore)
    2. Requests per second (minimum interval enforcement)
    """

    def __init__(self, max_workers=10, requests_per_second=5):
        self.semaphore = threading.Semaphore(max_workers)
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.last_request_time = 0
        self.lock = threading.Lock()
        self.total_requests = 0
        self.total_wait_time = 0.0

    @contextmanager
    def acquire(self):
        acquired = self.semaphore.acquire(timeout=300)
        if not acquired:
            raise TimeoutError("Rate limiter semaphore timeout")

        try:
            if self.min_interval > 0:
                with self.lock:
                    now = time.time()
                    elapsed = now - self.last_request_time
                    if elapsed < self.min_interval:
                        wait = self.min_interval - elapsed
                        time.sleep(wait)
                        now = time.time()
                        self.total_wait_time += wait
                    self.last_request_time = now
                    self.total_requests += 1
            yield
        finally:
            self.semaphore.release()

    def get_stats(self):
        with self.lock:
            avg_wait = (
                self.total_wait_time / self.total_requests
                if self.total_requests > 0
                else 0
            )
            return {
                "total_requests": self.total_requests,
                "total_wait_time": self.total_wait_time,
                "avg_wait_time": avg_wait,
            }


# --- Parallel scraping ---


def load_city_parallel(
    city="newhaven",
    db_path="ctcityscraper.duckdb",
    base_url=None,
    pid_min=1,
    pid_max=None,
    max_workers=10,
    requests_per_second=5,
    checkpoint_every=100,
    resume_from_checkpoint=True,
    batch_size=10,
    show_progress=True,
    download_photos=False,
    photo_dir="photos",
):
    """
    Scrape city properties in parallel.

    Args:
        city: City key (e.g., 'newhaven')
        db_path: Path to DuckDB database file
        base_url: Override VGSI URL (if None, looks up from DB)
        pid_min: Starting PID
        pid_max: Ending PID (required)
        max_workers: Number of concurrent threads
        requests_per_second: Rate limit
        checkpoint_every: Save progress every N properties
        resume_from_checkpoint: Resume from last checkpoint if available
        batch_size: Write to database every N properties
        show_progress: Show tqdm progress bar
        download_photos: Whether to download building photos
        photo_dir: Directory for downloaded photos

    Returns:
        Number of properties successfully scraped
    """
    if pid_max is None:
        raise ValueError("pid_max is required")

    logger.info(f"Starting parallel scrape: city={city}, PIDs {pid_min}-{pid_max}")
    logger.info(f"Workers: {max_workers}, Rate: {requests_per_second} req/sec")

    db_writer = DuckDBWriter(city, db_path)

    # Resolve VGSI URL
    if base_url is None:
        base_url = db_writer.get_city_url(city)
        if not base_url:
            db_writer.close()
            raise ValueError(
                f"City '{city}' not found in database. "
                f"Run fetch_vgsi_cities() + store_cities() first, or provide base_url."
            )
    # Ensure URL ends with /
    if not base_url.endswith("/"):
        base_url += "/"
    logger.info(f"URL: {base_url}")

    # Resume from checkpoint
    if resume_from_checkpoint:
        last_pid, total_scraped = db_writer.get_last_checkpoint(city)
        if last_pid > 0:
            pid_min = last_pid + 1
            logger.info(
                f"Resuming from PID {pid_min} ({total_scraped} previously scraped)"
            )

    rate_limiter = RateLimiter(max_workers, requests_per_second)

    # Shared state
    batch = []
    batch_lock = threading.Lock()
    completed = {"count": 0}
    completed_lock = threading.Lock()

    def worker(pid):
        """Thread worker: scrape one property."""
        try:
            with rate_limiter.acquire():
                start = time.time()
                result = scrape_property(base_url, pid)
                scrape_time = time.time() - start

                # Set VGSI property URL
                if result:
                    result["property"]["vgsi_url"] = f"{base_url}Parcel.aspx?pid={pid}"

                # Optionally download photos
                if download_photos and result:
                    for b in result.get("buildings", []):
                        photo_url = b.get("photo_url")
                        if photo_url:
                            local_path = download_photo(photo_url, city, pid, photo_dir)
                            b["photo_local_path"] = local_path

                return result, scrape_time

        except InvalidPIDException:
            return None, 0
        except Exception as e:
            logger.error(f"Failed to scrape PID {pid}: {e}")
            return None, 0

    def handle_result(result, pid):
        """Process a completed result: batch and write."""
        if result is None:
            return

        with batch_lock:
            batch.append(result)
            if len(batch) >= batch_size:
                try:
                    db_writer.write_batch(batch)
                    batch.clear()
                except Exception as e:
                    logger.error(f"Batch write failed: {e}")

        with completed_lock:
            completed["count"] += 1
            if completed["count"] % checkpoint_every == 0:
                try:
                    db_writer.save_checkpoint(city, pid, completed["count"])
                except Exception as e:
                    logger.error(f"Checkpoint failed: {e}")

    total_pids = pid_max - pid_min + 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(worker, pid): pid for pid in range(pid_min, pid_max + 1)
        }

        pbar = (
            tqdm(total=total_pids, desc=f"Scraping {city}", unit="prop")
            if show_progress
            else None
        )

        try:
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    result, scrape_time = future.result()
                    handle_result(result, pid)
                except Exception as e:
                    logger.error(f"Exception for PID {pid}: {e}")

                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(success=completed["count"])
        finally:
            if pbar:
                pbar.close()

    # Flush remaining batch
    if batch:
        try:
            db_writer.write_batch(batch)
        except Exception as e:
            logger.error(f"Final batch write failed: {e}")

    # Final checkpoint
    try:
        db_writer.save_checkpoint(city, pid_max, completed["count"])
    except Exception as e:
        logger.error(f"Final checkpoint failed: {e}")

    count = completed["count"]
    elapsed = (
        time.time() - rate_limiter.last_request_time
        if rate_limiter.total_requests > 0
        else 0
    )

    logger.info("=" * 50)
    logger.info(f"SCRAPE COMPLETE: {city}")
    logger.info(f"PIDs: {pid_min}-{pid_max} | Successful: {count}/{total_pids}")
    if elapsed > 0:
        logger.info(f"Rate: {count / elapsed:.1f} props/sec")
    logger.info("=" * 50)

    db_writer.close()
    return count
