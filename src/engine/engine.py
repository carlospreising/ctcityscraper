"""
Generic parallel execution engine for multi-source scraping.

Handles threading, rate limiting, batching, checkpoints, and progress.
Zero source-specific code — all source behavior comes through SourceDefinition
and the iter_entries_fn parameter.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Callable

from tqdm import tqdm

from .base import SourceDefinition
from .database import ParquetWriter

__all__ = ["RateLimiter", "TooManyErrors", "run_load", "run_refresh"]

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
                wait = 0
                with self.lock:
                    now = time.time()
                    elapsed = now - self.last_request_time
                    if elapsed < self.min_interval:
                        wait = self.min_interval - elapsed
                    # Set next slot time optimistically so other threads
                    # calculate their wait from this point
                    self.last_request_time = now + wait
                    self.total_requests += 1
                # Sleep outside the lock so other threads can proceed
                if wait > 0:
                    time.sleep(wait)
                    with self.lock:
                        self.total_wait_time += wait
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


# --- Parallel execution ---


class TooManyErrors(RuntimeError):
    """Raised when consecutive error count exceeds the configured threshold."""

    pass


def run_load(
    scope_key: str,
    writer: ParquetWriter,
    source: SourceDefinition,
    base_url: str,
    iter_entries_fn: Callable,
    max_workers: int = 10,
    requests_per_second: float = 5,
    checkpoint_every: int = 100,
    resume_from_checkpoint: bool = True,
    batch_size: int = 10,
    show_progress: bool = True,
    download_photos: bool = False,
    photo_dir: str = "photos",
    max_consecutive_errors: int = 50,
) -> int:
    """
    Scrape entries in parallel, writing results to parquet.

    Args:
        scope_key: Namespace key for this scrape (e.g. 'newhaven')
        writer: Parquet writer instance
        source: Source definition with scrape_fn, etc.
        base_url: Base URL for the source
        iter_entries_fn: Callable(base_url, data_dir, scope_key) -> Iterator of entry IDs
        max_workers: Number of concurrent threads
        requests_per_second: Rate limit
        checkpoint_every: Save progress every N entries
        resume_from_checkpoint: Resume from last checkpoint if available
        batch_size: Write to parquet every N entries
        show_progress: Show tqdm progress bar
        download_photos: Whether to download photos
        photo_dir: Directory for downloaded photos
        max_consecutive_errors: Abort after this many consecutive non-invalid errors (0=unlimited)

    Returns:
        Number of entries successfully scraped
    """
    # Materialize entry IDs from the iterator
    entry_ids = list(
        iter_entries_fn(base_url, str(writer.data_dir), scope_key)
    )
    if not entry_ids:
        logger.warning(f"No entry IDs to scrape for '{scope_key}'.")
        writer.close()
        return 0

    logger.info(f"Starting parallel scrape: {scope_key}, {len(entry_ids)} entries")
    logger.info(f"Workers: {max_workers}, Rate: {requests_per_second} req/sec")

    # Resume from checkpoint
    total_previously_scraped = 0
    if resume_from_checkpoint:
        last_entry_id, total_previously_scraped = writer.get_last_checkpoint(
            scope_key
        )
        if last_entry_id is not None:
            try:
                idx = None
                for i, eid in enumerate(entry_ids):
                    if str(eid) == str(last_entry_id):
                        idx = i
                        break
                if idx is not None:
                    entry_ids = entry_ids[idx + 1 :]
                    logger.info(
                        f"Resuming from entry {last_entry_id} "
                        f"({total_previously_scraped} previously scraped, "
                        f"{len(entry_ids)} remaining)"
                    )
                else:
                    logger.warning(
                        f"Checkpoint entry_id {last_entry_id} not found in current "
                        f"entry list. Starting from beginning."
                    )
                    total_previously_scraped = 0
            except (ValueError, IndexError):
                logger.warning(
                    "Could not resume from checkpoint. Starting from beginning."
                )
                total_previously_scraped = 0

    if not entry_ids:
        logger.info("All entries already scraped (resumed past end). Nothing to do.")
        writer.close()
        return 0

    rate_limiter = RateLimiter(max_workers, requests_per_second)
    start_time = time.time()

    # Shared state
    batch = []
    batch_lock = threading.Lock()
    completed = {"count": 0}
    errors = {"total": 0, "consecutive": 0}
    state_lock = threading.Lock()

    def worker(entry_id):
        """Thread worker: scrape one entry."""
        try:
            with rate_limiter.acquire():
                start = time.time()
                result = source.scrape_fn(base_url, entry_id)
                scrape_time = time.time() - start

                if (
                    download_photos
                    and result
                    and source.get_photo_items_fn
                    and source.download_fn
                ):
                    for item in source.get_photo_items_fn(result, scope_key, entry_id):
                        source.download_fn(*item, photo_dir)

                return result, scrape_time, False  # not an error

        except source.invalid_entry_exception:
            return None, 0, False  # invalid entry is not an error
        except Exception as e:
            logger.error(f"Failed to scrape entry {entry_id}: {e}")
            return None, 0, True  # is an error

    def handle_result(result, entry_id, is_error):
        """Process a completed result: batch and write."""
        with state_lock:
            if is_error:
                errors["total"] += 1
                errors["consecutive"] += 1
                if (
                    max_consecutive_errors > 0
                    and errors["consecutive"] >= max_consecutive_errors
                ):
                    raise TooManyErrors(
                        f"Aborting: {errors['consecutive']} consecutive errors "
                        f"({errors['total']} total). Likely a systemic issue "
                        f"(network outage, site down, etc.)."
                    )
            else:
                errors["consecutive"] = 0

        if result is None:
            return

        with batch_lock:
            batch.append(result)
            if len(batch) >= batch_size:
                try:
                    writer.write_batch(batch)
                    batch.clear()
                except Exception as e:
                    logger.error(f"Batch write failed: {e}")

        with state_lock:
            completed["count"] += 1
            if completed["count"] % checkpoint_every == 0:
                try:
                    writer.save_checkpoint(
                        scope_key,
                        entry_id,
                        completed["count"] + total_previously_scraped,
                    )
                except Exception as e:
                    logger.error(f"Checkpoint failed: {e}")

    total_entries = len(entry_ids)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, eid): eid for eid in entry_ids}

        pbar = (
            tqdm(total=total_entries, desc=f"Scraping {scope_key}", unit="entry")
            if show_progress
            else None
        )

        try:
            for future in as_completed(futures):
                entry_id = futures[future]
                try:
                    result, scrape_time, is_error = future.result()
                    handle_result(result, entry_id, is_error)
                except TooManyErrors:
                    logger.error(
                        f"Too many consecutive errors ({errors['consecutive']}). "
                        f"Cancelling remaining futures."
                    )
                    for f in futures:
                        f.cancel()
                    break
                except Exception as e:
                    logger.error(f"Exception for entry {entry_id}: {e}")

                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(
                        success=completed["count"], errors=errors["total"]
                    )
        finally:
            if pbar:
                pbar.close()

    # Flush remaining batch
    if batch:
        try:
            writer.write_batch(batch)
        except Exception as e:
            logger.error(f"Final batch write failed: {e}")

    # Final checkpoint
    if entry_ids:
        try:
            writer.save_checkpoint(
                scope_key,
                entry_ids[-1],
                completed["count"] + total_previously_scraped,
            )
        except Exception as e:
            logger.error(f"Final checkpoint failed: {e}")

    count = completed["count"]
    error_count = errors["total"]
    elapsed = time.time() - start_time

    logger.info("=" * 50)
    logger.info(f"SCRAPE COMPLETE: {scope_key}")
    logger.info(
        f"Entries: {total_entries} | Successful: {count}/{total_entries} | "
        f"Errors: {error_count}"
    )
    if elapsed > 0:
        logger.info(f"Rate: {count / elapsed:.1f} entries/sec")
    logger.info("=" * 50)

    writer.compact()
    writer.close()
    return count


def run_refresh(
    scope_key: str,
    writer: ParquetWriter,
    source: SourceDefinition,
    base_url: str,
    max_workers: int = 10,
    requests_per_second: float = 5,
    batch_size: int = 10,
    show_progress: bool = True,
    download_photos: bool = False,
    photo_dir: str = "photos",
    max_consecutive_errors: int = 50,
) -> int:
    """
    Re-scrape all known entries to detect and record any changes.

    Reads the list of existing entry IDs from parquet, re-fetches each one,
    and appends new rows. Changes are detected at query time via row_hash
    comparison.

    Returns:
        Number of entries successfully re-scraped
    """
    logger.info(f"Starting refresh scrape: {scope_key}")

    entry_ids = writer.get_known_entry_ids()
    if not entry_ids:
        logger.warning(f"No entries found for '{scope_key}'. Nothing to refresh.")
        writer.close()
        return 0

    logger.info(
        f"Refreshing {len(entry_ids)} known entries for {scope_key} | URL: {base_url}"
    )
    logger.info(f"Workers: {max_workers}, Rate: {requests_per_second} req/sec")

    # Preload existing hashes so write_batch skips unchanged rows
    writer.preload_hashes()

    rate_limiter = RateLimiter(max_workers, requests_per_second)

    batch = []
    batch_lock = threading.Lock()
    completed = {"count": 0}
    errors = {"total": 0, "consecutive": 0}
    state_lock = threading.Lock()

    def worker(entry_id):
        try:
            with rate_limiter.acquire():
                result = source.scrape_fn(base_url, entry_id)

                if (
                    download_photos
                    and result
                    and source.get_photo_items_fn
                    and source.download_fn
                ):
                    for item in source.get_photo_items_fn(result, scope_key, entry_id):
                        source.download_fn(*item, photo_dir)

                return result, False

        except source.invalid_entry_exception:
            return None, False
        except Exception as e:
            logger.error(f"Failed to refresh entry {entry_id}: {e}")
            return None, True

    def handle_result(result, is_error):
        with state_lock:
            if is_error:
                errors["total"] += 1
                errors["consecutive"] += 1
                if (
                    max_consecutive_errors > 0
                    and errors["consecutive"] >= max_consecutive_errors
                ):
                    raise TooManyErrors(
                        f"Aborting: {errors['consecutive']} consecutive errors "
                        f"({errors['total']} total)."
                    )
            else:
                errors["consecutive"] = 0

        if result is None:
            return

        with batch_lock:
            batch.append(result)
            if len(batch) >= batch_size:
                try:
                    writer.write_batch(batch)
                    batch.clear()
                except Exception as e:
                    logger.error(f"Batch write failed: {e}")

        with state_lock:
            completed["count"] += 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, eid): eid for eid in entry_ids}

        pbar = (
            tqdm(total=len(entry_ids), desc=f"Refreshing {scope_key}", unit="entry")
            if show_progress
            else None
        )

        try:
            for future in as_completed(futures):
                entry_id = futures[future]
                try:
                    result, is_error = future.result()
                    handle_result(result, is_error)
                except TooManyErrors:
                    logger.error(
                        f"Too many consecutive errors ({errors['consecutive']}). "
                        f"Cancelling remaining futures."
                    )
                    for f in futures:
                        f.cancel()
                    break
                except Exception as e:
                    logger.error(f"Exception for entry {entry_id}: {e}")

                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(
                        success=completed["count"], errors=errors["total"]
                    )
        finally:
            if pbar:
                pbar.close()

    if batch:
        try:
            writer.write_batch(batch)
        except Exception as e:
            logger.error(f"Final batch write failed: {e}")

    count = completed["count"]
    error_count = errors["total"]
    stats = writer.get_write_stats()
    logger.info("=" * 50)
    logger.info(f"REFRESH COMPLETE: {scope_key}")
    logger.info(f"Entries refreshed: {count}/{len(entry_ids)} | Errors: {error_count}")
    logger.info(
        f"Rows written: {stats['rows_written']} | "
        f"Unchanged rows skipped: {stats['rows_skipped']}"
    )
    logger.info("=" * 50)

    writer.compact()
    writer.close()
    return count
