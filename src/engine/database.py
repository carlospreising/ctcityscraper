"""
Append-only parquet writer for the multi-source scraper engine.

Writes scrape results as parquet files — no DuckDB write lock held during
scraping, so the data is queryable at any time via:

    SELECT * FROM read_parquet('data/newhaven/properties/*.parquet')

Checkpoints are stored as JSON files.
"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb

from .base import SourceDefinition
from .hash import compute_row_hash

logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Thread-safe append-only parquet writer.

    Each write_batch call:
    1. Calls source.flatten_fn to get per-table row dicts
    2. Adds scraped_at + row_hash to each row
    3. Writes rows to parquet via an in-memory DuckDB connection

    Usage:
        writer = ParquetWriter('newhaven', source, 'data')
        writer.write_batch([result1, result2, ...])
        writer.save_checkpoint('newhaven', last_entry_id=100, total=95)
        writer.close()
    """

    def __init__(
        self,
        scope_key: str,
        source: SourceDefinition,
        data_dir: str = "data",
    ):
        self.scope_key = scope_key
        self.source = source
        self.data_dir = Path(data_dir)
        self.scope_dir = self.data_dir / scope_key

        self._lock = threading.Lock()
        self._batch_num = 0
        self._session_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        self._existing_hashes: dict[str, set[str]] | None = None
        self._rows_written = 0
        self._rows_skipped = 0

        logger.info(f"ParquetWriter initialized: {self.scope_dir}")

    def load_existing_hashes(self, table_name: str) -> set[str]:
        """Load all unique row_hash values from existing parquet files for a table.

        Uses DuckDB column pruning to only read the row_hash column.
        """
        table_dir = self.scope_dir / table_name
        if not table_dir.exists():
            return set()

        pattern = str(table_dir / "*.parquet")
        conn = duckdb.connect()
        try:
            rows = conn.execute(
                f"SELECT DISTINCT row_hash FROM read_parquet('{pattern}')"
            ).fetchall()
            return {r[0] for r in rows}
        except Exception:
            return set()
        finally:
            conn.close()

    def preload_hashes(self):
        """Load existing row_hash sets for all tables under this scope.

        Called once before refresh starts. Subsequent write_batch calls
        skip rows whose hash already exists.
        """
        self._existing_hashes = {}
        if not self.scope_dir.exists():
            return

        for table_dir in self.scope_dir.iterdir():
            if table_dir.is_dir():
                self._existing_hashes[table_dir.name] = self.load_existing_hashes(
                    table_dir.name
                )

        total = sum(len(v) for v in self._existing_hashes.values())
        logger.info(
            f"Preloaded {total} existing hashes across "
            f"{len(self._existing_hashes)} tables"
        )

    def get_write_stats(self) -> dict:
        """Return counts of rows written and skipped during this session."""
        return {
            "rows_written": self._rows_written,
            "rows_skipped": self._rows_skipped,
        }

    def write_batch(self, results: List[dict]):
        """Flatten results into per-table rows and write to parquet.

        If preload_hashes() was called, rows whose row_hash matches an
        existing hash are skipped (change-only writes).
        """
        if not results:
            return

        tables = self.source.flatten_fn(results)
        now = datetime.now()

        with self._lock:
            batch_num = self._batch_num
            self._batch_num += 1

        for table_name, rows in tables.items():
            if not rows:
                continue

            for row in rows:
                row["scraped_at"] = now
                row["row_hash"] = compute_row_hash(row)

            # Filter out unchanged rows if hash cache is loaded
            if self._existing_hashes is not None:
                known = self._existing_hashes.get(table_name, set())
                new_rows = [r for r in rows if r["row_hash"] not in known]
                self._rows_skipped += len(rows) - len(new_rows)
                # Track newly written hashes to avoid intra-session dupes
                for r in new_rows:
                    known.add(r["row_hash"])
                if table_name not in self._existing_hashes:
                    self._existing_hashes[table_name] = known
                rows = new_rows

            if not rows:
                continue

            self._rows_written += len(rows)

            out_dir = self.scope_dir / table_name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{self._session_ts}_{batch_num:04d}.parquet"

            self._write_parquet(rows, str(out_path))
            logger.debug(
                f"Wrote {len(rows)} rows to {out_path.name} ({table_name})"
            )

    def _write_parquet(self, rows: list[dict], path: str):
        """Write a list of dicts to a parquet file using in-memory DuckDB."""
        import pyarrow as pa

        # Convert list of dicts to PyArrow Table, then write via DuckDB
        # This handles type inference properly
        table = pa.Table.from_pylist(rows)
        conn = duckdb.connect()
        try:
            conn.register("_batch", table)
            conn.execute(
                f"COPY _batch TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            conn.close()

    # --- Checkpoints (JSON) ---

    def _checkpoint_path(self, scope_key: str) -> Path:
        path = self.data_dir / "_checkpoints"
        path.mkdir(parents=True, exist_ok=True)
        return path / f"{scope_key}.json"

    def save_checkpoint(self, scope_key: str, last_entry_id, total_scraped: int):
        with self._lock:
            cp = {
                "scope_key": scope_key,
                "last_entry_id": str(last_entry_id),
                "total_scraped": total_scraped,
                "checkpoint_time": datetime.now().isoformat(),
            }
            self._checkpoint_path(scope_key).write_text(json.dumps(cp))
            logger.info(
                f"Checkpoint: scope_key={scope_key}, "
                f"last_entry_id={last_entry_id}, total={total_scraped}"
            )

    def get_last_checkpoint(self, scope_key: str) -> Tuple[Optional[str], int]:
        """Returns (last_entry_id, total_scraped). last_entry_id is None if no checkpoint."""
        path = self._checkpoint_path(scope_key)
        if not path.exists():
            return None, 0
        try:
            cp = json.loads(path.read_text())
            logger.info(
                f"Resuming: scope_key={scope_key}, "
                f"last_entry_id={cp['last_entry_id']}, total={cp['total_scraped']}"
            )
            return cp["last_entry_id"], cp["total_scraped"]
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Invalid checkpoint file {path}: {e}")
            return None, 0

    # --- Query helpers ---

    def get_known_entry_ids(self) -> list:
        """Query parquet files for known entry IDs. Delegates to source."""
        return self.source.get_known_entry_ids_fn(
            str(self.data_dir), self.scope_key
        )

    def compact(self):
        """
        Merge parquet files from this session into one file per table.

        Replaces all files matching this session's timestamp prefix with a
        single consolidated file. Files from other sessions are untouched.
        """
        if not self.scope_dir.exists():
            return

        for table_dir in self.scope_dir.iterdir():
            if not table_dir.is_dir():
                continue

            session_files = sorted(table_dir.glob(f"{self._session_ts}_*.parquet"))
            if len(session_files) <= 1:
                continue

            logger.info(
                f"Compacting {len(session_files)} files in {table_dir.name}"
            )

            # Read all session files into one table and write back
            conn = duckdb.connect()
            try:
                glob_pattern = str(table_dir / f"{self._session_ts}_*.parquet")
                compact_path = str(table_dir / f"{self._session_ts}.parquet")

                conn.execute(
                    f"COPY (SELECT * FROM read_parquet('{glob_pattern}')) "
                    f"TO '{compact_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            finally:
                conn.close()

            # Remove the old batch files
            for f in session_files:
                f.unlink()

            logger.info(
                f"Compacted {table_dir.name}: "
                f"{len(session_files)} files → 1"
            )

    def close(self):
        logger.info(f"ParquetWriter closed: {self.scope_dir}")
