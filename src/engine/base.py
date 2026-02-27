"""
Contracts for the multi-source scraper engine.

Every source must provide a SourceDefinition and a SourceConfig.

- SourceDefinition: the engine contract (scrape, flatten, known IDs)
- SourceConfig: CLI/orchestration contract (args, base URL, scope key)
"""

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass
class SourceDefinition:
    """
    Everything the engine needs to scrape a data source.

    A source provides one of these as a module-level constant (e.g. VGSI_SOURCE).
    The engine never imports source-specific code directly — it only calls the
    callables on this object.
    """

    # Unique key for this source (e.g. "vgsi")
    source_key: str

    # (base_url, entry_id) -> dict | None
    # Returns a result dict. Raises invalid_entry_exception when entry_id doesn't exist.
    scrape_fn: Callable[[str, int | str], dict | None]

    # (results: list[dict]) -> dict[str, list[dict]]
    # Extracts per-table flat rows from scrape results.
    # Keys are table names, values are lists of row dicts.
    flatten_fn: Callable[[list[dict]], dict[str, list[dict]]]

    # (data_dir: str, scope_key: str) -> list[int | str]
    # Query known entry IDs from parquet files. Used by refresh mode.
    get_known_entry_ids_fn: Callable[[str, str], list]

    # Exception type(s) that mean "entry doesn't exist" — the engine catches
    # these silently and skips the entry.
    invalid_entry_exception: type[Exception] | tuple[type[Exception], ...]

    # Optional: (result, scope_key, entry_id) -> list[tuple]
    # Returns items to download (photo_url, scope_key, entry_id).
    get_photo_items_fn: Callable[[dict, str, int | str], list[tuple]] | None = None

    # Optional: (photo_url, scope_key, entry_id, photo_dir) -> str
    download_fn: Callable | None = None


@dataclass
class ResolvedParams:
    """Output of SourceConfig.resolve() — everything the engine needs to run."""

    base_url: str
    scope_key: str
    iter_entries_fn: Callable | None = None  # None means refresh-only


class SourceConfig:
    """
    CLI and orchestration configuration for a source.

    Each scraper subclasses this to declare its CLI args and how to resolve
    them into engine parameters. The CLI never contains source-specific logic.
    """

    def __init__(
        self,
        source: SourceDefinition,
        default_base_url: str | None = None,
    ):
        self.source = source
        self.source_key = source.source_key
        self.default_base_url = default_base_url

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        """Register source-specific CLI arguments. Override in subclass."""
        pass

    def resolve(self, args: argparse.Namespace) -> ResolvedParams:
        """Resolve CLI args into engine parameters. Must be overridden."""
        raise NotImplementedError

    def get_all_scope_keys(self, data_dir: str) -> list[str]:
        """Return all known scope_keys for refresh-all. Override for smarter logic."""
        base = Path(data_dir)
        if not base.exists():
            return []
        return [
            d.name
            for d in base.iterdir()
            if d.is_dir() and not d.name.startswith("_")
        ]

    def post_refresh(self, args: argparse.Namespace, params: "ResolvedParams") -> None:
        """Called after a successful refresh. Override for post-refresh reporting."""
        pass

    def run_admin(self, args: argparse.Namespace) -> bool:
        """Handle source-specific admin commands. Return True if handled."""
        return False
