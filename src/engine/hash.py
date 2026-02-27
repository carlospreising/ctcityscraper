"""Shared row hashing for change detection across all sources."""

import hashlib
import json

# Fields that are never part of the content hash (SCD metadata, internal IDs)
_DEFAULT_EXCLUDE = frozenset(
    {
        "id",
        "version",
        "row_hash",
        "effective_from",
        "effective_to",
        "is_current",
        "loaded_at",
        "updated_at",
        "created_at",
        "scraped_at",
        "city_id",
        "vgsi_url",
        "photo_paths",
        "photo_local_path",
    }
)


def compute_row_hash(data: dict, extra_exclude: set | None = None) -> str:
    """Compute MD5 hash of a row dict, excluding metadata fields.

    Uses JSON with sorted keys for canonical serialization, ensuring
    the hash is stable regardless of dict insertion order.
    """
    exclude = _DEFAULT_EXCLUDE | (extra_exclude or set())
    hash_data = {
        k: str(v)
        for k, v in sorted(data.items())
        if k not in exclude and v is not None
    }
    canonical = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(canonical.encode("utf-8")).hexdigest()
