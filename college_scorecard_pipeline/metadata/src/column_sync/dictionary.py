"""Helpers for parsing the data.yaml dictionary."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DictionaryEntry:
    """Parsed dictionary entry from data.yaml."""

    key: str
    source: str | None
    description: str | None
    normalized_source: str | None


def normalize_column_name(name: str) -> str:
    """Normalize a column name for comparison."""

    return name.strip().strip("`").strip().casefold()


def load_dictionary_entries(yaml_path: str | Path, *, validate_required: bool = True) -> list[DictionaryEntry]:
    """Load dictionary entries from a data.yaml file."""

    path = Path(yaml_path)
    if not path.exists():
        raise ValueError(f"Missing YAML dictionary file at {path}.")

    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Unable to parse YAML at {path}.") from exc

    if not isinstance(content, dict):
        raise ValueError(f"Expected YAML mapping at {path}.")

    raw_dictionary = content.get("dictionary")
    if not isinstance(raw_dictionary, dict):
        raise ValueError("Missing or invalid 'dictionary' section in data.yaml.")

    entries: list[DictionaryEntry] = []
    for key, payload in raw_dictionary.items():
        if not isinstance(payload, dict):
            logger.warning("Skipping dictionary entry %r; expected mapping.", key)
            continue
        source = payload.get("source")
        description = payload.get("description")
        normalized_source = normalize_column_name(source) if isinstance(source, str) else None
        entries.append(
            DictionaryEntry(
                key=str(key),
                source=source if isinstance(source, str) else None,
                description=description if isinstance(description, str) else None,
                normalized_source=normalized_source,
            )
        )

    if validate_required:
        validate_required_fields(entries)

    return entries


def validate_required_fields(entries: list[DictionaryEntry]) -> None:
    """Validate that each entry has required fields."""

    missing_source = [entry.key for entry in entries if not entry.source]
    missing_description = [entry.key for entry in entries if not entry.description]

    if not missing_source and not missing_description:
        return

    message_parts: list[str] = []
    if missing_source:
        message_parts.append(f"Missing source for {len(missing_source)} entries.")
        logger.debug(
            "Entries missing source (%s): %s",
            len(missing_source),
            ", ".join(missing_source),
        )
    if missing_description:
        message_parts.append(f"Missing description for {len(missing_description)} entries.")
        logger.debug(
            "Entries missing description (%s): %s",
            len(missing_description),
            ", ".join(missing_description),
        )

    logger.warning("Dictionary validation warning: %s", " ".join(message_parts))
