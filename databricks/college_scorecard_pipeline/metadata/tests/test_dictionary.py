from __future__ import annotations

import textwrap

import pytest

from column_sync.dictionary import (
    DictionaryEntry,
    load_dictionary_entries,
    normalize_column_name,
    validate_required_fields,
)


def _write_yaml(tmp_path, content: str) -> str:
    path = tmp_path / "data.yaml"
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(path)


def test_normalize_column_name_strips_and_casefolds() -> None:
    assert normalize_column_name("  `UnitID`  ") == "unitid"
    assert normalize_column_name("School.Name") == "school.name"


def test_load_dictionary_entries_parses_sources_and_descriptions(tmp_path) -> None:
    yaml_path = _write_yaml(
        tmp_path,
        """
        dictionary:
          id:
            source: UNITID
            description: Unit ID for institution
          school.name:
            source: INSTNM
            description: Institution name
        """,
    )

    entries = load_dictionary_entries(yaml_path)

    assert entries == [
        DictionaryEntry(
            key="id",
            source="UNITID",
            description="Unit ID for institution",
            normalized_source="unitid",
        ),
        DictionaryEntry(
            key="school.name",
            source="INSTNM",
            description="Institution name",
            normalized_source="instnm",
        ),
    ]


def test_load_dictionary_entries_requires_dictionary_key(tmp_path) -> None:
    yaml_path = _write_yaml(tmp_path, "version: 2025-01-01")

    with pytest.raises(ValueError, match="dictionary"):
        load_dictionary_entries(yaml_path)


def test_load_dictionary_entries_invalid_yaml(tmp_path) -> None:
    yaml_path = _write_yaml(tmp_path, "dictionary: [unclosed")

    with pytest.raises(ValueError, match="parse YAML"):
        load_dictionary_entries(yaml_path)


def test_validate_required_fields_warns_for_missing_values(caplog) -> None:
    entries = [
        DictionaryEntry(
            key="id",
            source="UNITID",
            description=None,
            normalized_source="unitid",
        ),
        DictionaryEntry(
            key="school.name",
            source=None,
            description="Institution name",
            normalized_source=None,
        ),
    ]

    with caplog.at_level("DEBUG"):
        validate_required_fields(entries)

    assert "Missing source for 1 entries." in caplog.text
    assert "Missing description for 1 entries." in caplog.text
    assert "Entries missing source (1): school.name" in caplog.text
    assert "Entries missing description (1): id" in caplog.text


def test_load_dictionary_entries_can_skip_validation(tmp_path) -> None:
    yaml_path = _write_yaml(
        tmp_path,
        """
        dictionary:
          id:
            source: UNITID
          school.name:
            description: Institution name
        """,
    )

    entries = load_dictionary_entries(yaml_path, validate_required=False)

    assert len(entries) == 2
