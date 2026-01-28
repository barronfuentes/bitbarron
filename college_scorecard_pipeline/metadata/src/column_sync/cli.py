"""CLI entrypoint for syncing column definitions to Unity Catalog."""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from column_sync.config import load_databricks_config, validate_databricks_credentials


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="column-sync",
        description=(
            "Sync column descriptions in Databricks Unity Catalog with the data.yaml dictionary."
        ),
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog name (e.g., main).",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Unity Catalog schema name (e.g., default).",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Target table name (e.g., college_scorecard).",
    )
    parser.add_argument(
        "--yaml",
        required=True,
        help="Path to data.yaml containing column descriptions.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show intended updates without applying changes.",
    )
    parser.add_argument(
        "--column-prefix",
        default=None,
        help="Only sync columns with this prefix.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help="Comma-separated list of column names to include.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write a JSON report of outcomes.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile name from ~/.databrickscfg (default: DEFAULT).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (e.g., INFO, DEBUG).",
    )

    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""

    return build_parser().parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""

    args = parse_args(argv)
    logging.basicConfig(level=args.log_level.upper())
    logger = logging.getLogger(__name__)
    # Load config now to validate auth/host early.
    config = load_databricks_config(args.profile)
    validate_databricks_credentials(config, args.catalog, args.schema, args.table)
    logger.info(
        "Validated Databricks credentials and found table %s.%s.%s.",
        args.catalog,
        args.schema,
        args.table,
    )
    # Functionality is implemented in later tasks.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
