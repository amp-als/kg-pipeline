"""Asset definitions for the ALS KP knowledge graph pipeline.

Asset graph per table:
    portal/csv/{table}  →  portal/rdf/{table}

Full DAG:
    portal/csv/files     ─┐
    portal/csv/datasets  ─┼→  portal/quality/fk_validation
                          │
    portal/csv/files     ─→  portal/rdf/files
    portal/csv/datasets  ─→  portal/rdf/datasets

Assets are generated from the TABLES dict in prepare_portal_tables.py via
factory functions, not copy-pasted per table.
"""

import sys
from pathlib import Path
from typing import List

import pandas as pd
from dagster import AssetExecutionContext, asset

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.prepare_portal_tables import TABLES, _query_table  # noqa: E402
from scripts.prepare_portal_tables import _save_raw, _process_table, _save_processed  # noqa: E402
from orchestration.dagster_pipeline.resources import SynapseResource, RMLMapperResource  # noqa: E402


# ---------------------------------------------------------------------------
# CSV asset factory
# ---------------------------------------------------------------------------

def create_csv_asset(table_key: str):
    """Generate a Dagster asset that extracts a Synapse table to CSV."""
    cfg = TABLES[table_key]

    @asset(
        name=table_key,
        key_prefix=["portal", "csv"],
        compute_kind="synapse",
        group_name=table_key,
        metadata={
            "synapse_id": cfg["synapse_id"],
            "source_version": str(cfg.get("source_version")),
        },
    )
    def _csv_asset(context: AssetExecutionContext, synapse: SynapseResource) -> None:
        raw_path = PROJECT_ROOT / "data" / "raw" / f"{table_key}_raw.csv"

        if raw_path.exists():
            context.log.info(f"Using cached raw export: {raw_path}")
            df = pd.read_csv(raw_path, dtype=str, keep_default_na=False)
        else:
            context.log.info(f"Querying Synapse ({cfg['synapse_id']})")
            df = _query_table(synapse.client, table_key)
            _save_raw(df, table_key)

        processed = _process_table(df, cfg["columns"])
        _save_processed(processed, table_key)

        context.add_output_metadata({
            "num_rows": len(processed),
            "num_columns": len(processed.columns),
            "path": str(cfg["csv_path"].relative_to(PROJECT_ROOT)),
        })
        context.log.info(f"Wrote {len(processed)} rows to {cfg['csv_path'].name}")

    _csv_asset.__name__ = f"csv_{table_key}"
    _csv_asset.__qualname__ = f"csv_{table_key}"
    return _csv_asset


# ---------------------------------------------------------------------------
# RDF asset factory
# ---------------------------------------------------------------------------

def create_rdf_asset(table_key: str):
    """Generate a Dagster asset that runs RMLMapper on a table's CSV."""
    rml_path = f"mappings/rml/{table_key}.rml.ttl"
    rdf_path = f"data/rdf/{table_key}.ttl"
    log_path = f"logs/{table_key}_rml.log"

    @asset(
        name=table_key,
        key_prefix=["portal", "rdf"],
        compute_kind="rml",
        group_name=table_key,
        deps=[["portal", "csv", table_key]],
    )
    def _rdf_asset(context: AssetExecutionContext, rml_mapper: RMLMapperResource) -> None:
        rml_mapper.run(
            mapping_file=rml_path,
            output_file=rdf_path,
            log_file=log_path,
        )
        size_mb = (PROJECT_ROOT / rdf_path).stat().st_size / (1024 * 1024)
        context.add_output_metadata({
            "path": rdf_path,
            "size_mb": round(size_mb, 2),
        })
        context.log.info(f"Generated {rdf_path} ({size_mb:.2f} MB)")

    _rdf_asset.__name__ = f"rdf_{table_key}"
    _rdf_asset.__qualname__ = f"rdf_{table_key}"
    return _rdf_asset


# ---------------------------------------------------------------------------
# FK validation asset (non-blocking)
# ---------------------------------------------------------------------------

def create_fk_validation_asset(csv_asset_keys: List):
    """Generate a Dagster asset that checks FK constraints across all CSVs."""

    @asset(
        name="fk_validation",
        key_prefix=["portal", "quality"],
        compute_kind="python",
        group_name="validation",
        deps=csv_asset_keys,
    )
    def _fk_validation(context: AssetExecutionContext) -> None:
        from scripts.validate_fks import validate_all

        results = validate_all(PROJECT_ROOT / "data" / "csv")
        failures = sum(1 for r in results if not r.passed)

        for r in results:
            c = r.constraint
            label = f"{c.source_table}.{c.source_col} → {c.target_table}.{c.target_col}"
            if r.passed:
                context.log.info(f"[ok]   {label}")
            else:
                context.log.warning(
                    f"[FAIL] {label}: {r.orphan_count}/{r.total_populated} orphans "
                    f"({r.orphan_pct}%) — sample: {r.sample_violations}"
                )

        if not results:
            context.log.info("No FK constraints declared — nothing to validate.")

        context.add_output_metadata({
            "total_constraints": len(results),
            "failures": failures,
            "passed": len(results) - failures,
        })

    return _fk_validation


# ---------------------------------------------------------------------------
# Generate all assets from TABLES dict
# ---------------------------------------------------------------------------

def generate_portal_assets() -> List:
    """Build the full asset list from the TABLES registry."""
    assets = []
    csv_asset_keys = []

    for table_key in TABLES:
        assets.append(create_csv_asset(table_key))
        csv_asset_keys.append(["portal", "csv", table_key])

        assets.append(create_rdf_asset(table_key))

    assets.append(create_fk_validation_asset(csv_asset_keys))
    return assets


portal_assets = generate_portal_assets()
