"""Asset definitions for the ALS KP knowledge graph pipeline.

Asset graph per table:
    portal/csv/{table}  →  portal/rdf/{table}

Full DAG:
    portal/csv/files     ─┐
    portal/csv/datasets  ─┼→  portal/quality/fk_validation
                          │
    portal/csv/files     ─→  portal/rdf/files
    portal/csv/datasets  ─→  portal/rdf/datasets
"""

import sys
from pathlib import Path

from dagster import AssetExecutionContext, asset

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.prepare_portal_tables import TABLES, _get_syn, _query_table, _load_raw  # noqa: E402
from scripts.prepare_portal_tables import _save_raw, _process_table, _save_processed  # noqa: E402
from orchestration.dagster_pipeline.resources import SynapseResource, RMLMapperResource  # noqa: E402

# ---------------------------------------------------------------------------
# CSV assets
# ---------------------------------------------------------------------------

@asset(
    name="files",
    key_prefix=["portal", "csv"],
    compute_kind="synapse",
    group_name="files",
    metadata={
        "synapse_id": TABLES["files"]["synapse_id"],
        "source_version": str(TABLES["files"].get("source_version")),
    },
)
def csv_files(context: AssetExecutionContext, synapse: SynapseResource) -> None:
    """Download and process the ALS KP file view from Synapse → data/csv/files.csv."""
    raw_path = PROJECT_ROOT / "data" / "raw" / "files_raw.csv"

    if raw_path.exists():
        context.log.info(f"Using cached raw export: {raw_path}")
        import pandas as pd
        df = pd.read_csv(raw_path, dtype=str, keep_default_na=False)
    else:
        context.log.info("Querying Synapse (syn66271104)")
        df = _query_table(synapse.client, "files")
        _save_raw(df, "files")

    processed = _process_table(df, TABLES["files"]["columns"])
    _save_processed(processed, "files")

    context.add_output_metadata({
        "num_rows": len(processed),
        "num_columns": len(processed.columns),
        "path": "data/csv/files.csv",
    })
    context.log.info(f"Wrote {len(processed)} rows to data/csv/files.csv")


@asset(
    name="datasets",
    key_prefix=["portal", "csv"],
    compute_kind="synapse",
    group_name="datasets",
    metadata={
        "synapse_id": TABLES["datasets"]["synapse_id"],
        "source_version": str(TABLES["datasets"].get("source_version")),
    },
)
def csv_datasets(context: AssetExecutionContext, synapse: SynapseResource) -> None:
    """Download and process the ALS KP dataset collection → data/csv/datasets.csv."""
    raw_path = PROJECT_ROOT / "data" / "raw" / "datasets_raw.csv"

    if raw_path.exists():
        context.log.info(f"Using cached raw export: {raw_path}")
        import pandas as pd
        df = pd.read_csv(raw_path, dtype=str, keep_default_na=False)
    else:
        context.log.info("Querying Synapse (syn66496326)")
        df = _query_table(synapse.client, "datasets")
        _save_raw(df, "datasets")

    processed = _process_table(df, TABLES["datasets"]["columns"])
    _save_processed(processed, "datasets")

    context.add_output_metadata({
        "num_rows": len(processed),
        "num_columns": len(processed.columns),
        "path": "data/csv/datasets.csv",
    })
    context.log.info(f"Wrote {len(processed)} rows to data/csv/datasets.csv")


# ---------------------------------------------------------------------------
# FK validation asset (non-blocking)
# ---------------------------------------------------------------------------

@asset(
    name="fk_validation",
    key_prefix=["portal", "quality"],
    compute_kind="python",
    group_name="validation",
    deps=[["portal", "csv", "files"], ["portal", "csv", "datasets"]],
)
def fk_validation(context: AssetExecutionContext) -> None:
    """Run FK validation across all processed CSVs (non-blocking)."""
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


# ---------------------------------------------------------------------------
# RDF assets
# ---------------------------------------------------------------------------

@asset(
    name="files",
    key_prefix=["portal", "rdf"],
    compute_kind="rml",
    group_name="files",
    deps=[["portal", "csv", "files"]],
)
def rdf_files(context: AssetExecutionContext, rml_mapper: RMLMapperResource) -> None:
    """Run RMLMapper on files CSV → data/rdf/files.ttl."""
    rml_mapper.run(
        mapping_file="mappings/rml/files.rml.ttl",
        output_file="data/rdf/files.ttl",
        log_file="logs/files_rml.log",
    )
    size_mb = (PROJECT_ROOT / "data" / "rdf" / "files.ttl").stat().st_size / (1024 * 1024)
    context.add_output_metadata({
        "path": "data/rdf/files.ttl",
        "size_mb": round(size_mb, 2),
    })
    context.log.info(f"Generated data/rdf/files.ttl ({size_mb:.1f} MB)")


@asset(
    name="datasets",
    key_prefix=["portal", "rdf"],
    compute_kind="rml",
    group_name="datasets",
    deps=[["portal", "csv", "datasets"]],
)
def rdf_datasets(context: AssetExecutionContext, rml_mapper: RMLMapperResource) -> None:
    """Run RMLMapper on datasets CSV → data/rdf/datasets.ttl."""
    rml_mapper.run(
        mapping_file="mappings/rml/datasets.rml.ttl",
        output_file="data/rdf/datasets.ttl",
        log_file="logs/datasets_rml.log",
    )
    size_mb = (PROJECT_ROOT / "data" / "rdf" / "datasets.ttl").stat().st_size / (1024 * 1024)
    context.add_output_metadata({
        "path": "data/rdf/datasets.ttl",
        "size_mb": round(size_mb, 4),
    })
    context.log.info(f"Generated data/rdf/datasets.ttl ({size_mb:.2f} MB)")
