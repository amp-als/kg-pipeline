"""
ALS Knowledge Portal — portal table extraction script.

Downloads tables from Synapse (unauthenticated / public access) and writes
processed CSVs to data/csv/.  Raw exports are cached in data/raw/ so that
re-runs with --from-cache skip Synapse calls entirely.

Usage:
    python scripts/prepare_portal_tables.py
    python scripts/prepare_portal_tables.py --from-cache
    python scripts/prepare_portal_tables.py --check-config
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Silence deprecation warnings from synapseclient
warnings.filterwarnings("ignore", category=DeprecationWarning)
import synapseclient  # noqa: E402

ROOT = Path(__file__).parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_CSV = ROOT / "data" / "csv"
SOURCES_FILE = ROOT / "data_sources.yaml"

# ---------------------------------------------------------------------------
# Column transform helpers
# ---------------------------------------------------------------------------

def _fmt_string(v: Any) -> str:
    """Return string value or empty string for null."""
    if pd.isna(v) or v is None:
        return ""
    return str(v).strip()


def _fmt_string_list(v: Any) -> str:
    """Flatten a Synapse STRING_LIST value to pipe-delimited string.

    Synapse returns lists either as Python lists (via SDK) or as JSON strings
    like '["foo","bar"]'.  We normalise both representations.
    """
    # Handle Python lists first (pd.isna raises on list inputs)
    if isinstance(v, list):
        items = [str(i).strip() for i in v if i not in (None, "")]
        return "|".join(items)
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s == "":
        return ""
    if s.startswith("["):
        # Try JSON first, then Python literal (single-quoted lists from raw CSV cache)
        try:
            items = json.loads(s)
            items = [str(i).strip() for i in items if i not in (None, "")]
            return "|".join(items)
        except json.JSONDecodeError:
            pass
        try:
            import ast
            items = ast.literal_eval(s)
            if isinstance(items, list):
                items = [str(i).strip() for i in items if i not in (None, "")]
                return "|".join(items)
        except (ValueError, SyntaxError):
            pass
    return s


def _fmt_number(v: Any) -> str:
    """Coerce numeric value; produce empty string for null/non-numeric."""
    if pd.isna(v) or v is None or str(v).strip() == "":
        return ""
    try:
        n = float(str(v).strip())
        if n == int(n):
            return str(int(n))
        return str(n)
    except (ValueError, TypeError):
        return ""


def _fmt_synapse_id(v: Any) -> str:
    """Normalise a Synapse entity ID, stripping any spurious version suffix."""
    if pd.isna(v) or v is None:
        return ""
    s = str(v).strip()
    s = re.sub(r"\.\d+$", "", s)   # strip .version suffix
    if not s.startswith("syn"):
        return ""
    return s


# Transform function registry
TRANSFORMS = {
    "text": _fmt_string,
    "text+": _fmt_string_list,
    "number": _fmt_number,
    "synapse_id": _fmt_synapse_id,
}

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

FILES_SELECT = """
SELECT
    id, name, fileFormat,
    BioProject, BioSample, GEOSuperSeries, GSE, GSM, SRR, SRS, SRX,
    assay, biospecimenSubtype, biospecimenType, cellType,
    contributor, dataSubtype, dataType, disease,
    genomeReference, globalSubjectId, isPostMortem,
    libraryLayout, libraryPreparationMethod,
    nucleicAcidSource, platform, readLength,
    sex, softwareAndVersion, species, source,
    totalReads, CNSRegion, BrodmannArea,
    originalSampleName, originalSubjectId
FROM syn66271104
"""

DATASETS_SELECT = """
SELECT
    id, name,
    contributor, keywords,
    individualCount, participant_count,
    collection, source, sameAs, GEOSuperSeries, url,
    disease, diseaseSubtype, assay, studyType,
    dataType, dataSubtype, species,
    biospecimenSubtype, biospecimenType,
    CNSRegion, cellType, libraryLayout,
    duoCodes, subject, acknowledgementStatement,
    portalRelease, description, dataset_code
FROM syn66496326
"""

# Each column entry: (output_name, synapse_name, transform_key)
# output_name == synapse_name when not renamed.

FILES_COLUMNS = [
    ("id",                      "id",                      "synapse_id"),
    ("name",                    "name",                    "text"),
    ("fileFormat",              "fileFormat",              "text"),
    ("BioProject",              "BioProject",              "text"),
    ("BioSample",               "BioSample",               "text"),
    ("GEOSuperSeries",          "GEOSuperSeries",          "text"),
    ("GSE",                     "GSE",                     "text"),
    ("GSM",                     "GSM",                     "text"),
    ("SRR",                     "SRR",                     "text"),
    ("SRS",                     "SRS",                     "text"),
    ("SRX",                     "SRX",                     "text"),
    ("assay",                   "assay",                   "text"),
    ("biospecimenSubtype",      "biospecimenSubtype",      "text"),
    ("biospecimenType",         "biospecimenType",         "text"),
    ("cellType",                "cellType",                "text"),
    ("contributor",             "contributor",             "text+"),
    ("dataSubtype",             "dataSubtype",             "text"),
    ("dataType",                "dataType",                "text"),
    ("disease",                 "disease",                 "text+"),
    ("genomeReference",         "genomeReference",         "text"),
    ("globalSubjectId",         "globalSubjectId",         "text"),
    ("isPostMortem",            "isPostMortem",            "text"),
    ("libraryLayout",           "libraryLayout",           "text"),
    ("libraryPreparationMethod","libraryPreparationMethod","text"),
    ("nucleicAcidSource",       "nucleicAcidSource",       "text"),
    ("platform",                "platform",                "text"),
    ("readLength",              "readLength",              "text"),
    ("sex",                     "sex",                     "text+"),
    ("softwareAndVersion",      "softwareAndVersion",      "text+"),
    ("species",                 "species",                 "text"),
    ("source",                  "source",                  "text"),
    ("totalReads",              "totalReads",              "number"),
    ("CNSRegion",               "CNSRegion",               "text"),
    ("BrodmannArea",            "BrodmannArea",            "text"),
    ("originalSampleName",      "originalSampleName",      "text"),
    ("originalSubjectId",       "originalSubjectId",       "text"),
]

# FK references: (source_col, target_table_key, target_col)
FILES_FK_REFS: list[tuple[str, str, str]] = []  # no FK constraints for files in bootstrap

DATASETS_COLUMNS = [
    ("id",                      "id",                      "synapse_id"),
    ("name",                    "name",                    "text"),
    ("contributor",             "contributor",             "text+"),
    ("keywords",                "keywords",                "text+"),
    ("individualCount",         "individualCount",         "text"),
    ("participantCount",        "participant_count",       "number"),
    ("collection",              "collection",              "text"),
    ("source",                  "source",                  "text"),
    ("sameAs",                  "sameAs",                  "text"),
    ("GEOSuperSeries",          "GEOSuperSeries",          "text"),
    ("url",                     "url",                     "text"),
    ("disease",                 "disease",                 "text+"),
    ("diseaseSubtype",          "diseaseSubtype",          "text+"),
    ("assay",                   "assay",                   "text+"),
    ("studyType",               "studyType",               "text"),
    ("dataType",                "dataType",                "text"),
    ("dataSubtype",             "dataSubtype",             "text"),
    ("species",                 "species",                 "text"),
    ("biospecimenSubtype",      "biospecimenSubtype",      "text"),
    ("biospecimenType",         "biospecimenType",         "text+"),
    ("CNSRegion",               "CNSRegion",               "text+"),
    ("cellType",                "cellType",                "text+"),
    ("libraryLayout",           "libraryLayout",           "text+"),
    ("duoCodes",                "duoCodes",                "text"),
    ("subject",                 "subject",                 "text"),
    ("acknowledgementStatement","acknowledgementStatement","text"),
    ("portalRelease",           "portalRelease",           "text"),
    ("description",             "description",             "text"),
    ("datasetCode",             "dataset_code",            "text"),
]

DATASETS_FK_REFS: list[tuple[str, str, str]] = []  # no FK constraints for datasets in bootstrap

# Authoritative table schema: used by both this script and validate_fks.py
TABLES: dict[str, dict] = {
    "files": {
        "synapse_id":   "syn66271104",
        "csv_path":     DATA_CSV / "files.csv",
        "select":       FILES_SELECT,
        "columns":      FILES_COLUMNS,
        "fk_refs":      FILES_FK_REFS,
        "source_version": None,
    },
    "datasets": {
        "synapse_id":   "syn66496326",
        "csv_path":     DATA_CSV / "datasets.csv",
        "select":       DATASETS_SELECT,
        "columns":      DATASETS_COLUMNS,
        "fk_refs":      DATASETS_FK_REFS,
        "source_version": 2,
    },
}

# Short aliases
ALIASES: dict[str, str] = {
    "file":    "files",
    "dataset": "datasets",
}

# ---------------------------------------------------------------------------
# Synapse query helpers
# ---------------------------------------------------------------------------

def _get_syn() -> synapseclient.Synapse:
    """Return an anonymous Synapse client (public portal metadata only)."""
    syn = synapseclient.Synapse()
    # Do NOT call syn.login() — portal Layer 1 metadata is public.
    return syn


def _query_table(syn: synapseclient.Synapse, table_key: str) -> pd.DataFrame:
    """Download a table from Synapse and return a raw DataFrame."""
    cfg = TABLES[table_key]
    sid = cfg["synapse_id"]
    version = cfg.get("source_version")
    query = cfg["select"].strip()

    # Note: EntityView snapshot versions are not directly queryable via
    # the Python SDK's tableQuery method.  The pinned `source_version` is
    # recorded in data_sources.yaml and raw CSVs capture the extraction state.
    if version is not None:
        print(f"  [info] Querying {sid} at HEAD (pinned version={version} "
              f"documented in data_sources.yaml; SDK does not support snapshot queries)")
    else:
        print(f"  [warn] Querying {sid} at HEAD (no pinned version)")
    result = syn.tableQuery(query)
    df = result.asDataFrame()
    return df


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def _process_table(df: pd.DataFrame, columns: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Apply transforms and produce a clean output DataFrame."""
    out: dict[str, list] = {}
    for out_name, src_name, transform_key in columns:
        fn = TRANSFORMS[transform_key]
        if src_name not in df.columns:
            print(f"  [warn] Column '{src_name}' not found in source — filling with empty strings")
            out[out_name] = [""] * len(df)
        else:
            out[out_name] = [fn(v) for v in df[src_name]]
    return pd.DataFrame(out)


def _save_raw(df: pd.DataFrame, table_key: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    path = DATA_RAW / f"{table_key}_raw.csv"
    df.to_csv(path, index=False)
    print(f"  [raw] Saved to {path} ({len(df)} rows)")


def _load_raw(table_key: str) -> pd.DataFrame:
    path = DATA_RAW / f"{table_key}_raw.csv"
    if not path.exists():
        raise FileNotFoundError(f"No cached raw file at {path}. Run without --from-cache first.")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    print(f"  [cache] Loaded {path} ({len(df)} rows)")
    return df


def _save_processed(df: pd.DataFrame, table_key: str) -> None:
    DATA_CSV.mkdir(parents=True, exist_ok=True)
    path = TABLES[table_key]["csv_path"]
    # Replace empty strings with None so RMLMapper sees truly empty CSV cells
    # (empty cell → no triple; empty string "" → triple with empty literal).
    df = df.replace("", None)
    df.to_csv(path, index=False)
    print(f"  [csv] Saved to {path} ({len(df)} rows, {len(df.columns)} cols)")


# ---------------------------------------------------------------------------
# Config check
# ---------------------------------------------------------------------------

def check_config() -> bool:
    """Validate table definitions and data_sources.yaml consistency."""
    sources = yaml.safe_load(SOURCES_FILE.read_text())
    ok = True
    for key, cfg in TABLES.items():
        # Check each select references the right synapse_id
        if cfg["synapse_id"] not in cfg["select"]:
            print(f"[ERROR] {key}: synapse_id {cfg['synapse_id']} not found in SELECT clause")
            ok = False
        # Check data_sources.yaml agrees
        if key not in sources.get("sources", {}):
            print(f"[ERROR] {key}: not found in data_sources.yaml")
            ok = False
        else:
            yaml_id = sources["sources"][key]["synapse_id"]
            if yaml_id != cfg["synapse_id"]:
                print(f"[ERROR] {key}: synapse_id mismatch — script={cfg['synapse_id']} yaml={yaml_id}")
                ok = False
        # Check all column output names are unique
        names = [c[0] for c in cfg["columns"]]
        if len(names) != len(set(names)):
            print(f"[ERROR] {key}: duplicate output column names")
            ok = False
        print(f"[ok] {key}: {len(cfg['columns'])} columns, synapse_id={cfg['synapse_id']}")
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract ALS KP portal tables to CSV.")
    parser.add_argument("--from-cache", action="store_true",
                        help="Re-process from cached raw CSVs without hitting Synapse.")
    parser.add_argument("--check-config", action="store_true",
                        help="Validate config consistency and exit.")
    parser.add_argument("tables", nargs="*",
                        help="Table keys to process (default: all). Use 'files' or 'datasets'.")
    args = parser.parse_args()

    if args.check_config:
        ok = check_config()
        sys.exit(0 if ok else 1)

    target_keys = [ALIASES.get(t, t) for t in args.tables] if args.tables else list(TABLES.keys())
    invalid = [k for k in target_keys if k not in TABLES]
    if invalid:
        print(f"Unknown table key(s): {invalid}. Valid keys: {list(TABLES.keys())}")
        sys.exit(1)

    syn = None if args.from_cache else _get_syn()

    for key in target_keys:
        print(f"\n=== Processing table: {key} ===")
        cfg = TABLES[key]
        if args.from_cache:
            raw_df = _load_raw(key)
        else:
            raw_df = _query_table(syn, key)
            _save_raw(raw_df, key)

        processed_df = _process_table(raw_df, cfg["columns"])
        _save_processed(processed_df, key)

    print("\nDone.")


if __name__ == "__main__":
    main()
