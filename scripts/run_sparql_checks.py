"""
ALS KP — SPARQL use-case spot-check runner.

Loads all RDF files from data/rdf/ into a pyoxigraph store and runs every
.rq query in sparql/.  Reports result counts and first 3 rows for each query.

Usage:
    python scripts/run_sparql_checks.py
    python scripts/run_sparql_checks.py --query sparql/uc1_als_rnaseq_datasets.rq
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyoxigraph

ROOT = Path(__file__).parent.parent
RDF_DIR = ROOT / "data" / "rdf"
SPARQL_DIR = ROOT / "sparql"


def load_store() -> pyoxigraph.Store:
    store = pyoxigraph.Store()
    ttl_files = sorted(RDF_DIR.glob("*.ttl"))
    if not ttl_files:
        print(f"[error] No .ttl files found in {RDF_DIR}. Run 'make rdf' first.")
        sys.exit(1)
    for ttl in ttl_files:
        store.bulk_load(ttl.open("rb"), format=pyoxigraph.RdfFormat.TURTLE)
        print(f"  [loaded] {ttl.name}")
    return store


def run_query(store: pyoxigraph.Store, query_file: Path) -> None:
    query = query_file.read_text()
    print(f"\n{'='*60}")
    print(f"Query: {query_file.name}")
    print("="*60)

    try:
        result = store.query(query)
    except Exception as e:
        print(f"  [error] {e}")
        return

    if hasattr(result, "variables"):
        # SELECT query — QuerySolutions object
        keys = [v.value for v in result.variables]
        rows = list(result)
        print(f"  Columns : {keys}")
        print(f"  Row count: {len(rows)}")
        for row in rows[:5]:
            parts = []
            for k in keys:
                v = row[k]
                parts.append(f"{k}={v.value!r}" if v is not None else f"{k}=NULL")
            print("  " + "  |  ".join(parts))
        if len(rows) > 5:
            print(f"  ... ({len(rows) - 5} more rows)")
    elif isinstance(result, pyoxigraph.QueryBoolean):
        print(f"  ASK result: {bool(result)}")
    else:
        # CONSTRUCT or other
        triples = list(result)
        print(f"  Triples: {len(triples)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SPARQL spot-checks against local RDF store.")
    parser.add_argument("--query", help="Path to a specific .rq file (default: all in sparql/).")
    args = parser.parse_args()

    print("Loading RDF store...")
    store = load_store()
    print(f"  Total triples: {sum(1 for _ in store)}")

    if args.query:
        query_files = [Path(args.query)]
    else:
        query_files = sorted(SPARQL_DIR.glob("*.rq"))

    if not query_files:
        print(f"[error] No .rq files found in {SPARQL_DIR}.")
        sys.exit(1)

    for qf in query_files:
        run_query(store, qf)

    print("\nDone.")


if __name__ == "__main__":
    main()
