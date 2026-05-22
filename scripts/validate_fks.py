"""
ALS Knowledge Portal — foreign key constraint validator.

Reads processed CSVs from data/csv/ and checks that every declared FK
value exists as a PK in the referenced table.  Violations are logged but
non-blocking by default (use --strict for exit code 1 on violations).

Usage:
    python scripts/validate_fks.py
    python scripts/validate_fks.py --json
    python scripts/validate_fks.py --strict
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

# Import authoritative schema from the extraction script
sys.path.insert(0, str(Path(__file__).parent))
from prepare_portal_tables import TABLES  # noqa: E402


@dataclass
class FKConstraint:
    source_table: str
    source_col: str
    target_table: str
    target_col: str


@dataclass
class FKResult:
    constraint: FKConstraint
    total_populated: int
    orphan_count: int
    sample_violations: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.orphan_count == 0

    @property
    def orphan_pct(self) -> float:
        if self.total_populated == 0:
            return 0.0
        return round(100.0 * self.orphan_count / self.total_populated, 1)


def discover_constraints() -> list[FKConstraint]:
    """Scan TABLES schema for declared FK references."""
    constraints = []
    for table_key, cfg in TABLES.items():
        for fk_ref in cfg.get("fk_refs", []):
            src_col, target_table, target_col = fk_ref
            constraints.append(FKConstraint(
                source_table=table_key,
                source_col=src_col,
                target_table=target_table,
                target_col=target_col,
            ))
    return constraints


def check_constraint(
    constraint: FKConstraint,
    data_dir: Path,
    pk_cache: dict[tuple[str, str], set[str]],
) -> FKResult | None:
    """Validate a single FK constraint; returns None if source CSV is missing."""
    src_path = TABLES[constraint.source_table]["csv_path"]
    tgt_path = TABLES[constraint.target_table]["csv_path"]

    if not src_path.exists():
        print(f"  [skip] {constraint.source_table}: CSV not found at {src_path}")
        return None
    if not tgt_path.exists():
        print(f"  [skip] {constraint.target_table}: CSV not found at {tgt_path}")
        return None

    # Load PK set with cache
    pk_key = (constraint.target_table, constraint.target_col)
    if pk_key not in pk_cache:
        tgt_df = pd.read_csv(tgt_path, dtype=str, keep_default_na=False)
        if constraint.target_col not in tgt_df.columns:
            print(f"  [warn] PK column '{constraint.target_col}' not in {constraint.target_table}")
            pk_cache[pk_key] = set()
        else:
            pk_cache[pk_key] = set(tgt_df[constraint.target_col].dropna().unique())

    pk_set = pk_cache[pk_key]

    # Load source FK column
    src_df = pd.read_csv(src_path, dtype=str, keep_default_na=False)
    if constraint.source_col not in src_df.columns:
        print(f"  [warn] FK column '{constraint.source_col}' not in {constraint.source_table}")
        return FKResult(constraint=constraint, total_populated=0, orphan_count=0)

    fk_values = src_df[constraint.source_col]
    populated = fk_values[fk_values != ""]
    orphans = populated[~populated.isin(pk_set)]

    return FKResult(
        constraint=constraint,
        total_populated=len(populated),
        orphan_count=len(orphans),
        sample_violations=orphans.unique()[:5].tolist(),
    )


def validate_all(data_dir: Path) -> list[FKResult]:
    constraints = discover_constraints()
    if not constraints:
        print("No FK constraints declared — nothing to validate.")
        return []

    pk_cache: dict[tuple[str, str], set[str]] = {}
    results = []
    for c in constraints:
        result = check_constraint(c, data_dir, pk_cache)
        if result is not None:
            results.append(result)
    return results


def print_report(results: list[FKResult]) -> None:
    if not results:
        print("No FK constraints to report.")
        return
    print(f"\n{'='*60}")
    print(f"FK Validation Report ({len(results)} constraints)")
    print(f"{'='*60}")
    for r in results:
        c = r.constraint
        status = "PASS" if r.passed else "FAIL"
        print(f"\n[{status}] {c.source_table}.{c.source_col} → {c.target_table}.{c.target_col}")
        print(f"  Populated rows: {r.total_populated}")
        if not r.passed:
            print(f"  Orphans: {r.orphan_count} ({r.orphan_pct}%)")
            print(f"  Sample violations: {r.sample_violations}")
    passes = sum(1 for r in results if r.passed)
    print(f"\n{'='*60}")
    print(f"Summary: {passes}/{len(results)} constraints passed")


def print_json(results: list[FKResult]) -> None:
    out = []
    for r in results:
        c = r.constraint
        out.append({
            "source_table": c.source_table,
            "source_col": c.source_col,
            "target_table": c.target_table,
            "target_col": c.target_col,
            "total_populated": r.total_populated,
            "orphan_count": r.orphan_count,
            "orphan_pct": r.orphan_pct,
            "passed": r.passed,
            "sample_violations": r.sample_violations,
        })
    print(json.dumps(out, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate FK constraints across portal CSVs.")
    parser.add_argument("--data-dir", default=None,
                        help="Path to processed CSV directory (default: data/csv/ relative to repo root).")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of human-readable report.")
    parser.add_argument("--strict", action="store_true", help="Exit 1 if any violation is found.")
    args = parser.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = Path(__file__).parent.parent / "data" / "csv"

    results = validate_all(data_dir)

    if args.json:
        print_json(results)
    else:
        print_report(results)

    if args.strict and any(not r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
