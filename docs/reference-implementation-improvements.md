# Recommended improvements to the NF reference implementation

Issue filed from the ALS KP bootstrap. These findings are based on executing
the `knowledge-graph-dev` skill against a new 2-table portal (ALS KP,
[Sage-Bionetworks/amp-als/kg-pipeline](https://github.com/Sage-Bionetworks/amp-als/kg-pipeline)),
reviewing the reference NF pipeline code at
[nf-osi/kg-pipeline@develop](https://github.com/nf-osi/kg-pipeline/tree/develop),
and a self-review of the resulting translation.

---

## Current assessed grades

| Component | Grade | Summary |
|---|---|---|
| **Reference NF pipeline** | **B+** | Mature, production-proven (25 tables, QLever deployment). Sound architecture. Weaknesses are DRY violations that accumulate maintenance burden at scale. |
| **SKILL.md (knowledge-graph-dev)** | **B** | Correct stage sequencing, good reference pointers. Six SDK/API gaps caused friction during bootstrap execution. |
| **Dagster orchestration** | **B+** | Good factory pattern for 25+ tables. Weaknesses in hardcoded dependency strings and bypassed materialisation tracking. |

---

## Improvements — reference pipeline (`nf-osi/kg-pipeline`)

### 1. `TABLES` dict is config-in-code (480 lines)

**Current:** `scripts/prepare_portal_tables.py` contains ~480 lines of hand-maintained
dict literals. The `*_SELECT` constants nearly duplicate the `columns` list — both encode
source-to-target column mappings. A `_synapse_select_clause()` helper exists to derive
SELECT from columns, but the manual clauses are still the primary path.

**Impact:** Every schema change (add/rename/remove a column) requires editing both the
SELECT string and the columns list, then verifying they match. At 25+ tables this is the
single biggest source of maintenance friction.

**Fix:** Derive the SELECT clause from the columns config. The column list is already the
authoritative source; the SELECT should be generated from it:

```python
def _build_select(synapse_id: str, columns: list) -> str:
    col_names = [src_name for _, src_name, _ in columns]
    return f"SELECT {', '.join(col_names)} FROM {synapse_id}"
```

Remove the manual `*_SELECT` constants. This eliminates ~200 lines and one full class
of copy-paste bugs.

**Grade impact:** B+ → A-

---

### 2. `data_sources.yaml` profile duplication

**Current:** The `release` and `evaluation` profiles are near-complete copy-pastes differing
in 2–3 version numbers. Adding a new table requires editing both profiles with no enforcement
that they stay in sync.

**Fix:** Use YAML anchors/aliases:

```yaml
_base_sources: &base_sources
  studies:
    synapse_id: syn21868602
    # ...

release:
  sources:
    <<: *base_sources
    cell_lines:
      source_version: 8  # override for release

evaluation:
  sources:
    <<: *base_sources
    cell_lines:
      source_version: 9  # override for evaluation
```

**Grade impact:** Minor (documentation quality, not correctness).

---

### 3. Hardcoded dependency strings in Dagster assets

**Current:** `create_csv_asset()` in `assets.py` contains:

```python
if table_name == "animal_models":
    deps.append(["portal", "csv", "donors"])
if table_name == "cell_lines":
    deps.append(["portal", "csv", "donors"])
```

This duplicates the dependency knowledge that lives in `apply_derived_columns` in the
extraction script.

**Impact:** If a third table gains a donor dependency, both files must be updated. The
Dagster graph becomes silently wrong if only one is edited.

**Fix:** Derive Dagster deps from the extraction script's dependency metadata. Add a
`derived_from` field to the TABLES dict:

```python
"animal_models": {
    "derived_from": ["donors"],  # used by both extraction and Dagster
    ...
}
```

Then in `create_csv_asset`:
```python
deps = [["portal", "csv", d] for d in config.get("derived_from", [])]
```

**Grade impact:** B+ → A- (eliminates a class of silent inconsistency)

---

### 4. Donors bypass Dagster materialisation tracking

**Current:** `_csv_asset` reads `data/csv/donors.csv` from disk via `pd.read_csv()` rather
than receiving the data through Dagster's asset dependency mechanism. If the donors asset
fails but a stale CSV exists from a prior run, downstream assets silently use stale data
without Dagster knowing.

**Fix:** Either return the DataFrame as the asset's output value (Dagster IO manager), or
at minimum verify the CSV's mtime is newer than the current materialisation start time.

**Grade impact:** B+ → A (correctness under failure conditions)

---

### 5. No ontology/RML cross-validation

**Current:** No automated check that predicates used in RML mappings are declared in the
ontology. A property can be mapped in RML but absent from `schema/ontology.ttl`, producing
valid RDF that loads and queries fine but breaks OWL reasoners and SHACL validators.

This bug was found in the ALS KP translation: `alskp:subject` was used in
`datasets.rml.ttl` but never declared in the ontology. It passed all 22 unit tests
because pyoxigraph doesn't enforce schema.

**Fix:** Add `test/test_ontology_rml_consistency.py` that parses both files, extracts
predicate IRIs, and asserts every RML predicate is declared in the ontology (filtering
RML plumbing like `rdf:type`, `grel:*`, `fno:*`). We implemented this for ALS KP — the
same pattern should be added to the NF pipeline.

**Grade impact:** B+ → A (catches an otherwise-invisible class of bug)

---

### 6. No enforced anonymous access

**Current:** The comment says "Do NOT call `syn.login()`" but if `SYNAPSE_AUTH_TOKEN` is
present in the environment, `synapseclient.Synapse()` silently authenticates. This means:
- Local runs may succeed due to implicit auth while CI (intended anonymous) fails
- The assertion "this pipeline works without credentials" is not tested

**Fix:** Explicitly clear auth state:

```python
import os
os.environ.pop("SYNAPSE_AUTH_TOKEN", None)
syn = synapseclient.Synapse()
```

Or validate after construction:
```python
syn = synapseclient.Synapse()
assert not syn.credentials, "Pipeline requires anonymous access; unset SYNAPSE_AUTH_TOKEN"
```

**Grade impact:** Minor (robustness, not correctness in typical use).

---

### 7. No retry/timeout on Synapse API calls

**Current:** `syn.tableQuery()` is called with no retry logic, timeout, or error handling
beyond the SDK's defaults. A transient Synapse outage or rate limit fails the entire
pipeline.

**Fix:** Wrap in a retry with exponential backoff:

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
def _query_with_retry(syn, query):
    return syn.tableQuery(query)
```

**Grade impact:** Minor (production resilience, not correctness).

---

## Improvements — SKILL.md (`knowledge-graph-dev`)

### 8. pyoxigraph API example is stale (High)

The skill shows `store.load(open(...), mime_type="text/turtle")`.
pyoxigraph ≥0.4 uses `format=pyoxigraph.RdfFormat.TURTLE`.

**Fix:** Update the code example. Also document that `QuerySolution` fields
are accessed via `.value` (not `str()`), and that ASK queries return
`QueryBoolean` (use `bool(result)`), not an iterable.

### 9. Snapshot query SDK limitation undocumented (High)

Step 2 says to "pin a `source_version`" with no guidance on how to actually
query a pinned version. `syn.tableQuery(query, version=N)` raises `TypeError`
for EntityViews.

**Fix:** Document: "The Synapse Python SDK does not support snapshot queries
for EntityViews via `tableQuery`. Record the version in `data_sources.yaml`;
raw CSV exports capture the extraction state. Query always hits HEAD."

### 10. Empty-string triple generation undocumented (High)

Writing `""` to CSV produces `?s ?p ""` triples; writing a blank cell
produces no triple. This distinction is critical for data quality but only
hinted at in the skill's numeric coercion note.

**Fix:** Add to Steps 5 and 7: "Write `None` (not `""`) for missing values.
`df.replace("", None)` before `to_csv()`. RMLMapper treats blank unquoted
cells as absent."

### 11. STRING_LIST cache roundtrip not documented (Medium)

Synapse SDK returns lists live but they serialise as Python-repr strings in
cached CSVs. `json.loads()` fails on `['val']` (single quotes).

**Fix:** Add to Step 5: "Fall back to `ast.literal_eval()` after
`json.loads()` when parsing cached STRING_LIST values."

### 12. Ontology confirmation default stalls agents (Medium)

Step 4 says "confirm with client before proceeding." In AI-agent contexts the
client has almost always delegated. Agents stall or ask unnecessary questions.

**Fix:** Change default to "proceed with reasonable BioLink alignment and flag
decisions for client review."

### 13. No orchestration decision heuristic (Low)

Step 8 says "use the client's preferred tool" but clients often have no
preference.

**Fix:** Add: "≤3 tables → Makefile only. ≥4 tables → Makefile + Dagster.
Not mutually exclusive."

### 14. GREL function files described as downloaded, not committed (Low)

The skill implies downloading `functions_grel.ttl` and `grel_java_mapping.ttl`
at runtime. They should be committed to the repository — they're static
support files, not generated outputs.

**Fix:** Add to Step 7 (or a setup step): "Commit `tools/functions_grel.ttl`
and `tools/grel_java_mapping.ttl` to the repository. Only the RMLMapper JAR
(184 MB) should be gitignored and downloaded at build time."

---

## Estimated grade improvements

| Component | Current | After fixes | Key drivers |
|---|---|---|---|
| **Reference NF pipeline** | B+ | **A** | #1 (derive SELECT, -200 LOC), #3 (declarative deps), #5 (cross-validation) |
| **SKILL.md** | B | **A-** | #8–#10 (SDK fixes), #12 (agent-friendly defaults), #14 (GREL committed) |
| **Dagster orchestration** | B+ | **A** | #3 (declarative deps), #4 (materialisation tracking), factory pattern |
| **Overall system** | B+ | **A** | Cross-validation test + derived SELECT eliminate the two most common bug classes |

The biggest single improvement across the system is **#5 (ontology/RML cross-validation)** —
it's a 30-minute implementation that prevents an otherwise-invisible class of bug that only
surfaces when formal validation is attempted, often weeks or months after the predicate was
introduced.
