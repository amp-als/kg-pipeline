# ALS KP Knowledge Graph — Architecture

## Graph layers

This pipeline implements **Layer 1** of the reference architecture (see [nf-osi/kg-pipeline architecture doc](https://github.com/nf-osi/kg-pipeline/blob/develop/docs/kg-pipeline-architecture.md)). The full layer model:

| Layer | Scope | This repo |
|---|---|---|
| 1 — Core | Public portal metadata → RDF | ✅ Complete (files, datasets) |
| 2 — Derived | Materialized cross-table edges | Planned (file↔dataset join) |
| 3 — Publications | PubMed/PMC full-text index | Not started |
| 4 — External ontologies | OBO / external KGs | Not started |

## Namespace and ontology design

**Namespace:** `https://alskp.synapse.org/terms#` (prefix `alskp:`)

**IRI scheme for portal entities:**
```
https://www.synapse.org/Synapse:{synapseId}
```
This makes every portal entity directly dereferenceable in a browser.

**Class alignment strategy:**
- `alskp:Dataset rdfs:subClassOf biolink:Dataset` — datasets align well to BioLink
- `alskp:PortalFile` — no BioLink parent. `biolink:DataFile` exists but is rarely used in practice and has unclear semantics; a portal-specific class avoids overfitting to an immature type hierarchy.
- Future: when/if subjects or donors are added (Layer 2), use `biolink:Patient` / `biolink:IndividualOrganism`.

**Why `rdfs:subClassOf` not `owl:equivalentClass`:**
Bidirectional subsumption (`equivalentClass`) forces a reasoner to infer that any `biolink:Dataset` is an `alskp:Dataset`, which is false. Subclassing gives the correct one-directional relationship.

## Pipeline stages

```
Synapse (public)
    │
    ▼ scripts/prepare_portal_tables.py
data/raw/{table}_raw.csv          ← raw export cache (gitignored)
    │
    ▼ (transform: list-flatten, numeric coerce, null→empty cell)
data/csv/{table}.csv              ← RMLMapper input (gitignored)
    │
    ▼ java -jar tools/rmlmapper-8.1.0.jar -m mappings/rml/{table}.rml.ttl
data/rdf/{table}.ttl              ← Turtle output (gitignored)
    │
    ▼ pyoxigraph (in-process)
SPARQL queries / pytest assertions
```

**Key design choices in extraction:**
- Empty values → blank CSV cells (not `""`) so RMLMapper generates no triple for absent data.
  See `_save_processed()` in `prepare_portal_tables.py`.
- Synapse `STRING_LIST` columns come back as Python list objects live, and as Python-repr strings
  (`['value']`) from cached CSVs. Both forms are handled by `_fmt_string_list()`.
- `totalReads` is STRING in the Synapse schema but coerced to integer for RDF.

## Multi-value handling

Portal columns like `disease`, `assay`, `contributor` are multi-value fields. The Synapse SDK returns
them as Python lists; the portal UI displays them comma-separated. The extraction script normalises
both representations to pipe-delimited strings in the processed CSVs. RML uses `grel:string_split` with `\\|`
as separator to produce one triple per value:

```turtle
map:DatasetDisease a rr:TriplesMap ;
    rr:predicateObjectMap [
        rr:predicate alskp:disease ;
        rr:objectMap [
            a fnml:FunctionTermMap ;
            fnml:functionValue [
                fno:executes grel:string_split ;
                grel:valueParameter  "ALS|Control|FTD" ;
                grel:p_string_sep    "\\|"
            ]
        ]
    ] .
```

`grel:string_split` logs a null-input error when the field is empty — this is non-fatal and expected.

## Cross-table relationship (Layer 2 — planned)

Files and datasets share GEO accession IDs:
- `files.GSE` (column `gseId` in RDF) — e.g. `GSE115310`
- `datasets.sameAs` — e.g. `geo:GSE115310`

A Layer 2 derived asset could materialise `alskp:partOfDataset` edges:
```sparql
CONSTRUCT { ?file alskp:partOfDataset ?dataset }
WHERE {
    ?file   alskp:gseId ?gse .
    ?dataset alskp:sameAs ?sameAsRaw .
    FILTER(STRENDS(STR(?sameAsRaw), ?gse))
}
```
This yields ~1,800 file→dataset links based on the current data.

## Version pinning

- `syn66271104` (file view): no snapshots exist. The table is queried live. Create a snapshot before
  any release build: `syn.create_snapshot_version("syn66271104")`.
- `syn66496326` (dataset collection): snapshot v2 is the intended source. The Python SDK's `tableQuery`
  does not support versioned EntityView queries; the version is documented in `data_sources.yaml` and
  the raw export captures the extraction state at build time.

## Testing strategy

**Unit tests** (`test/`): Each RML mapping is exercised by running RMLMapper and loading the output
into an in-process pyoxigraph store. Assertions cover:
- Triple count (exact for datasets, exact for files)
- IRI pattern correctness
- Multi-value splits produce multiple triples
- Integer/anyURI datatype typing
- No empty-string triples

**Use-case SPARQL** (`sparql/`): Five representative queries verified against the live graph. These
are the primary acceptance criteria for Layer 1 — all must return non-empty results before handoff.

## Orchestration

Two equivalent ways to run the pipeline:

| Method | Command | Use when |
|---|---|---|
| Makefile | `make all` | CI, simple re-runs, scripting |
| Dagster | `make dagster` then materialise in UI | Monitoring, lineage tracking, incremental re-runs |

The Dagster asset graph (`orchestration/dagster_pipeline/`) reuses the same scripts and RML mappings;
it adds metadata tracking (row counts, file sizes) and dependency enforcement.
