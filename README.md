# ALS KP Knowledge Graph Pipeline

Converts structured metadata from Synapse portal tables into a queryable RDF knowledge graph for the [ALS Knowledge Portal](https://www.synapse.org/Synapse:syn64892175).

## Overview

| Layer | Description | Status |
|---|---|---|
| Core | Portal entity metadata → RDF (files, datasets) | ✅ Bootstrap complete |
| Derived | Cross-table relationships | Planned |
| Publications | PubMed/PMC full-text index | Not applicable |

**Graph contents (Layer 1):**
- `data/rdf/files.ttl` — ~204k triples from 8,590 portal files (syn66271104)
- `data/rdf/datasets.ttl` — ~770 triples from 25 curated datasets (syn66496326)

## Quick Start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Download RMLMapper JAR (one-time, ~184 MB)
make download-jar

# 3. Full pipeline: extract → RDF → test
make all

# Or step by step:
make extract        # pull from Synapse → data/csv/
make rdf            # RML mappings → data/rdf/
make test           # 23 pytest assertions
make sparql         # SPARQL use-case spot-checks
make validate       # FK constraint check
```

**Re-run from cached raw exports (no Synapse access needed):**
```bash
make from-cache
```

**Launch Dagster UI** (requires full package install — `pip install -e .`):
```bash
make dagster
# then open http://localhost:3000
```

## Repository Layout

```
kg-pipeline/
├── data_sources.yaml              # Synapse table registry (IDs, versions)
├── schema/ontology.ttl            # OWL ontology (alskp: namespace)
├── scripts/
│   ├── prepare_portal_tables.py   # Extraction: Synapse → data/csv/
│   ├── validate_fks.py            # FK constraint checker
│   └── run_sparql_checks.py       # SPARQL use-case runner
├── mappings/rml/
│   ├── files.rml.ttl              # File view → RDF
│   └── datasets.rml.ttl           # Dataset collection → RDF
├── orchestration/dagster_pipeline/ # Dagster assets + resources
├── sparql/                        # SPARQL use-case queries (*.rq)
├── test/                          # pytest unit tests (pyoxigraph SPARQL)
├── tools/                         # RMLMapper JAR + GREL function files
│   ├── functions_grel.ttl
│   ├── grel_java_mapping.ttl
│   └── rmlmapper-8.1.0.jar        # gitignored — download with make download-jar
└── data/
    ├── raw/                       # gitignored — Synapse raw exports (cache)
    ├── csv/                       # gitignored — processed inputs to RMLMapper
    └── rdf/                       # gitignored — generated RDF output
```

## Data Sources

| Key | Synapse ID | Type | Pinned Version | Content |
|---|---|---|---|---|
| `files` | [syn66271104](https://www.synapse.org/Synapse:syn66271104) | EntityView | none (no snapshots exist) | 8,590 portal files |
| `datasets` | [syn66496326](https://www.synapse.org/Synapse:syn66496326) | EntityView | v2 (documented; SDK limitation prevents live query pinning) | 25 curated datasets |

See `data_sources.yaml` for full details.

## Ontology

Namespace: `https://alskp.synapse.org/terms#` (prefix `alskp:`)

Key classes:
- `alskp:PortalFile` — individual data file in the portal file view
- `alskp:Dataset rdfs:subClassOf biolink:Dataset` — curated study-level dataset (both types are materialized in the RDF; GraphDB runs without reasoning)

See `schema/ontology.ttl` and `docs/architecture.md`.

## SPARQL Use Cases

Pre-written queries in `sparql/`:

| File | Question answered |
|---|---|
| `uc1_als_rnaseq_datasets.rq` | Which RNA-seq datasets cover ALS? |
| `uc2_dataset_landscape.rq` | What assays × diseases are in the portal? |
| `uc3_files_for_dataset.rq` | What files belong to a given GEO series? |
| `uc4_epigenomics_files.rq` | Which CNS regions have epigenomics data? |
| `uc5_postmortem_human_files.rq` | What post-mortem human tissue files exist for ALS? |

Run all: `make sparql`

## Requirements

| Tool | Version | Notes |
|---|---|---|
| Python | ≥3.10 | |
| Java | ≥21 | For RMLMapper |
| RMLMapper | 8.1.0 | `make download-jar` |
| Dagster | ≥1.9 | Included in requirements.txt |

## See Also

- [Architecture](docs/architecture.md) — design decisions and graph structure
- [Maintenance runbook](docs/maintenance.md) — how to add tables, update versions, handle data changes
- Reference NF implementation: [nf-osi/kg-pipeline](https://github.com/nf-osi/kg-pipeline/tree/develop)
