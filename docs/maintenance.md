# ALS KP KG Pipeline — Maintenance Runbook

## Routine checks

Run these after any portal data update:

```bash
make check-config   # config consistency
make from-cache     # reprocess without hitting Synapse (if raw/ is current)
make validate       # FK constraints (currently none declared, but log output)
make test           # 23 unit tests — must all pass
make sparql         # SPARQL spot-checks — review result counts for anomalies
```

For a fresh pull from Synapse (e.g. after a portal release):
```bash
make extract        # re-fetches both tables live
make rdf
make test
make sparql
```

## Pinning a new snapshot for `syn66496326`

When the dataset collection has been updated and you want to pin the new version:

1. Create a snapshot (requires write access to the project):
   ```python
   import synapseclient
   syn = synapseclient.login()
   syn.create_snapshot_version("syn66496326", comment="Snapshot for KG vX release")
   ```
2. Note the new version number from the response.
3. Update `data_sources.yaml`:
   ```yaml
   datasets:
     source_version: <new_version>
   ```
4. Delete `data/raw/datasets_raw.csv` to force a fresh pull.
5. Re-run: `make extract rdf test`.

## Pinning a snapshot for `syn66271104` (file view)

No snapshots exist yet. Before any production release build, create one:
```python
import synapseclient
syn = synapseclient.login()
syn.create_snapshot_version("syn66271104", comment="Snapshot for KG vX release")
```
Then update `data_sources.yaml` `source_version` accordingly.

## Adding a new table

1. **Inspect the table:**
   ```python
   import synapseclient, warnings
   warnings.filterwarnings("ignore")
   syn = synapseclient.Synapse()
   cols = list(syn.getTableColumns("synXXXXXXX"))
   for c in cols: print(f"{c.name}: {c.columnType}")
   ```

2. **Add to `data_sources.yaml`:**
   ```yaml
   my_new_table:
     synapse_id: synXXXXXXX
     concrete_type: EntityView   # or TableEntity / MaterializedView
     note: "Description of what this table contains."
     source_version: 1           # or null if no snapshots
   ```

3. **Add to `scripts/prepare_portal_tables.py`:**
   - Define `MY_TABLE_SELECT` query string
   - Define `MY_TABLE_COLUMNS` list of `(out_name, src_name, transform)` tuples
   - Add entry to `TABLES` dict with `csv_path`, `select`, `columns`, `fk_refs`

4. **Extend the ontology (`schema/ontology.ttl`):**
   - Add a new class (align to BioLink if applicable)
   - Add datatype and object properties

5. **Write `mappings/rml/my_new_table.rml.ttl`** — copy an existing mapping as template.

6. **Write `test/test_my_new_table_mapping.py`** — cover count, IRI pattern, multi-value splits, nulls.

7. **Add Dagster assets** to `orchestration/dagster_pipeline/assets.py`:
   - `csv_my_new_table` (CSV asset, depends on Synapse)
   - `rdf_my_new_table` (RDF asset, depends on CSV asset)
   - Add both to `defs` in `definitions.py`

8. **Add to Makefile** — extend `CSV_FILES` and `RDF_FILES` variables.

9. **Write a SPARQL use-case query** in `sparql/` that exercises the new table.

## Updating the ontology

The ontology at `schema/ontology.ttl` is the source of truth for property names used in RML mappings.
When adding a property:
1. Add the `owl:DatatypeProperty` or `owl:ObjectProperty` declaration to `ontology.ttl`.
2. Update the relevant `{table}.rml.ttl` mapping to use the new predicate.
3. Update test assertions if the property is expected to be non-null for known entities.

Avoid renaming properties once the graph is published — use `owl:deprecated` + `owl:equivalentProperty`
to mark the old name as superseded and add the new name.

## Handling FK violations

`make validate` runs `scripts/validate_fks.py`. Currently no cross-table FK constraints are declared
(see `fk_refs` in `TABLES`). When constraints are added:

- A pass means every FK value has a matching PK in the referenced table.
- Failures are **non-blocking** by default — they represent upstream data quality issues to report
  to the portal owner, not pipeline errors.
- To fail CI on violations: `python scripts/validate_fks.py --strict`

## Adding a Layer 2 derived relationship

Derived relationships (cross-table edges not directly in source tables) belong in
`scripts/materialize_*.py` and corresponding Dagster assets. See the reference NF implementation for
examples (shared donor links, mutation sets).

The most immediate Layer 2 candidate for this pipeline is the file↔dataset link via GEO accession —
see `docs/architecture.md` for the proposed SPARQL CONSTRUCT pattern.

## Depositing to Sage Brain

Building and publishing are separate workflows, so a build can be produced and
inspected without anything reaching Sage Brain, and a deposit always names the
build it is publishing.

| Workflow | Does | Never does |
|---|---|---|
| `build-graph.yml` — *Build graph snapshot* | Extract, generate, validate, upload the `kg-snapshot` artifact | Touch AWS (unless dispatched with `deposit: true`, which hands off to the workflow below) |
| `deposit-sagebrain.yml` — *Deposit to Sage Brain* | Download a build's artifact, re-verify it, upload it to S3 | Build, or alter what the build produced |

The Sage Brain append-only ingestion pipeline then bulk-loads the snapshot into
Neptune. See [sagebrain-infra#39](https://github.com/Sage-Bionetworks-IT/sagebrain-infra/pull/39).

### How it runs

**Build.** Runs on a `v*` tag push, or manually from any branch. Extraction is
anonymous (portal Layer 1 metadata is public), so no Synapse credentials are
needed and a build from any branch is harmless. A build leaves two artifacts:
`kg-snapshot` (90 days — exactly the bytes a deposit would upload, plus
`build-metadata.json`) and `kg-intermediates` (14 days — raw exports, CSVs, RML
logs, for debugging a bad build).

**Deposit.** Three ways to reach it, in increasing order of automation:

| Path | How | When to use |
|---|---|---|
| Deposit an existing build | Run *Deposit to Sage Brain* with `build_run_id` = a successful build run | The normal path — look at the build first, publish it after |
| Build and deposit | Run *Build graph snapshot* with `deposit: true` | A release you already intend to publish |
| Rehearse | *Deposit to Sage Brain* with `dry_run: true` | Check the payload and manifest with no upload |

A tag push builds only. Tagging publishes nothing on its own; depositing is
always an explicit choice.

The deposit job authenticates to AWS via GitHub OIDC using the
`SAGEBRAIN_ROLE_ARN` repository secret — no long-lived keys.

### Confirming a deposit

The deposit job runs in the `sagebrain-prod` environment. Add **required
reviewers** to that environment (repo Settings → Environments) and every deposit,
chained or standalone, pauses for approval before it fetches or uploads anything.
Restrict the environment's deployment branches too, if the OIDC trust policy is
ever widened.

Two further guards make publishing deliberate rather than incidental:

- a named `build_run_id` must be a **successful** run of `build-graph.yml` in
  this repo — a failed or unrelated run is rejected before the download;
- an S3 prefix that already holds a snapshot for the target date fails the run
  unless it was dispatched with `allow_overwrite: true`.

### One-time setup: `SAGEBRAIN_ROLE_ARN`

The IAM role is already provisioned for this repo by Sage IT — see the
`GithubOidcSageBionetworksItSageBrainInfraAmpAls` task in
[organizations-infra `org-formation/650-identity-providers/_tasks.yaml`](https://github.com/Sage-Bionetworks-IT/organizations-infra/blob/main/org-formation/650-identity-providers/_tasks.yaml).
Only the repository secret is missing:

```bash
gh secret set SAGEBRAIN_ROLE_ARN --repo amp-als/kg-pipeline \
  --body "arn:aws:iam::620117233256:role/sagebase-github-oidc-sage-bionetworks-it-sagebrain-infra-als"
```

`620117233256` is `org-sagebase-sagebrain-prod`, which holds the `app-prod-neptune-*`
bucket. The same role name exists in the dev account (`org-sagebase-sagebrain-dev`)
if a dev target is ever wanted.

The role grants S3 only — `ListBucket` plus `PutObject`/`GetObject`/`DeleteObject`
on `app-*-neptune-neptunedatabucket*`. That covers `sync --delete`; it grants no
Neptune access, since loading is the pipeline's job, not ours.

**Trusted refs.** The trust policy accepts `refs/tags/*`, `refs/heads/main` and
`refs/heads/develop` only, and that applies to the ref the *deposit* runs from.
Deposits from a tag or `main` work; one dispatched from a feature branch is
rejected up front. Builds are unaffected — they use no credentials, so a feature
branch can build freely and the resulting artifact can be deposited later from
`main`.

### What lands in S3

```
s3://<NeptuneDataBucketName>/als/YYYY-MM-DD/
    data/schema/ontology.ttl
    data/rdf/files.ttl
    data/rdf/datasets.ttl
    data/_provenance.ttl    ← build lineage, inside the load path
    manifest.ttl            ← uploaded LAST
```

`data/` is the `kg-snapshot` artifact verbatim; the deposit adds only the two
manifest copies. The manifest records the *build's* commit, ref, run URL and
timestamp alongside the deposit run that published it, so a snapshot in Neptune
points back at the build that made it rather than at whoever pressed the button.

`manifest.ttl` is the completion sentinel: its `ObjectCreated` event triggers the
Neptune bulk load into `urn:sagebrain:als:YYYY-MM-DD`. Upload order matters — if
the manifest landed first, the loader would fire against an incomplete snapshot.

### Why everything loadable lives under `data/`

[sagebrain-infra#42](https://github.com/Sage-Bionetworks-IT/sagebrain-infra/pull/42)
(open at time of writing) narrows the load path from the whole dated folder to the
`data/` subprefix, because Neptune's bulk loader takes a *literal* S3 prefix — no
glob, no extension filter — and parses every object under it as Turtle. With
`failOnError=TRUE`, one stray non-RDF object fails the entire snapshot.

This layout satisfies both loaders: the current one loads the whole folder (all
Turtle, so it succeeds), and #42 loads `data/` (which holds the ontology and the
graph). Keep `data/` Turtle-only; anything else belongs in a sibling folder such
as `other/`.

#42 also moves the sentinel out of the load path, so the manifest's triples would
no longer reach Neptune. `data/_provenance.ttl` is a byte-identical copy deposited
*inside* the load path, which keeps build lineage (run URL, commit, triple count)
queryable. Under the current loader both files are read into the same named graph;
identical triples, so nothing to de-duplicate.

### Gates before the deposit

The load runs with `failOnError=TRUE` and never deletes, so a bad snapshot is
permanent.

In the build, every step must pass before an artifact exists at all:

`make check-config` → `make extract` → `make rdf` → `validate_fks.py --strict`
→ `make test` → `make sparql` → Turtle-only `data/`, non-zero triple count in
every `.ttl`

In the deposit, the downloaded artifact is re-checked before any upload: every
file under `data/` is Turtle, parses, is non-empty, and the total matches the
triple count the build recorded in `build-metadata.json`. A mismatch means the
artifact is not what was validated, and the run stops.

Then, after the upload but before `manifest.ttl` fires the load, the *destination*
is checked: `als/YYYY-MM-DD/data/` must hold only `.ttl` keys, and exactly as many
as were uploaded. The load path is whatever sits under the prefix at sentinel
time — not only what this run wrote — so this catches a leftover from an aborted
run or anything else that wrote there. The sync itself is unfiltered on purpose:
`--exclude "*" --include "*.ttl"` would apply to the destination listing too, and
`--delete` would then leave a stray non-Turtle object in place instead of sweeping
it away.

### Re-running on the same date

A date that already holds a snapshot is refused unless the run sets
`allow_overwrite: true`. With it set, `aws s3 sync --delete` fully replaces the
day's files (a plain `cp` would leave renamed or dropped files behind, and the
loader ingests everything under the prefix). Re-uploading `manifest.ttl` gives it
a new etag, which the Sage Brain loader treats as a genuine re-publish and
reloads. A duplicate event with an unchanged etag is skipped.

### Querying a snapshot

Neptune's SPARQL default graph is the union of all named graphs, so an unscoped
query returns every snapshot merged. Scope to one build:

```sparql
SELECT (COUNT(*) AS ?n) WHERE { GRAPH <urn:sagebrain:als:2026-09-14> { ?s ?p ?o } }
```

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `secret SAGEBRAIN_ROLE_ARN is not set` | Repo secret missing | Add the IAM role ARN from the sagebrain account as a repository secret |
| `'<branch>' is not trusted by the OIDC role` | Deposit dispatched from a feature branch | Deposit from a tag or `main`; the build itself runs anywhere |
| `run <id> is '<name>', not build-graph.yml` | `build_run_id` points at another workflow's run | Copy the run ID from a *Build graph snapshot* run |
| `build run <id> concluded 'failure'` | Naming a build that did not pass its gates | Fix the build and deposit the successful run |
| `artifact holds N triples, build recorded M` | Artifact does not match what the build validated | Re-run the build and deposit that run |
| `already holds a snapshot` | Date was published before | Re-run with `allow_overwrite`, or pick another `snapshot_date` |
| Deposit job sits in *Waiting* | `sagebrain-prod` has required reviewers | Approve the run from the run page |
| `Artifact not found: kg-snapshot` | Build older than the 90-day retention | Re-run the build |
| AWS step fails with `Not authorized to perform sts:AssumeRoleWithWebIdentity` | Role trust policy changed, or the secret points at the wrong account | Check the ARN against the `GithubOidcSageBionetworksItSageBrainInfraAmpAls` task in organizations-infra |
| `snapshot_date '...' is not YYYY-MM-DD` | Bad manual input | The loader rejects any other shape; use e.g. `2026-09-14` |
| Files uploaded but nothing loads into Neptune | `manifest.ttl` not at `als/YYYY-MM-DD/manifest.ttl` | The loader parses exactly three key segments; check the prefix |
| `non-Turtle objects under s3://.../data/` | Something other than this deposit wrote to the load path | Remove or move the object to a sibling folder, then re-run the deposit |
| `holds N objects, expected M` | The prefix was written to concurrently, or a sync was interrupted | Re-run the deposit with `allow_overwrite`; no sentinel was written, so nothing loaded |
| Load fails on a snapshot that uploaded cleanly | A non-Turtle object appeared under `data/` after the deposit | The bulk loader parses everything under the prefix as Turtle; move it to a sibling folder |
| `parsed to zero triples — refusing to publish` | RMLMapper produced an empty graph | Check the `kg-intermediates` artifact (`logs/*_rml.log`) and the source CSVs |

## Test failures

| Symptom | Likely cause | Fix |
|---|---|---|
| `test_file_count` fails with wrong number | New files added to portal | Update count assertion or make it `>= N` |
| `test_dataset_count` fails | New datasets added | Update count assertion |
| `test_no_empty_string_triples` fails | New column added without null handling | Check `_save_processed()` replaces `""` with `None` |
| `grel:string_split null` errors in RML log | Empty cell passed to split function | Expected; non-fatal. Verify no empty-string triples in output. |
| `RMLMapper JAR not found` | JAR not downloaded | `make download-jar` |
| SPARQL use-case returns duplicate rows | Multi-value disease field: `CONTAINS(LCASE(?disease), "als")` also matches "Pre-fALS" | Use exact match: `FILTER(?disease = "ALS")` instead of substring match |

## Contact

- Portal owner / data manager: see Jira project SKG, issue SKG-132
- Reference implementation: [nf-osi/kg-pipeline](https://github.com/nf-osi/kg-pipeline/tree/develop)
- SageBrain data lead: contact for cross-portal alignment questions
