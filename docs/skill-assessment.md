# Knowledge Graph Skill — Assessment and Refactor Proposal

Assessment based on executing [`knowledge-graph-dev/SKILL.md`](https://github.com/Sage-Bionetworks/agent-skills/tree/add-kg-dev-skill/skills/knowledge-graph-dev) to bootstrap this pipeline.

---

## What works well

- **Stage sequencing is correct.** Extract → Harmonize → Map → Derive → Orchestrate → Validate is the right dependency order and maps cleanly to Dagster assets.
- **Reference implementations are high-quality.** Pointing to the NF repo for concrete patterns (RML templates, GREL functions, CSV column conventions) was faster than specifying everything from scratch. The skill correctly says "use this as a template" rather than "implement from first principles."
- **Scope guards are appropriate.** The instructions to skip harmonization unless cross-portal standards exist, and to confirm the ontology with domain experts before writing mappings, are the right conservative defaults.
- **pyoxigraph for testing** is the right call — fast, in-process, no server required for a bootstrap phase.

---

## Gaps and friction encountered

### 1. SDK version skew — `mime_type` vs `format` in pyoxigraph

The skill shows:
```python
store.load(open("/tmp/my_table.ttl"), mime_type="text/turtle")
```
pyoxigraph 0.5.x changed this to `format=pyoxigraph.RdfFormat.TURTLE`. The skill's code example is stale. This produced a `TypeError` on first run.

**Fix:** Replace the example with `format=pyoxigraph.RdfFormat.TURTLE`, or use the `path=` kwarg which infers format from extension and is more version-stable.

### 2. Synapse SDK version skew — `tableQuery(version=N)`

Step 2 says to "pin a `source_version`" but gives no guidance on how to query a pinned version at runtime. Attempting `syn.tableQuery(query, version=2)` raises `TypeError: unexpected keyword argument`. The correct approach for EntityView snapshots (vs TableEntity versions) is undocumented.

**Fix:** Document that `tableQuery` does not support snapshot queries for EntityViews. The workaround is: record the version in `data_sources.yaml` and preserve it in the raw export filename/metadata, so the provenance is captured even if live queries always hit HEAD.

### 3. Empty-string triples — the `""` vs null cell distinction

The skill says "Coerce all numeric columns with a `number` transform so nulls produce empty cells" but doesn't explain that this applies to *all* columns: writing `""` (empty quoted string) to CSV causes RMLMapper to generate `?s ?p ""` triples, while writing a blank unquoted cell generates no triple. This was a non-obvious data quality issue.

**Fix:** Add a note to Step 5 (extraction) and Step 7 (RML): "Write `None`/null to CSV for missing values — never write empty quoted strings. RMLMapper's CSV source treats an empty *unquoted* cell as absent and generates no triple."

### 4. Python-repr STRING_LIST values from raw cache

When Synapse STRING_LIST columns are cached to raw CSV and re-read, they appear as Python repr strings (`['value1', 'value2']`) rather than JSON (`["value1", "value2"]`). The skill doesn't mention this.

**Fix:** Add to Step 5: "When re-reading raw CSV exports, STRING_LIST values may be Python-repr strings (single-quoted lists). Use `ast.literal_eval()` as a fallback after `json.loads()` fails."

### 5. The ontology step requires a design decision that the skill defers too much

Step 4 says "unless the client has delegated design decisions to you, confirm the ontology with the client." In an AI-agent context, the client almost always delegates. The phrasing creates unnecessary ambiguity that makes agents stall or ask unnecessary questions. Additionally, BioLink guidance is helpful but incomplete — the skill doesn't address the case where BioLink has a nominally matching type (`biolink:DataFile`) that is poorly specified in practice.

**Fix:** Make the default "proceed with reasonable BioLink alignment, flag decisions made for client review" rather than "block until confirmed." Add a note about `biolink:DataFile` being used rarely enough that a portal-specific class is often preferable.

### 6. No guidance on orchestration defaults

Step 8 says "use the client's preferred orchestration tool" but the interview step (Step 1) often yields no strong preference. For a 2-table bootstrap, Dagster is overkill; for 10+ tables, it's essential. A decision heuristic would help.

**Fix:** Add: "For ≤3 tables: start with a Makefile. For ≥5 tables or when incremental re-runs matter: use Dagster. Makefile and Dagster are not mutually exclusive — keep the Makefile as a thin wrapper for common operations."

---

## Proposed refactor: multi-agent plugin

The current SKILL.md is a single 9-step document executed by one agent in a long-running session. This works but has two structural problems:

1. **Context exhaustion.** At 10+ tables, a single agent accumulates enough context (table schemas, ontology decisions, RML templates, test output) to approach context limits before reaching Step 9.
2. **Monolithic responsibility.** All expertise — semantic engineering, data engineering, test engineering, orchestration — is loaded into a single agent system prompt. Specialisation improves quality.

### Proposed structure

```
skills/knowledge-graph-dev/
  SKILL.md                    # orchestrator: interviews, delegates, assembles
  agents/
    kg-scoping.md             # Step 1–2: interview + data_sources.yaml
    kg-ontology.md            # Step 4: OWL ontology given table schemas
    kg-extract.md             # Step 5: extraction script + FK validator
    kg-rml.md                 # Step 7: RML mappings + pyoxigraph tests
    kg-orchestrate.md         # Step 8: Dagster or Makefile wiring
    kg-maintenance.md         # Ongoing: snapshot checks, re-runs, anomaly detection
```

**Orchestrator SKILL.md** becomes a coordinator that:
1. Runs `kg-scoping` to produce `data_sources.yaml` and table schemas
2. Runs `kg-ontology` with the schema as input to produce `schema/ontology.ttl`
3. Runs `kg-extract` to produce extraction scripts and initial CSVs
4. Runs `kg-rml` (once per table, in parallel where possible) to produce mappings and tests
5. Runs `kg-orchestrate` to wire the DAG
6. Hands off a `kg-maintenance` agent spec to the repository

Each sub-agent has a narrow, well-defined input/output contract and can be run independently
when updating a single table or fixing a single mapping.

### `kg-maintenance` agent (repo-resident)

Unlike the bootstrap agents, `kg-maintenance` lives in the target repository as
`.claude/agents/kg-maintenance.md` and can be invoked by the portal team without the
full skill plugin. It knows the specific table IDs, version scheme, and test suite for
this portal. See `.claude/agents/kg-maintenance.md` in this repository for the
ALS KP implementation.

---

## Summary of recommended skill changes

| Issue | Priority | Change |
|---|---|---|
| pyoxigraph API example stale | High | Update to `format=pyoxigraph.RdfFormat.TURTLE` |
| Snapshot query SDK limitation undocumented | High | Document workaround in Step 2 |
| Empty-string vs null CSV cell | High | Add note to Steps 5 and 7 |
| Python-repr STRING_LIST cache | Medium | Add note to Step 5 |
| Ontology confirmation default | Medium | Change default to "proceed and flag" |
| Orchestration decision heuristic | Low | Add table-count heuristic to Step 8 |
| Multi-agent refactor | Enhancement | Split into orchestrator + 6 sub-agents |
