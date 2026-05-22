# ALS Knowledge Portal — KG pipeline Makefile
#
# Stages:
#   extract   → data/csv/{files,datasets}.csv
#   rdf       → data/rdf/{files,datasets}.ttl
#   validate  → FK validation report
#   test      → pytest unit tests
#   sparql    → run SPARQL use-case spot-checks against data/rdf/
#   dagster   → launch Dagster UI (dev mode)
#   all       → extract + rdf + test
#
# Usage:
#   make              # run full pipeline
#   make extract      # download from Synapse and write CSVs
#   make from-cache   # reprocess CSVs from cached raw exports
#   make rdf          # run RML mappings (requires CSVs)
#   make test         # run pytest suite (requires CSVs + RMLMapper JAR)
#   make validate     # run FK validation
#   make check-config # validate config consistency
#   make sparql       # run SPARQL spot-checks against local RDF files
#   make dagster      # launch Dagster UI

PYTHON      := python3
RMLMAPPER   := tools/rmlmapper-8.1.0.jar
RMLMAPPER_VERSION := 8.1.0
FUNCTIONS_GREL := tools/functions_grel.ttl
GREL_MAPPING   := tools/grel_java_mapping.ttl
DAGSTER_MODULE := orchestration.dagster_pipeline.definitions

JAVA_ARGS := -jar $(RMLMAPPER) \
             -f $(FUNCTIONS_GREL) \
             -f $(GREL_MAPPING)

CSV_FILES := data/csv/files.csv data/csv/datasets.csv
RDF_FILES := data/rdf/files.ttl data/rdf/datasets.ttl

.PHONY: all extract from-cache rdf test validate check-config download-jar sparql dagster clean

all: extract rdf test

# ---------------------------------------------------------------------------
# Config check
# ---------------------------------------------------------------------------
check-config:
	$(PYTHON) scripts/prepare_portal_tables.py --check-config

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
extract: $(CSV_FILES)

$(CSV_FILES): scripts/prepare_portal_tables.py data_sources.yaml
	@mkdir -p data/csv data/raw
	$(PYTHON) scripts/prepare_portal_tables.py

from-cache:
	@mkdir -p data/csv
	$(PYTHON) scripts/prepare_portal_tables.py --from-cache

# ---------------------------------------------------------------------------
# RDF generation via RMLMapper
# ---------------------------------------------------------------------------
rdf: $(RDF_FILES)

data/rdf/files.ttl: mappings/rml/files.rml.ttl data/csv/files.csv $(RMLMAPPER)
	@mkdir -p data/rdf logs
	java $(JAVA_ARGS) -m mappings/rml/files.rml.ttl -o $@ 2>logs/files_rml.log
	@echo "Generated $@ ($$( wc -l < $@ ) triples)"

data/rdf/datasets.ttl: mappings/rml/datasets.rml.ttl data/csv/datasets.csv $(RMLMAPPER)
	@mkdir -p data/rdf logs
	java $(JAVA_ARGS) -m mappings/rml/datasets.rml.ttl -o $@ 2>logs/datasets_rml.log
	@echo "Generated $@ ($$( wc -l < $@ ) triples)"

# ---------------------------------------------------------------------------
# FK validation
# ---------------------------------------------------------------------------
validate:
	$(PYTHON) scripts/validate_fks.py

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
test: $(RMLMAPPER)
	pytest test/ -x -q

# ---------------------------------------------------------------------------
# SPARQL spot-checks (runs queries in sparql/ against local RDF files)
# ---------------------------------------------------------------------------
sparql: $(RDF_FILES)
	$(PYTHON) scripts/run_sparql_checks.py

# ---------------------------------------------------------------------------
# Dagster
# ---------------------------------------------------------------------------
dagster:
	dagster dev -m $(DAGSTER_MODULE)

dagster-materialize: $(RMLMAPPER)
	dagster asset materialize -m $(DAGSTER_MODULE) --select '*'

# ---------------------------------------------------------------------------
# Download RMLMapper JAR (not committed to repo)
# ---------------------------------------------------------------------------
download-jar:
	@mkdir -p tools
	curl -sL -o $(RMLMAPPER) \
	  "https://github.com/RMLio/rmlmapper-java/releases/download/v$(RMLMAPPER_VERSION)/rmlmapper-$(RMLMAPPER_VERSION)-r380-all.jar"
	@echo "Downloaded $(RMLMAPPER)"

$(RMLMAPPER):
	@echo "RMLMapper JAR not found. Run: make download-jar"
	@exit 1

# ---------------------------------------------------------------------------
# Cleanup (preserves raw cache and tools)
# ---------------------------------------------------------------------------
clean:
	rm -f data/csv/*.csv data/rdf/*.ttl logs/*.log
	rm -rf .pytest_cache __pycache__ test/__pycache__
