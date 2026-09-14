"""
Unit tests for the ALS KP files RML mapping.

Tests run RMLMapper against the real data/csv/files.csv and assert
expected triples via SPARQL queries on the output.

Note: files.csv has several thousand rows; RMLMapper takes ~60s on full data.
The module-scoped fixture runs once per test session.

Row counts come from the live Synapse portal and change as data is added or
retired, so count assertions are derived from the CSV rather than hard-coded.
"""

import csv
import subprocess
from pathlib import Path

import pyoxigraph
import pytest

ROOT = Path(__file__).parent.parent
RMLMAPPER = ROOT / "tools" / "rmlmapper-8.1.0.jar"
FUNCTIONS_GREL = ROOT / "tools" / "functions_grel.ttl"
GREL_MAPPING = ROOT / "tools" / "grel_java_mapping.ttl"
MAPPING = ROOT / "mappings" / "rml" / "files.rml.ttl"
CSV = ROOT / "data" / "csv" / "files.csv"

ALSKP = "https://alskp.synapse.org/terms#"
SYNAPSE_BASE = "https://www.synapse.org/Synapse:"

# A known file from the first row of the CSV. Rows can be retired from the
# portal, so tests anchored on it skip rather than fail once it disappears.
KNOWN_FILE_ID = "syn68724262"
KNOWN_FILE = f"{SYNAPSE_BASE}{KNOWN_FILE_ID}"


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    """Run RMLMapper once per test session and load output into pyoxigraph."""
    if not CSV.exists():
        pytest.skip(f"CSV not found at {CSV}; run extraction first.")
    if not RMLMAPPER.exists():
        pytest.skip(f"RMLMapper JAR not found at {RMLMAPPER}.")

    out_ttl = tmp_path_factory.mktemp("rdf") / "files.ttl"
    subprocess.run(
        [
            "java", "-jar", str(RMLMAPPER),
            "-f", str(FUNCTIONS_GREL),
            "-f", str(GREL_MAPPING),
            "-m", str(MAPPING),
            "-o", str(out_ttl),
        ],
        check=True,
        capture_output=True,
        cwd=str(ROOT),
    )
    st = pyoxigraph.Store()
    st.load(out_ttl.open("rb"), format=pyoxigraph.RdfFormat.TURTLE)
    return st


@pytest.fixture(scope="module")
def known_file():
    """IRI of the anchor file; skip value-level tests if it has been retired."""
    if not CSV.exists():
        pytest.skip(f"CSV not found at {CSV}; run extraction first.")
    if not csv_has_id(CSV, KNOWN_FILE_ID):
        pytest.skip(
            f"Anchor file {KNOWN_FILE_ID} is no longer in {CSV.name}; "
            "choose a new KNOWN_FILE_ID to restore value-level coverage."
        )
    return KNOWN_FILE


def sparql_count(store, query: str) -> int:
    results = list(store.query(query))
    return int(results[0]["n"].value)


def val(row, key: str) -> str:
    """Extract string value from a SPARQL result term."""
    return row[key].value


def csv_subject_count(path: Path) -> int:
    """Number of distinct `id` values in the CSV — one subject IRI per id."""
    with path.open(newline="", encoding="utf-8") as fh:
        ids = {row["id"] for row in csv.DictReader(fh) if row["id"]}
    assert ids, f"{path.name} has no rows; extraction likely failed."
    return len(ids)


def csv_has_id(path: Path, synid: str) -> bool:
    """Whether the anchor row is still present in the extracted CSV."""
    with path.open(newline="", encoding="utf-8") as fh:
        return any(row["id"] == synid for row in csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Triple count / class assertions
# ---------------------------------------------------------------------------

def test_file_count(store):
    """Every CSV row is mapped to an alskp:PortalFile instance."""
    expected = csv_subject_count(CSV)
    n = sparql_count(
        store,
        f"SELECT (COUNT(?s) AS ?n) WHERE {{ ?s a <{ALSKP}PortalFile> }}"
    )
    assert n == expected, f"Expected {expected} files from {CSV.name}, got {n}"


def test_known_file_type(store, known_file):
    """Known file syn68724262 is typed as alskp:PortalFile."""
    result = store.query(f"ASK {{ <{known_file}> a <{ALSKP}PortalFile> }}")
    assert bool(result) is True


# ---------------------------------------------------------------------------
# IRI minting
# ---------------------------------------------------------------------------

def test_iri_pattern_sample(store):
    """Spot-check that file IRIs follow the Synapse: pattern (sample 10)."""
    results = list(store.query(
        f"SELECT ?s WHERE {{ ?s a <{ALSKP}PortalFile> }} LIMIT 10"
    ))
    for row in results:
        iri = val(row, "s")
        assert iri.startswith(SYNAPSE_BASE), f"Unexpected IRI: {iri}"


# ---------------------------------------------------------------------------
# Literal properties
# ---------------------------------------------------------------------------

def test_known_file_name(store, known_file):
    """Known file has the expected name literal."""
    results = list(store.query(
        f"SELECT ?name WHERE {{ <{known_file}> <{ALSKP}name> ?name }}"
    ))
    assert len(results) == 1
    assert "fastq" in val(results[0], "name").lower()


def test_assay_literal(store, known_file):
    """Known file has assay = RNA-seq."""
    results = list(store.query(
        f"SELECT ?a WHERE {{ <{known_file}> <{ALSKP}assay> ?a }}"
    ))
    assert len(results) == 1
    assert val(results[0], "a") == "RNA-seq"


def test_species_present(store):
    """Files with species data have the alskp:species triple."""
    n = sparql_count(
        store,
        f"SELECT (COUNT(?s) AS ?n) WHERE {{ ?s <{ALSKP}species> ?sp }}"
    )
    assert n > 0


# ---------------------------------------------------------------------------
# Multi-value: sex (pipe-delimited)
# ---------------------------------------------------------------------------

def test_sex_split(store, known_file):
    """Known file has sex = Female as a single split value."""
    results = list(store.query(
        f"SELECT ?sex WHERE {{ <{known_file}> <{ALSKP}sex> ?sex }}"
    ))
    values = {val(r, "sex") for r in results}
    assert "Female" in values


def test_contributor_split(store, known_file):
    """Known file has at least one alskp:contributor triple."""
    n = sparql_count(
        store,
        f"SELECT (COUNT(?c) AS ?n) WHERE {{ <{known_file}> <{ALSKP}contributor> ?c }}"
    )
    assert n >= 1


# ---------------------------------------------------------------------------
# Numeric property
# ---------------------------------------------------------------------------

def test_total_reads_integer(store, known_file):
    """totalReads is typed as xsd:integer for files that have it."""
    results = list(store.query(
        f"""
        SELECT ?n ?dt WHERE {{
            <{known_file}> <{ALSKP}totalReads> ?n .
            BIND(DATATYPE(?n) AS ?dt)
        }}
        """
    ))
    assert len(results) == 1
    assert "integer" in val(results[0], "dt")
    assert int(val(results[0], "n")) > 0


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

def test_no_empty_string_triples(store):
    """No triples should have an empty string as their object."""
    n = sparql_count(
        store,
        'SELECT (COUNT(*) AS ?n) WHERE { ?s ?p "" }'
    )
    assert n == 0, f"Found {n} triples with empty-string objects"


# ---------------------------------------------------------------------------
# External accession IDs
# ---------------------------------------------------------------------------

def test_biosample_id_present(store, known_file):
    """Known file has a BioSample accession."""
    results = list(store.query(
        f"SELECT ?id WHERE {{ <{known_file}> <{ALSKP}bioSampleId> ?id }}"
    ))
    assert len(results) == 1
    assert val(results[0], "id").startswith("SAM")


def test_srr_id_present(store, known_file):
    """Known file has an SRR accession."""
    results = list(store.query(
        f"SELECT ?id WHERE {{ <{known_file}> <{ALSKP}srrId> ?id }}"
    ))
    assert len(results) == 1
    assert val(results[0], "id").startswith("SRR")
