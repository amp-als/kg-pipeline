"""
Unit tests for the ALS KP datasets RML mapping.

Tests run RMLMapper against the real data/csv/datasets.csv and assert
expected triples via SPARQL queries on the output.

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
MAPPING = ROOT / "mappings" / "rml" / "datasets.rml.ttl"
CSV = ROOT / "data" / "csv" / "datasets.csv"

ALSKP = "https://alskp.synapse.org/terms#"
BIOLINK = "https://w3id.org/biolink/vocab/"
SYNAPSE_BASE = "https://www.synapse.org/Synapse:"

# One known dataset to use as an anchor in tests. Rows can be retired from the
# portal, so tests anchored on it skip rather than fail once it disappears.
KNOWN_DATASET_ID = "syn67713129"
KNOWN_DATASET = f"{SYNAPSE_BASE}{KNOWN_DATASET_ID}"


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    """Run RMLMapper once per test session and load output into pyoxigraph."""
    if not CSV.exists():
        pytest.skip(f"CSV not found at {CSV}; run extraction first.")
    if not RMLMAPPER.exists():
        pytest.skip(f"RMLMapper JAR not found at {RMLMAPPER}.")

    out_ttl = tmp_path_factory.mktemp("rdf") / "datasets.ttl"
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
def known_dataset():
    """IRI of the anchor dataset; skip value-level tests if it has been retired."""
    if not CSV.exists():
        pytest.skip(f"CSV not found at {CSV}; run extraction first.")
    if not csv_has_id(CSV, KNOWN_DATASET_ID):
        pytest.skip(
            f"Anchor dataset {KNOWN_DATASET_ID} is no longer in {CSV.name}; "
            "choose a new KNOWN_DATASET_ID to restore value-level coverage."
        )
    return KNOWN_DATASET


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

def test_dataset_count(store):
    """Every CSV row is mapped to an alskp:Dataset instance."""
    expected = csv_subject_count(CSV)
    n = sparql_count(
        store,
        f"SELECT (COUNT(?s) AS ?n) WHERE {{ ?s a <{ALSKP}Dataset> }}"
    )
    assert n == expected, f"Expected {expected} datasets from {CSV.name}, got {n}"


def test_known_dataset_type(store, known_dataset):
    """Known dataset syn67713129 is typed as alskp:Dataset."""
    result = store.query(f"ASK {{ <{known_dataset}> a <{ALSKP}Dataset> }}")
    assert bool(result) is True


def test_biolink_dataset_type_materialized(store):
    """alskp:Dataset rdfs:subClassOf biolink:Dataset is asserted, not inferred.

    GraphDB runs without reasoning, so every alskp:Dataset must also carry an
    explicit biolink:Dataset type or BioLink-level queries return nothing.
    """
    missing = list(store.query(
        f"SELECT ?s WHERE {{ ?s a <{ALSKP}Dataset> . "
        f"FILTER NOT EXISTS {{ ?s a <{BIOLINK}Dataset> }} }}"
    ))
    assert not missing, (
        f"{len(missing)} alskp:Dataset instances lack an explicit biolink:Dataset type: "
        f"{[val(r, 's') for r in missing[:5]]}"
    )


def test_biolink_dataset_count_matches(store):
    """No spurious biolink:Dataset instances beyond the mapped portal datasets."""
    expected = csv_subject_count(CSV)
    n = sparql_count(
        store,
        f"SELECT (COUNT(?s) AS ?n) WHERE {{ ?s a <{BIOLINK}Dataset> }}"
    )
    assert n == expected, f"Expected {expected} biolink:Dataset instances, got {n}"


# ---------------------------------------------------------------------------
# IRI minting
# ---------------------------------------------------------------------------

def test_iri_pattern(store):
    """All dataset IRIs follow the Synapse: pattern."""
    results = list(store.query(
        f"SELECT ?s WHERE {{ ?s a <{ALSKP}Dataset> }}"
    ))
    for row in results:
        iri = val(row, "s")
        assert iri.startswith(SYNAPSE_BASE), f"Unexpected IRI: {iri}"
        synid = iri.replace(SYNAPSE_BASE, "")
        assert synid.startswith("syn"), f"Synapse ID should start with 'syn': {synid}"


# ---------------------------------------------------------------------------
# Multi-value splitting
# ---------------------------------------------------------------------------

def test_disease_multivalues(store, known_dataset):
    """Datasets with multiple diseases produce multiple alskp:disease triples."""
    n = sparql_count(
        store,
        f"""
        SELECT (COUNT(?d) AS ?n) WHERE {{
            <{known_dataset}> <{ALSKP}disease> ?d
        }}
        """
    )
    assert n >= 2, f"Expected ≥2 disease triples for syn67713129, got {n}"


def test_assay_multivalues(store, known_dataset):
    """syn67713129 has both BruChase-seq and Bru-seq assay triples."""
    results = list(store.query(
        f"""
        SELECT ?a WHERE {{
            <{known_dataset}> <{ALSKP}assay> ?a
        }}
        """
    ))
    assays = {val(r, "a") for r in results}
    assert "BruChase-seq" in assays, f"BruChase-seq not in {assays}"
    assert "Bru-seq" in assays, f"Bru-seq not in {assays}"


def test_keyword_multivalues(store, known_dataset):
    """Keywords are split into individual triples."""
    n = sparql_count(
        store,
        f"""
        SELECT (COUNT(?k) AS ?n) WHERE {{
            <{known_dataset}> <{ALSKP}keywords> ?k
        }}
        """
    )
    assert n > 1, "Keywords should be split into multiple triples"


# ---------------------------------------------------------------------------
# Literal properties
# ---------------------------------------------------------------------------

def test_name_property(store, known_dataset):
    """Dataset name literal is present."""
    results = list(store.query(
        f"""
        SELECT ?name WHERE {{
            <{known_dataset}> <{ALSKP}name> ?name
        }}
        """
    ))
    assert len(results) == 1
    assert "amyotrophic lateral sclerosis" in val(results[0], "name").lower()


def test_url_property(store, known_dataset):
    """URL is present and non-empty."""
    results = list(store.query(
        f"""
        SELECT ?url WHERE {{
            <{known_dataset}> <{ALSKP}url> ?url
        }}
        """
    ))
    assert len(results) == 1
    assert val(results[0], "url").startswith("http")


def test_participant_count_integer(store, known_dataset):
    """participantCount is typed as xsd:integer."""
    results = list(store.query(
        f"""
        SELECT ?count ?dt WHERE {{
            <{known_dataset}> <{ALSKP}participantCount> ?count .
            BIND(DATATYPE(?count) AS ?dt)
        }}
        """
    ))
    assert len(results) == 1
    assert "integer" in val(results[0], "dt")
    assert int(val(results[0], "count")) > 0


# ---------------------------------------------------------------------------
# Null handling (no empty-string triples)
# ---------------------------------------------------------------------------

def test_no_empty_string_triples(store):
    """No triples should have an empty string as their object."""
    n = sparql_count(
        store,
        'SELECT (COUNT(*) AS ?n) WHERE { ?s ?p "" }'
    )
    assert n == 0, f"Found {n} triples with empty-string objects"
