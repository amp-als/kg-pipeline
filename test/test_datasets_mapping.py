"""
Unit tests for the ALS KP datasets RML mapping.

Tests run RMLMapper against the real data/csv/datasets.csv and assert
expected triples via SPARQL queries on the output.
"""

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
SYNAPSE_BASE = "https://www.synapse.org/Synapse:"

# One known dataset IRI to use as an anchor in tests
KNOWN_DATASET = f"{SYNAPSE_BASE}syn67713129"


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


def sparql_count(store, query: str) -> int:
    results = list(store.query(query))
    return int(results[0]["n"].value)


def val(row, key: str) -> str:
    """Extract string value from a SPARQL result term."""
    return row[key].value


# ---------------------------------------------------------------------------
# Triple count / class assertions
# ---------------------------------------------------------------------------

def test_dataset_count(store):
    """All 25 datasets are mapped as alskp:Dataset instances."""
    n = sparql_count(
        store,
        f"SELECT (COUNT(?s) AS ?n) WHERE {{ ?s a <{ALSKP}Dataset> }}"
    )
    assert n == 25, f"Expected 25 datasets, got {n}"


def test_known_dataset_type(store):
    """Known dataset syn67713129 is typed as alskp:Dataset."""
    result = store.query(f"ASK {{ <{KNOWN_DATASET}> a <{ALSKP}Dataset> }}")
    assert bool(result) is True


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

def test_disease_multivalues(store):
    """Datasets with multiple diseases produce multiple alskp:disease triples."""
    n = sparql_count(
        store,
        f"""
        SELECT (COUNT(?d) AS ?n) WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}disease> ?d
        }}
        """
    )
    assert n >= 2, f"Expected ≥2 disease triples for syn67713129, got {n}"


def test_assay_multivalues(store):
    """syn67713129 has both BruChase-seq and Bru-seq assay triples."""
    results = list(store.query(
        f"""
        SELECT ?a WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}assay> ?a
        }}
        """
    ))
    assays = {val(r, "a") for r in results}
    assert "BruChase-seq" in assays, f"BruChase-seq not in {assays}"
    assert "Bru-seq" in assays, f"Bru-seq not in {assays}"


def test_keyword_multivalues(store):
    """Keywords are split into individual triples."""
    n = sparql_count(
        store,
        f"""
        SELECT (COUNT(?k) AS ?n) WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}keywords> ?k
        }}
        """
    )
    assert n > 1, "Keywords should be split into multiple triples"


# ---------------------------------------------------------------------------
# Literal properties
# ---------------------------------------------------------------------------

def test_name_property(store):
    """Dataset name literal is present."""
    results = list(store.query(
        f"""
        SELECT ?name WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}name> ?name
        }}
        """
    ))
    assert len(results) == 1
    assert "amyotrophic lateral sclerosis" in val(results[0], "name").lower()


def test_url_property(store):
    """URL is present and non-empty."""
    results = list(store.query(
        f"""
        SELECT ?url WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}url> ?url
        }}
        """
    ))
    assert len(results) == 1
    assert val(results[0], "url").startswith("http")


def test_participant_count_integer(store):
    """participantCount is typed as xsd:integer."""
    results = list(store.query(
        f"""
        SELECT ?count ?dt WHERE {{
            <{KNOWN_DATASET}> <{ALSKP}participantCount> ?count .
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
