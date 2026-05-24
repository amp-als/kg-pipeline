"""
Cross-validation: every predicate used in RML mappings must be declared in the ontology.

Catches the class of bug where a property is mapped in RML but missing from
schema/ontology.ttl — this would produce valid RDF but undeclared triples that
break OWL reasoners and SHACL validators.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
ONTOLOGY = ROOT / "schema" / "ontology.ttl"
RML_DIR = ROOT / "mappings" / "rml"


def _extract_ontology_predicates(ttl_path: Path) -> set[str]:
    """Extract all property IRIs declared in the ontology."""
    text = ttl_path.read_text()
    prefixes = {}
    for m in re.finditer(r"@prefix\s+(\w+):\s+<([^>]+)>", text):
        prefixes[m.group(1)] = m.group(2)

    properties = set()
    # Match lines like:  alskp:foo\n    a owl:DatatypeProperty
    for m in re.finditer(r"^(\w+):(\S+)\s*$", text, re.MULTILINE):
        prefix, local = m.group(1), m.group(2)
        if prefix in prefixes:
            properties.add(f"{prefixes[prefix]}{local}")
    return properties


def _extract_rml_predicates(rml_dir: Path) -> dict[str, set[str]]:
    """Extract all predicates used in rr:predicate statements, per mapping file."""
    result = {}
    for rml_file in sorted(rml_dir.glob("*.rml.ttl")):
        text = rml_file.read_text()
        prefixes = {}
        for m in re.finditer(r"@prefix\s+(\w+):\s+<([^>]+)>", text):
            prefixes[m.group(1)] = m.group(2)

        # Namespaces that are RML/GREL plumbing, not portal predicates
        skip_ns = {
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",     # rdf:type
            "http://users.ugent.be/~bjdmeest/function/grel.ttl#",  # grel: function params
            "https://w3id.org/function/ontology#",              # fno: execution
        }

        predicates = set()
        for m in re.finditer(r"rr:predicate\s+(\w+):(\S+)", text):
            prefix, local = m.group(1), m.group(2)
            if prefix in prefixes:
                full_iri = f"{prefixes[prefix]}{local}"
                if not any(full_iri.startswith(ns) for ns in skip_ns):
                    predicates.add(full_iri)
        result[rml_file.name] = predicates
    return result


@pytest.fixture(scope="module")
def ontology_predicates():
    if not ONTOLOGY.exists():
        pytest.skip("Ontology not found")
    return _extract_ontology_predicates(ONTOLOGY)


@pytest.fixture(scope="module")
def rml_predicates():
    if not RML_DIR.exists():
        pytest.skip("RML directory not found")
    return _extract_rml_predicates(RML_DIR)


def test_all_rml_predicates_declared_in_ontology(ontology_predicates, rml_predicates):
    """Every predicate in RML mappings must have a declaration in ontology.ttl."""
    missing = {}
    for rml_file, predicates in rml_predicates.items():
        undeclared = predicates - ontology_predicates
        if undeclared:
            missing[rml_file] = undeclared

    if missing:
        lines = []
        for rml_file, undeclared in missing.items():
            for p in sorted(undeclared):
                lines.append(f"  {rml_file}: {p}")
        detail = "\n".join(lines)
        pytest.fail(
            f"RML predicates not declared in ontology:\n{detail}\n"
            f"Add these properties to schema/ontology.ttl"
        )
