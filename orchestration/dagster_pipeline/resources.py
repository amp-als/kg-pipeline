"""Dagster resources for the ALS KP KG pipeline."""

import subprocess
from pathlib import Path
from typing import List, Optional

import synapseclient
import warnings
from dagster import ConfigurableResource, InitResourceContext

PROJECT_ROOT = Path(__file__).parent.parent.parent


class SynapseResource(ConfigurableResource):
    """Anonymous Synapse client for public portal metadata."""

    def setup_for_execution(self, context: InitResourceContext) -> None:
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        # Do NOT call login() — portal Layer 1 metadata is public.
        self._client = synapseclient.Synapse()

    @property
    def client(self) -> synapseclient.Synapse:
        return self._client


class RMLMapperResource(ConfigurableResource):
    """Runs RMLMapper to produce RDF from CSV mappings."""

    jar_path: str = "tools/rmlmapper-8.1.0.jar"
    functions_grel: str = "tools/functions_grel.ttl"
    grel_java_mapping: str = "tools/grel_java_mapping.ttl"
    java_max_heap: str = "4g"

    def run(self, mapping_file: str, output_file: str, log_file: Optional[str] = None) -> None:
        """Run RMLMapper with the given mapping, writing output to output_file.

        Runs from PROJECT_ROOT so that relative paths in RML files resolve correctly.
        Stderr (RMLMapper logs) is written to log_file if provided, otherwise suppressed.
        """
        abs_output = PROJECT_ROOT / output_file
        abs_output.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "java",
            f"-Xmx{self.java_max_heap}",
            "-jar", self.jar_path,
            "-f", self.functions_grel,
            "-f", self.grel_java_mapping,
            "-m", mapping_file,
            "-o", output_file,
        ]

        if log_file:
            abs_log = PROJECT_ROOT / log_file
            abs_log.parent.mkdir(parents=True, exist_ok=True)
            stderr_dest = open(abs_log, "w")
        else:
            stderr_dest = subprocess.DEVNULL

        try:
            subprocess.run(
                cmd,
                check=True,
                stderr=stderr_dest,
                stdout=subprocess.DEVNULL,
                cwd=str(PROJECT_ROOT),
            )
        finally:
            if log_file and stderr_dest is not subprocess.DEVNULL:
                stderr_dest.close()

        if not abs_output.exists() or abs_output.stat().st_size == 0:
            raise RuntimeError(f"RMLMapper produced no output at {output_file}")
