"""Dagster Definitions for the ALS KP KG pipeline.

To launch the Dagster UI:
    dagster dev -f orchestration/dagster_pipeline/definitions.py

To materialize all assets from CLI:
    dagster asset materialize -f orchestration/dagster_pipeline/definitions.py --select '*'
"""

from dagster import Definitions

from orchestration.dagster_pipeline.assets import csv_files, csv_datasets, fk_validation, rdf_files, rdf_datasets
from orchestration.dagster_pipeline.resources import SynapseResource, RMLMapperResource

defs = Definitions(
    assets=[
        csv_files,
        csv_datasets,
        fk_validation,
        rdf_files,
        rdf_datasets,
    ],
    resources={
        "synapse": SynapseResource(),
        "rml_mapper": RMLMapperResource(),
    },
)
