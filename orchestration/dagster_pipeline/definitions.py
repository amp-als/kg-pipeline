"""Dagster Definitions for the ALS KP KG pipeline.

To launch the Dagster UI:
    dagster dev -m orchestration.dagster_pipeline.definitions

To materialize all assets from CLI:
    dagster asset materialize -m orchestration.dagster_pipeline.definitions --select '*'
"""

from dagster import Definitions

from orchestration.dagster_pipeline.assets import portal_assets
from orchestration.dagster_pipeline.resources import SynapseResource, RMLMapperResource

defs = Definitions(
    assets=portal_assets,
    resources={
        "synapse": SynapseResource(),
        "rml_mapper": RMLMapperResource(),
    },
)
