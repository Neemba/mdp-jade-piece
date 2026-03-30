"""External Assets from other MDP projects.

This module defines external assets from mdp-params project to maintain
lineage visibility across projects in Dagster.

In production (Kubernetes), these are resolved via the workspace.yaml ConfigMap
which has both mdp-params and mdp-company as separate gRPC code locations.

In local dev, without workspace.yaml, these appear as unresolved dependencies
(gray boxes in UI) but don't break functionality.
"""

from dagster import SourceAsset


# External asset from mdp-service: 
#external_silver_invoice_credit_note = SourceAsset(
#    key="silver_invoice_credit_note",
#    description="Silver layer  information from mdp-core",
#    group_name="core_silver",
#    metadata={
#        "source_project": "mdp-core",
#        "source_location": "mdp-core",
#        "schema": "core",
#        "table": "B_SILVER_INVOICE_CREDIT_NOTE",
#        "layer": "silver",
#    },
#)