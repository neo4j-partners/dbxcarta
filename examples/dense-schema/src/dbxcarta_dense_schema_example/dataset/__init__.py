"""Host-only dataset stage: generate the synthetic candidate blueprint.

The step runs on a laptop with no Databricks credentials and no ``.env``: its
parameters come from CLI flags. It writes the committed candidate blueprint
(``blueprint/candidates_<count>.json``) that materialize and question generation
both consume.
"""
