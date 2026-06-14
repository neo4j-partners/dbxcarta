"""Host-only dataset stage: produce the slice and the candidate blueprint.

Both steps run on a laptop with no Databricks credentials. The slice runner
shells out to upstream ``slice.py``; the candidate selector filters and ranks
that slice into the committed candidate blueprint that materialize and question
generation both consume.
"""
