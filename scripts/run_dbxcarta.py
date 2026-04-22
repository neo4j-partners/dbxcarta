import os

from _cluster_bootstrap import inject_params

inject_params()

job = os.environ.get("DBXCARTA_JOB", "schema")

if job == "schema":
    from dbxcarta import run_schema
    run_schema()
elif job == "sample":
    from dbxcarta import run_sample
    run_sample()
elif job == "embeddings":
    from dbxcarta import run_embeddings
    run_embeddings()
else:
    raise ValueError(f"Unknown DBXCARTA_JOB: {job!r}. Expected: schema | sample | embeddings")
