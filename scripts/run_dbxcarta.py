from _cluster_bootstrap import inject_params

inject_params()

from dbxcarta.ingest import run_dbxcarta

run_dbxcarta()
