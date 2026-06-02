# dbxcarta developer Makefile.
#
# The e2e-* targets run the full Databricks pipeline for one example end to
# end: rebuild+ship the wheels from current source, then submit ingest, then
# the client evaluation. Use them to test changes to the dbxcarta packages.
#
# Run from the repo root: `make e2e-finance-genie`.
#
# Each target points DBXCARTA_ENV_FILE at that example's committed dbxcarta
# *overlay* (dbxcarta-overlay.env), NOT the example's standalone ./.env. The
# overlay carries the dbxcarta-scoped config (catalogs, schemas, volume,
# flags) and layers over the shared base .env at the repo root. The standalone
# ./.env is only for the local read-only demo and never reaches this pipeline.
# The path is set inline on every command, so `make` works from the repo root
# with no `export` and no per-shell setup.

.PHONY: help \
	e2e-finance-genie e2e-schemapile e2e-dense-schema

# The three-step iterate loop for one example. $(1) is the overlay path.
# publish-wheels rebuilds the wheels from current source and already uploads
# the bootstrap script, so no separate `upload --all` is needed.
define e2e_pipeline
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit publish-wheels
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit submit-entrypoint ingest
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit submit-entrypoint client
endef

help:
	@echo "dbxcarta e2e pipeline targets (run from repo root):"
	@echo "  make e2e-finance-genie   Rebuild wheels, then ingest + client for finance-genie"
	@echo "  make e2e-schemapile      Rebuild wheels, then ingest + client for schemapile"
	@echo "  make e2e-dense-schema    Rebuild wheels, then ingest + client for dense-schema"
	@echo ""
	@echo "Each rebuilds wheels from current source, so it reflects local changes"
	@echo "to the dbxcarta packages. Prerequisites (one-time: install the example"
	@echo "preset, secrets, questions, upstream UC tables) are in each example README."

e2e-finance-genie:
	$(call e2e_pipeline,examples/finance-genie/dbxcarta-overlay.env)

e2e-schemapile:
	$(call e2e_pipeline,examples/schemapile/dbxcarta-overlay.env)

e2e-dense-schema:
	$(call e2e_pipeline,examples/dense-schema/dbxcarta-overlay.local.env)
