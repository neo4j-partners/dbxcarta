# dbxcarta developer Makefile.
#
# The e2e-* targets run the Databricks pipeline for one example in two phases:
# the *-ingest target rebuilds+ships the wheels from current source then submits
# ingest; the *-client target submits the client evaluation. Run ingest, let it
# finish, then run client. Use them to test changes to the dbxcarta packages.
#
# Run from the repo root: `make e2e-finance-genie-ingest`.
#
# Each target points DBXCARTA_ENV_FILE at that example's committed dbxcarta
# *overlay* (dbxcarta-overlay.env), NOT the example's standalone ./.env. The
# overlay carries the dbxcarta-scoped config (catalogs, schemas, volume,
# flags) and layers over the shared base .env at the repo root. The standalone
# ./.env is only for the local read-only demo and never reaches this pipeline.
# The path is set inline on every command, so `make` works from the repo root
# with no `export` and no per-shell setup.

.PHONY: help \
	e2e-finance-genie-ingest e2e-finance-genie-client \
	e2e-schemapile-ingest e2e-schemapile-client \
	e2e-dense-schema-ingest e2e-dense-schema-client

# Phase 1: rebuild+ship wheels from current source, then submit ingest. $(1) is
# the overlay path. publish-wheels rebuilds the wheels and already uploads the
# bootstrap script, so no separate `upload --all` is needed.
define e2e_ingest
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit publish-wheels
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit submit-entrypoint ingest
endef

# Phase 2: submit the client evaluation. $(1) is the overlay path.
define e2e_client
	DBXCARTA_ENV_FILE=$(1) uv run dbxcarta-submit submit-entrypoint client
endef

help:
	@echo "dbxcarta e2e pipeline targets (run from repo root, ingest then client):"
	@echo "  make e2e-finance-genie-ingest   Rebuild wheels, then ingest for finance-genie"
	@echo "  make e2e-finance-genie-client   Client evaluation for finance-genie"
	@echo "  make e2e-schemapile-ingest      Rebuild wheels, then ingest for schemapile"
	@echo "  make e2e-schemapile-client      Client evaluation for schemapile"
	@echo "  make e2e-dense-schema-ingest    Rebuild wheels, then ingest for dense-schema"
	@echo "  make e2e-dense-schema-client    Client evaluation for dense-schema"
	@echo ""
	@echo "The *-ingest targets rebuild wheels from current source, so they reflect"
	@echo "local changes to the dbxcarta packages. Prerequisites (one-time: install"
	@echo "the example preset, secrets, questions, upstream UC tables) are in each"
	@echo "example README."

e2e-finance-genie-ingest:
	$(call e2e_ingest,examples/finance-genie/dbxcarta-overlay.env)

e2e-finance-genie-client:
	$(call e2e_client,examples/finance-genie/dbxcarta-overlay.env)

e2e-schemapile-ingest:
	$(call e2e_ingest,examples/schemapile/dbxcarta-overlay.env)

e2e-schemapile-client:
	$(call e2e_client,examples/schemapile/dbxcarta-overlay.env)

e2e-dense-schema-ingest:
	$(call e2e_ingest,examples/dense-schema/dbxcarta-overlay.env)

e2e-dense-schema-client:
	$(call e2e_client,examples/dense-schema/dbxcarta-overlay.env)
