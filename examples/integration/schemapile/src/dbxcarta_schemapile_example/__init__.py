"""dbxcarta SchemaPile example.

Pass `dbxcarta_schemapile_example:preset` to the dbxcarta CLI:

    uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env
    uv run dbxcarta preset dbxcarta_schemapile_example:preset --check-ready
    uv run dbxcarta preset dbxcarta_schemapile_example:preset --upload-questions
"""

from dbxcarta_schemapile_example.preset import (
    SchemaPilePreset,
    preset,
)

__all__ = ["SchemaPilePreset", "preset"]
