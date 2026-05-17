"""dbxcarta dense-schema Phase G example.

Pass `dbxcarta_dense_schema_example:preset` to the dbxcarta CLI:

    uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env
    uv run dbxcarta preset dbxcarta_dense_schema_example:preset --check-ready
    uv run dbxcarta preset dbxcarta_dense_schema_example:preset --upload-questions
"""

from dbxcarta_dense_schema_example.preset import (
    DenseSchemaPreset,
    preset,
)

__all__ = ["DenseSchemaPreset", "preset"]
