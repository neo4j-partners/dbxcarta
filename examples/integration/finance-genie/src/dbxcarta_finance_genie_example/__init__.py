"""Reference dbxcarta preset for the Finance Genie Lakehouse example.

Pass `dbxcarta_finance_genie_example:preset` to the dbxcarta CLI:

    uv run dbxcarta preset dbxcarta_finance_genie_example:preset --print-env
    uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional
    uv run dbxcarta preset dbxcarta_finance_genie_example:preset --upload-questions
"""

from dbxcarta_finance_genie_example.finance_genie import preset

__all__ = ["preset"]
