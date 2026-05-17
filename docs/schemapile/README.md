# SchemaPile Notes

This folder holds generated or example-specific SchemaPile documentation that
is useful outside the active proposal queue.

`questions-schema.md` is intentionally generated, not hand-maintained. To
refresh it after a SchemaPile ingest run:

```bash
uv run --directory examples/schemapile python scripts/dump_question_context.py \
    > docs/schemapile/questions-schema.md
```
