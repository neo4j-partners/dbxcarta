# Python Focus

Guidelines for writing clean, focused, idiomatic Python. Combines general coding discipline with PEP 8 style and Pydantic usage rules.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

---

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

---

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

---

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

---

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

---

## 5. PEP 8 Style

**Follow PEP 8. No exceptions for personal preference.**

Naming:
- `snake_case` for variables, functions, methods, modules
- `PascalCase` for classes
- `UPPER_SNAKE_CASE` for module-level constants
- Single leading underscore `_name` for internal/private; double `__name` only for name mangling (rare)

Formatting:
- 4-space indentation, no tabs
- Max line length 88 characters (Black-compatible; 79 is PEP 8 strict, but 88 is standard in practice)
- Two blank lines between top-level definitions; one blank line between methods
- Imports at the top: stdlib → third-party → local, each group separated by a blank line
- No wildcard imports (`from foo import *`)

Expressions:
- `is`/`is not` for `None` comparisons, not `==`
- `not x` instead of `x == False`
- Don't compare booleans to `True`/`False` with `==`
- Avoid mutable default arguments (`def f(x=[])` is a bug)

Type hints:
- Add type hints to all public function signatures
- Use `X | None` (Python 3.10+) over `Optional[X]`
- Use `list[X]`, `dict[K, V]`, `tuple[X, ...]` (lowercase, Python 3.9+) over `List`, `Dict`, `Tuple` from `typing`
- Annotate return types; use `-> None` explicitly when a function returns nothing meaningful

---

## 6. Pydantic

**Use Pydantic at system boundaries. Avoid it for internal-only data.**

### Use Pydantic when

Data crosses a trust boundary — it arrives from outside your codebase and could be malformed:
- Parsing API responses or webhook payloads
- Validating user input (HTTP request bodies, form data, CLI args)
- Loading configuration from env vars or config files (use `pydantic-settings`)
- Reading structured data from files (JSON, YAML, CSV rows)
- Defining request/response schemas for an API you're building

Also use Pydantic when:
- A data structure has non-trivial validation rules (field constraints, cross-field checks, coercion)
- You need serialization/deserialization alongside validation (`model_dump()`, `model_validate()`)
- You want a self-documenting schema that can generate a JSON Schema or OpenAPI spec

### Skip Pydantic when

Data stays inside your own codebase and is already well-typed:
- Internal intermediate objects constructed from already-validated values
- Simple groupings of a few scalars — use `dataclass` or just pass args
- Performance-critical hot paths where Pydantic's validation overhead matters
- One-off local variables that don't need a schema

**Rule of thumb:** if constructing the object can fail due to bad external data, use Pydantic. If it's built from values your own code controls, a `dataclass` is fine.

### Pydantic patterns

```python
from pydantic import BaseModel, Field, model_validator

class EventPayload(BaseModel):
    event_type: str
    user_id: int
    timestamp: float
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_event_type(self) -> "EventPayload":
        if not self.event_type:
            raise ValueError("event_type must not be empty")
        return self
```

- Prefer `Field(default_factory=list)` over `Field(default=[])` for mutable defaults
- Use `model_validate(data)` to parse dicts/JSON, not the constructor directly for untrusted data
- Use `model_dump()` / `model_dump_json()` for serialization
- Use `BaseSettings` from `pydantic-settings` for env-var config:

```python
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    api_key: str
    timeout_seconds: int = 30

    model_config = {"env_prefix": "APP_"}
```

### Dataclass vs Pydantic quick reference

**Default to `@dataclass`. Reach for Pydantic only when the scenario below says so.**

| Scenario | Use |
|---|---|
| API request/response body | Pydantic |
| Environment / file config | Pydantic (`BaseSettings`) |
| Parsing external JSON/YAML | Pydantic |
| Internal DTO between functions | `dataclass` |
| Simple named group of values | `dataclass` or `NamedTuple` |
| Needs JSON Schema / OpenAPI | Pydantic |
| Performance-critical inner loop | `dataclass` or plain tuple |

---

## 7. Exception Handling

**Catch specific exceptions. Never swallow errors silently.**

- Catch the narrowest exception type that applies (`ValueError`, `KeyError`, `IOError`), not `Exception` or bare `except:`
- Don't catch an exception only to `pass` — if you suppress it, log it or document why
- Use `raise NewError("msg") from original` when re-raising to preserve the chain; never `raise NewError("msg")` alone inside an `except` block
- Let unexpected exceptions propagate — only catch what you can meaningfully handle
- Use `finally` only for cleanup that must run regardless of outcome; prefer a context manager when one exists

```python
# bad
try:
    result = parse(data)
except Exception:
    pass

# good
try:
    result = parse(data)
except ValueError as e:
    raise RuntimeError(f"Invalid data format: {data!r}") from e
```

---

## 8. Python Idioms

**Prefer comprehensions for transforms. Use generators when you don't need the full list.**

Comprehensions:
- Use list/dict/set comprehensions for simple transforms — they're faster and more readable than an equivalent `for` loop that appends
- Keep comprehensions to one logical operation; if you need a nested condition and a transform, a loop is clearer
- Never use a comprehension purely for side effects — use a `for` loop

```python
# prefer
ids = [row.id for row in rows if row.active]

# not
ids = []
for row in rows:
    if row.active:
        ids.append(row.id)
```

Generators:
- Use a generator expression (`(x for x in ...)`) instead of a list comprehension when the result is consumed once and you don't need it in memory
- Prefer `any(...)` / `all(...)` over building a list just to check a condition

Misc idioms:
- Use f-strings for formatting, not `.format()` or `%`
- Unpack tuples explicitly: `a, b = pair` not `a = pair[0]; b = pair[1]`
- Use `enumerate` instead of manual index tracking; `zip` instead of index-based parallel iteration

---

## 9. Context Managers

**Use `with` for every resource that needs cleanup.**

- Always open files with `with open(...) as f:` — never call `.close()` manually
- Use `with` for locks, database connections, HTTP sessions, and any other resource with an `__exit__`
- Don't use `try/finally` for cleanup when a context manager already exists for that resource
- Write your own context manager (`@contextmanager` or `__enter__`/`__exit__`) when a resource pattern repeats more than once

```python
# bad
f = open("data.txt")
data = f.read()
f.close()

# good
with open("data.txt") as f:
    data = f.read()
```

---

**These guidelines are working if:** diffs are minimal and traceable, style is consistent, data validation happens at the edges, and exceptions surface rather than disappear.
