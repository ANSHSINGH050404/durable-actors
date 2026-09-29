# Python actors

Install Node.js 22+, Python 3.11+, and uv. Run `pnpm install`, `uv sync`, then `pnpm exec durable-actors dev`. The shared TypeScript CLI selects `.venv` and checks actor types before starting and reloading.

Edit `src/actors.py`. Write synchronous actor methods with ordinary `def`. Annotated fields persist by default; use `emitted()` for state broadcasts and `ephemeral()` for temporary values such as caches and service clients. From your client application, install `durable-actors[codegen]` and run `pnpm exec durable-actors generate` against the running server. Generated Python clients include synchronous typed methods and models. Import `actors` from the generated package and use `actors.Counter.get("one")`; the SDK manages the HTTP connection pool. Actors with emitted fields also expose `actor.subscribe(callback)` for typed live state; close the returned subscription when finished. The CLI runs strict mypy after generation.

Set `DURABLE_ACTORS_PYTHON` to select another interpreter. `DURABLE_ACTORS_BINARY` selects a locally built runtime.
