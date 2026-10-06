# Contributing to Durable Actors

We welcome bug reports, documentation improvements, examples, and code contributions. For a larger change, open an [issue](https://github.com/TerseAI/durable-actors/issues) to discuss the approach before starting. Small fixes can go straight to a pull request.

Please follow our [code of conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately through [SECURITY.md](SECURITY.md).

## Set up the repository

To use Durable Actors in your own application, start with the [language quickstarts](README.md#languages). To develop this repository, install:

- Node.js 22.19+ and pnpm 10.17.1 (the pinned workspace version).
- Bun 1.3.9+; CI exercises both 1.3.9 and 1.4.2.
- Rust 1.89+ with Cargo and rustfmt, and a native C/C++ build toolchain.
- Helm 3 for changes to the Kubernetes chart.
- Python 3.11+ and uv for changes to `sdk-python`.
- PostgreSQL 16 for database tests. Docker is an optional way to run it.

Fork the repository, then clone your fork and install the workspace dependencies:

```sh
git clone https://github.com/YOUR_USERNAME/durable-actors.git
cd durable-actors
pnpm install --frozen-lockfile
pnpm --dir sdk build
cargo build --locked
```

Repository builds set the pinned Google SDK’s Rapid append opt-in in `.cargo/config.toml`. When building outside the checkout (including `cargo install`), set `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`.

The SDK build includes the observer UI and actor template. A full native bundle can be built with `pnpm build`.

## Find your way around

| Directory               | Contents                                            |
| ----------------------- | --------------------------------------------------- |
| `src/`                  | Rust control plane, host, storage, and runtime      |
| `sdk-python/`           | Python actors, executor, and generated clients |
| `sdk/`                  | TypeScript SDK, compiler, generated client, and CLI |
| `packages/observer-ui/` | Actor observability UI                              |
| `charts/durable-actors/`          | GKE hosting with Standard GCS and optional Rapid          |
| `tests/`                | Rust tests and repository script tests              |
| `examples/`             | Chat, AI chat, and collaborative documents          |
| `docs/`                 | API and configuration references                    |

## Make a change

Keep changes focused and follow the [engineering conventions](AGENTS.md). For behavior changes, add a failing test first, implement the change, then refactor. Put tests and fixtures in the relevant project's `tests/` directory; CLI tests belong in `sdk/tests/cli/`. Rust unit tests use `#[path]` declarations to retain private access.

Update documentation and examples when changing a public API, configuration, or CLI behavior.

## Run checks

From the repository root:

```sh
pnpm format:check
pnpm test
pnpm docs:check
```

`pnpm test` covers the observer UI, repository scripts, Rust tests, and SDK. PostgreSQL tests skip their database work unless `DURABLE_ACTORS_TEST_POSTGRES_URL` is set. Use a dedicated test database; the suite creates and removes test schemas. For example, with Docker:

```sh
docker run --name durable-actors-test-postgres --rm -d \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=durable_actors_test \
  -p 127.0.0.1:5432:5432 postgres:16
docker exec durable-actors-test-postgres pg_isready -U postgres -d durable_actors_test
export DURABLE_ACTORS_TEST_POSTGRES_URL='postgresql://postgres:postgres@127.0.0.1:5432/durable_actors_test?sslmode=disable'
pnpm test
```

Wait until `pg_isready` reports that PostgreSQL is accepting connections before running the tests. Stop the disposable database with `docker stop durable-actors-test-postgres` when finished.

For runtime and integration changes, build the SDK and runtime, then run the opt-in tests with Bun on your PATH:

```sh
pnpm --dir sdk build
cargo build --locked
cargo test --locked -- --ignored
```

For Kubernetes deployment changes:

```sh
helm lint charts/durable-actors -f charts/durable-actors/tests/values.yaml
node --test charts/durable-actors/tests/chart.test.mjs
```

For Python SDK changes, build the runtime and run from `sdk-python`:

```sh
uv sync --locked --all-extras
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src/durable_actors
uv run pyright
DURABLE_ACTORS_TEST_RUNTIME="$(cd .. && pwd)/target/debug/durable-actors" uv run pytest -q
uv build --no-sources
```

Python integration tests require `DURABLE_ACTORS_TEST_RUNTIME`; they skip without it. To run the shared CLI's Python tests from the repository root after building the SDK:

```sh
pnpm --dir sdk exec tsc -p tsconfig.test.json
DURABLE_ACTORS_TEST_PYTHON="$PWD/sdk-python/.venv/bin/python" DURABLE_ACTORS_TEST_RUNTIME="$PWD/target/debug/durable-actors" node --test sdk/.test-dist/tests/cli/python.test.js
```
The release workflow requires a [PyPI trusted publisher](https://docs.pypi.org/trusted-publishers/adding-a-publisher/) for package `durable-actors`: owner `TerseAI`, repository `durable-actors`, workflow `release.yml`, environment `pypi`.

For SDK packaging or documentation changes, run the relevant checks:

```sh
pnpm --dir sdk package:check
pnpm --dir packages/observer-ui package:check
pnpm docs:build
```

The [CI workflow](.github/workflows/ci.yml) is the source of truth for toolchain versions and the full validation matrix.

## Open a pull request

Describe the problem, the resulting behavior, and how you verified it. Link related issues and include screenshots for UI changes. Note any checks you could not run. Maintainers will review the change and arrange releases through the existing [release workflow](.github/workflows/release.yml).

Contributions are made under the repository's [MIT license](LICENSE.md).

Published chart packages include the runtime image digest from the same release. The release job uploads the chart to GitHub Releases and `ghcr.io/terseai/charts/durable-actors`. Configure that GHCR package for public access when first published.
