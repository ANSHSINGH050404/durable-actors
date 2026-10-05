# CLI

`durable-actors` runs durable TypeScript and Python actors locally or in the cloud.

```sh
durable-actors [options] [command]
```

| Option          | Description  |
| --------------- | ------------ |
| `-h, --help`    | Show help    |
| `-V, --version` | Show version |

| Command    | Description                                                     |
| ---------- | --------------------------------------------------------------- |
| `init`     | Create a sample actor project                                   |
| `dev`      | Run local actors and reload code changes                        |
| `generate` | Generate actor clients from the running server or a source file |
| `observe`  | Open the local observability UI                                 |
| `start`    | Start the production control plane server                       |

Other settings are environment variables. See [Configuration](configuration.md).

## durable-actors init

Create a sample actor project.

```sh
durable-actors init <directory> [--template <name>]
```

| Argument    | Required | Description           |
| ----------- | -------- | --------------------- |
| `directory` | Yes      | Destination directory |

| Flag                | Default | Description                                                            |
| ------------------- | ------- | ---------------------------------------------------------------------- |
| `--template <name>` | `actor` | Project template: `actor`, `chat`, `ai-chat`, `documents`, or `python` |

```sh
durable-actors init my-project
durable-actors init my-project --template python
```

## durable-actors dev

Run local actors and reload code changes. Run it from the actor project directory. It loads `src/actors.ts`, or `src/actors.py` when only that file exists, so no configuration is required.

```sh
durable-actors dev [--port <number>] [--no-watch]
```

| Flag              | Default | Description                                                                                                      |
| ----------------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `--port <number>` | `7100`  | Localhost port. `0` selects a free port. Integer from 0 to 65535. `DURABLE_ACTORS_PORT` supplies the same value. |
| `--no-watch`      |         | Disable automatic code reload                                                                                    |

Optional `.env` overrides:

| Variable                    | Default                            | Description                                |
| --------------------------- | ---------------------------------- | ------------------------------------------ |
| `DURABLE_ACTORS_PROJECT`    | Current directory                  | Actor project directory                    |
| `DURABLE_ACTORS_ENTRYPOINT` | `src/actors.ts` or `src/actors.py` | Actor source file, relative to the project |
| `DURABLE_ACTORS_PYTHON`     | Project `.venv`                    | Python interpreter                         |

Python projects start from `durable-actors init my-project --template python`.

```sh
durable-actors dev
durable-actors dev --port 0
durable-actors dev --no-watch
```

## durable-actors generate

Generate actor clients from the running server, or from an explicit source file.

```sh
durable-actors generate [entrypoint] [--out-dir <directory>] [--language <language>] [--config <file>] [--control-plane-url <url>]
```

| Argument     | Required | Description                                                                   |
| ------------ | -------- | ----------------------------------------------------------------------------- |
| `entrypoint` | No       | Actor source file to compile instead of fetching the contract from the server |

| Flag                        | Default     | Description                                                         |
| --------------------------- | ----------- | ------------------------------------------------------------------- |
| `--out-dir <directory>`     | `generated` | Generated client directory                                          |
| `--language <language>`     | Inferred    | `typescript` or `python`. Inferred from the contract when omitted.  |
| `--config <file>`           |             | TypeScript configuration file. Local TypeScript source only.        |
| `--control-plane-url <url>` |             | Control-plane origin. Overrides `DURABLE_ACTORS_CONTROL_PLANE_URL`. |

`--config` requires a TypeScript entrypoint. `--control-plane-url` applies when the contract is fetched from the server.

Connection settings come from the environment or `.env`:

| Variable                           | Default                                                      |
| ---------------------------------- | ------------------------------------------------------------ |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost                                         |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                                      |
| `DURABLE_ACTORS_SECRET`            | Unset. Must match the server when authentication is enabled. |

```sh
durable-actors generate
durable-actors generate src/actors.ts --out-dir generated
durable-actors generate src/actors.py --language python
```

## durable-actors observe

Open the local observability UI. It uses the same connection settings as `generate`.

```sh
durable-actors observe [--no-open]
```

| Flag        | Description                                |
| ----------- | ------------------------------------------ |
| `--no-open` | Print the UI URL without opening a browser |

```sh
durable-actors observe
durable-actors observe --no-open
```

## durable-actors start

Start the production control plane server.

```sh
durable-actors start
```
