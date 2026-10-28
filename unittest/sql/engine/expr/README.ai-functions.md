# AI function validation

This runner validates the AI-function challenge only: AI_PROMPT, AI_COMPLETE,
AI_EMBED (single row and batching), Ollama endpoints, and AI_RERANK. It runs two
public mysqltest suites (ai_prompt and ai_function_enhancement), two added
regression suites (native JSON config and NULL dimension), and 31 additional
SQL/provider checks. The HTTP services
are local deterministic fixtures; no external model or API key is required.

## Requirements

- Linux or macOS, with a compiled seekdb executable. Use the repository's normal
  [build instructions](../../../../README.md).
- Python 3.9 or later with `PyMySQL` available in the active environment or on
  `PYTHONPATH`.
- A MySQL/MariaDB-compatible `mysqltest` executable. This is the test runner,
  a separate program from the interactive `mysql`/`obclient` client.
- Available local ports `8000` and `19125`, and writable `/tmp` storage.

## Run

From the repository root:

```sh
python3 -m venv work/ai-functions-venv
. work/ai-functions-venv/bin/activate
python3 -m pip install PyMySQL
python3 unittest/sql/engine/expr/run_ai_function_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest
```

If `mysqltest` is already on `PATH`, omit `--mysqltest`. Use `--repo` to select a
checkout explicitly and `--output-dir` to select a results directory. The runner
defaults to its containing checkout and `./work/ai-functions-validation`.

The runner creates disposable database instances with Unix sockets under `/tmp`,
runs the suites sequentially, and stops the processes it starts. Runtime data and
console logs remain available for diagnosing failures. All test and mock assets
come from the selected checkout.

## Results

A successful run exits with status `0` and produces:

- `ai-functions-ready-ai_prompt.log`
- `ai-functions-ready-ai_complete_json_config.log`
- `ai-functions-ready-ai_embed_null_dimension.log`
- `ai-functions-ready-ai_function_enhancement.log`
- `ai-functions-ready-provider.log` (expected summary: 31 passed, 0 failed)
- `ai-functions-ready-summary.json`, including the checkout revision (when Git metadata is available), executable
  SHA256, runtime directory, and each suite's exit status.

The runner removes only its own previous result files before each run. Public
suite success means the output matched its expected `.result` file. Official
competition scores still come from the organizer's evaluation.
