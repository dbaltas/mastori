# Benchmark datasets

Deterministic dataset samples for Mastori benchmark workloads.

The raw questions are **not committed** to this repository. A pinned manifest
(`datasets.yaml`) defines the fetch recipe, and the `mastori-datasets` CLI
downloads the samples into a local cache at run time.

## Manifest: `datasets.yaml`

Each entry pins:

| Field | Meaning |
|---|---|
| `source` | Hugging Face dataset id |
| `config` / `split` | HF config and split to read |
| `revision` | Pinned dataset commit, documenting the exact state the sample came from |
| `extract` | Row extraction mode (`mmlu`, `first_human_turn`) |
| `selection` | `start` offset and `count` of rows |
| `validate` | Minimum rows, required fields, and content spot checks |

Current datasets:

- **mmlu-short** — `cais/mmlu`: question + 4 choices (~50 tokens median), single-letter answer. Short-prompt workloads and quality benchmarking.
- **sharegpt-long** — `Aeala/ShareGPT_Vicuna_unfiltered`: first human turn as prompt plus the real assistant response stored as `reference_output` (p50 ~390 tokens). Medium/long workloads.

## Running

From the repository root:

```sh
# download all datasets defined in the manifest
go run ./cmd/datasets fetch

# download a single dataset
go run ./cmd/datasets fetch --dataset mmlu-short

# verify a cache: row count, required fields, content spot checks
go run ./cmd/datasets validate
```

Options:

- `--manifest <path>` — manifest to use (default `benchmark/workloads/datasets.yaml`)
- `--cache <dir>` — cache directory (default `benchmark/workloads/cache`)
- `--dataset <name>` — operate on one dataset only

A successful `validate` prints a `PASS` line per check; failures exit non-zero.

## Cache

Downloaded samples are JSONL files in `benchmark/workloads/cache/` (gitignored):

```json
{"index":0,"data":{"subject":"abstract_algebra","question":"...","choices":["4","8"],"answer":0}}
```

Each line of `data` is a ready-to-serve prompt for the traffic generator. Safe
to delete: re-running `fetch` rebuilds them deterministically.

## Gated datasets

Some HF datasets (e.g. LMSYS-Chat-1M) require authentication. Export a token
before fetching:

```sh
export HF_TOKEN=hf_...
```

## Determinism notes

- Rows come from the HF datasets-server `/rows` API with offset paging, so a
  given manifest produces the same sample as long as the upstream dataset
  revision does not change. The `revision` field records the state the sample
  was generated from; `validate` fails loudly if the cache does not match the
  manifest's expectations.
- The datasets-server throttles bursts with 5xx responses; `fetch` retries
  with exponential backoff.
