#!/usr/bin/env python3
"""Generate the story tables for an experiment dir from its own artifacts.

Review 2026-11-24 (05 §6, Analysis): "Story numbers generated from the
summaries by script, never copied by hand." This reads:

  benchmark/results/<experiment>/order.txt          (protocol execution order)
  benchmark/results/<experiment>/runs/*.summary.json
  benchmark/results/<experiment>/runs/*.jsonl       (per-request rows)
  benchmark/results/<experiment>/metrics/prometheus-*.json (15s series)

and prints the client-side and engine-side tables used in story.md, plus the
gates (smoke, worker dispatch) and the session-level cross-checks.

Usage: python3 scripts/story-numbers.py <experiment-dir> [<experiment-dir> ...]
Paths are relative to the repo root (or absolute). The runs/ dir may be empty
if run files still live in results/cluster/ — pass those dirs as extra args.
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone


def parse_ts(s):
    """RFC3339 with nanoseconds -> epoch seconds."""
    base, frac = s[:26], s[26:]
    dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return dt.timestamp() + float(frac.rstrip("Z")) / 1e9


def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    k = min(len(sorted_vals) - 1, max(0, int(round(p / 100 * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def order_entries(exp_dir):
    entries = []
    path = os.path.join(exp_dir, "order.txt")
    if not os.path.exists(path):
        return entries
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = dict(p.split("=", 1) for p in line.split() if "=" in p)
        entries.append((int(parts["rate"]), int(parts["rep"]), parts["run_id"]))
    return entries


def load_runs(run_dirs):
    summaries = {}
    for d in run_dirs:
        for path in sorted(glob.glob(os.path.join(d, "*.summary.json"))):
            s = json.load(open(path))
            summaries[s["run_id"]] = (s, path[: -len(".summary.json")] + ".jsonl")
    return summaries


def client_table(run_id, summary, jsonl_path, duration_s):
    rows = [json.loads(l) for l in open(jsonl_path)]
    measured = [r for r in rows if not r.get("warmup")]
    ok = [r for r in measured if not r.get("error")]
    bad = [r for r in measured if r.get("error")]

    ok_lat = sorted(r["latency_ms"] for r in ok if r.get("latency_ms") is not None)
    ok_ttft = sorted(r["ttft_ms"] for r in ok if r.get("ttft_ms") is not None)
    bad_ttft = sorted(r["ttft_ms"] for r in bad if r.get("ttft_ms") is not None)
    tpot = sorted(
        (r["latency_ms"] - r["ttft_ms"]) / (r["tokens_out"] - 1)
        for r in ok
        if r.get("tokens_out", 0) > 1 and r.get("ttft_ms") is not None
    )
    out_tok = sum(r.get("tokens_out", 0) for r in ok)
    in_tok = sum(r.get("tokens_in", 0) for r in ok)
    all_lat = sorted(r["latency_ms"] for r in measured if r.get("latency_ms") is not None)
    err_types = {}
    for r in bad:
        key = r.get("error", "?")
        key = "timeout_504" if "timed out waiting for result" in key else key[:40]
        err_types[key] = err_types.get(key, 0) + 1

    return {
        "run_id": run_id,
        "rate": summary.get("requested_rate"),
        "status": summary.get("status"),
        "measured": len(measured),
        "ok": len(ok),
        "bad": len(bad),
        "err_types": err_types,
        "lat_p50": pct(ok_lat, 50),
        "lat_p95": pct(ok_lat, 95),
        "lat_p50_all": pct(all_lat, 50),
        "ttft_p50": pct(ok_ttft, 50),
        "ttft_p95": pct(ok_ttft, 95),
        "bad_ttft_p50": pct(bad_ttft, 50),
        "tpot_p50": pct(tpot, 50),
        "out_tok_per_s": out_tok / duration_s if duration_s else float("nan"),
        "in_tok_per_s": in_tok / duration_s if duration_s else float("nan"),
        "skipped": summary.get("skipped_offered"),
        "offered": summary.get("offered_requests"),
        "smoke": summary.get("smoke_check", {}),
        "duration_s": duration_s,
    }


def series(d, name):
    """Flatten every series of one metric name into (ts, value) samples."""
    return [(t, float(v)) for s in d[name] for t, v in s["values"]]


def series_by_label(d, name, label):
    """One (label_value, final_value) tuple per series of a metric name."""
    out = []
    for s in d[name]:
        vals = [(t, float(v)) for t, v in s["values"]]
        out.append((s["metric"].get(label, "?"), vals[-1][1]))
    return out


def engine_table(exp_dir, summaries, duration_default=120.0):
    metrics = sorted(glob.glob(os.path.join(exp_dir, "metrics", "prometheus-*.json")))
    if not metrics:
        return None
    d = json.load(open(metrics[-1]))  # the longest export window
    def one(name):
        return series(d, name)
    out = []
    for run_id, (s, _) in summaries.items():
        t0 = parse_ts(s["measured_started_at"])
        dur = s.get("offered_requests", 0) / max(s.get("requested_rate", 1), 1e-9) or duration_default
        t1 = t0 + dur
        win = lambda vals: [v for t, v in vals if t0 <= t <= t1]
        def slope(vals):
            w = [(t, v) for t, v in vals if t0 <= t <= t1]
            if len(w) < 2:
                return float("nan")
            return (w[-1][1] - w[0][1]) / (w[-1][0] - w[0][0])
        gpu = win(one("DCGM_FI_DEV_GPU_UTIL_RATIO"))
        tensor = win(one("DCGM_FI_PROF_TENSOR_UTIL_RATIO"))
        power = win(one("DCGM_FI_DEV_BOARD_POWER_WATTS"))
        out.append({
            "run_id": run_id,
            "rate": s.get("requested_rate"),
            "gen_tok_s": slope(one("vllm:generation_tokens_total")),
            "prompt_tok_s": slope(one("vllm:prompt_tokens_total")),
            "running_max": max(win(one("vllm:num_requests_running")), default=0),
            "running_mean": (sum(win(one("vllm:num_requests_running"))) / len(win(one("vllm:num_requests_running")))) if win(one("vllm:num_requests_running")) else 0,
            "waiting_max": max(win(one("vllm:num_requests_waiting")), default=0),
            "qdepth_max": max(win(one("mastori_scheduler_queue_depth")), default=0),
            "wq_max": max(win(one("mastori_scheduler_worker_queue_depth")), default=0),
            "kv_max": max(win(one("vllm:kv_cache_usage_perc")), default=0),
            "gpu_max": max(gpu, default=0),
            "tensor_max": max(tensor, default=0),
            "power_mean": sum(power) / len(power) if power else float("nan"),
        })
    return out


def fmt(x, nd=0):
    if x != x:  # NaN
        return "—"
    return f"{x:.{nd}f}"


def main():
    exp_dirs = sys.argv[1:]
    run_dirs = [os.path.join(e, "runs") for e in exp_dirs]
    summaries = load_runs([d for d in run_dirs if os.path.isdir(d)])

    print("== client side (successes only unless noted; measured window) ==")
    print("run | rate | status | ok/total | lat p50 | lat p95 | ttft p50 | ttft p95 | tpot p50 | out tok/s | skipped/offered")
    for rate, rep, run_id in order_entries(exp_dirs[0]):
        if run_id not in summaries:
            continue
        s, path = summaries[run_id]
        dur = s["offered_requests"] / max(s["requested_rate"], 1e-9)
        c = client_table(run_id, s, path, dur)
        print(f"{run_id} | {c['rate']} | {c['status']} | {c['ok']}/{c['measured']}"
              f" | {fmt(c['lat_p50'])} | {fmt(c['lat_p95'])} | {fmt(c['ttft_p50'])}"
              f" | {fmt(c['ttft_p95'])} | {fmt(c['tpot_p50'], 1)}ms"
              f" | {fmt(c['out_tok_per_s'])} | {c['skipped']}/{c['offered']}")
        if c["bad"]:
            print(f"    failures: {c['err_types']} (all-rows p50 {fmt(c['lat_p50_all'])} ms,"
                  f" failed-row ttft p50 {fmt(c['bad_ttft_p50'])} ms)")

    print("\n== gates ==")
    for rate, rep, run_id in order_entries(exp_dirs[0]):
        if run_id not in summaries:
            continue
        s, _ = summaries[run_id]
        print(f"{run_id} smoke={s.get('smoke_check')} workers={s.get('workers_healthy_preflight')}/{s.get('expected_workers')} dispatched={s.get('dispatched_by_worker')}")

    print("\n== engine side (per measured window, Prometheus 15s samples) ==")
    print("run | rate | decode tok/s | prefill tok/s | run max/mean | wait max | plat q max | worker q max | kv max | gpu max | tensor max | W mean")
    for e in engine_table(exp_dirs[0], summaries) or []:
        print(f"{e['run_id']} | {e['rate']} | {fmt(e['gen_tok_s'])} | {fmt(e['prompt_tok_s'])}"
              f" | {e['running_max']:.0f}/{e['running_mean']:.0f} | {e['waiting_max']:.0f}"
              f" | {e['qdepth_max']:.0f} | {e['wq_max']:.0f} | {e['kv_max']:.0%}"
              f" | {e['gpu_max']:.0%} | {e['tensor_max']:.2f} | {fmt(e['power_mean'])}")

    print("\n== session cross-checks ==")
    metrics = sorted(glob.glob(os.path.join(exp_dirs[0], "metrics", "prometheus-*.json")))
    if metrics:
        d = json.load(open(metrics[-1]))
        gen = series(d, "vllm:generation_tokens_total")[-1][1]
        print(f"engine generated tokens (whole export): {gen:.0f}")
        ok_tok = sum(
            r.get("tokens_out", 0)
            for _, p in summaries.values()
            for r in (json.loads(l) for l in open(p))
            if not r.get("error") and r.get("tokens_out")
        )
        print(f"client-observed output tokens (ok rows incl. warmup): {ok_tok:.0f}")
        print(f"delta (generated for abandoned/timed-out requests): {gen - ok_tok:.0f}"
              f" ({(gen - ok_tok) / gen:.1%} of engine output)")
        by_src = series_by_label(d, "vllm:prompt_tokens_by_source_total", "source")
        tot = sum(v for _, v in by_src)
        for lbl, v in by_src:
            share = f"{v / tot:.1%}" if tot else "?"
            print(f"prompt tokens {lbl}: {v:.0f} ({share})")


if __name__ == "__main__":
    main()
