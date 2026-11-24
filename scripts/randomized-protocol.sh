#!/usr/bin/env bash
# Randomized benchmark protocol (review 2026-11-24, 05 §6 / H7): the 11-22/23
# sessions ran configs in ascending order, N=1, 30s windows, no warmup, no
# restarts. This script runs the fix: a shuffled list of rate configs and
# repetitions, a discarded warmup per run, a Redis flush between runs, and an
# engine restart between configs (reset prefix cache / KV state), recording
# the executed order so results stay attributable.
#
# Usage: EXPERIMENT=20261124-randomized-protocol-pilot scripts/randomized-protocol.sh
#
# Env overrides: RATES="5 10 20 40" REPS=3 RUN_DURATION=2m WARMUP=30s
#                ORDER_SEED=42 WORKLOAD=short MAX_TOKENS=256
#                RESTART_ENGINE=0 skips the between-config engine restart
#                (e.g. while GPU node changes are off-limits).
set -euo pipefail

EXPERIMENT="${EXPERIMENT:?set EXPERIMENT=<name>, e.g. 20261124-randomized-protocol-pilot}"
RATES="${RATES:-5 10 20 40}"
REPS="${REPS:-3}"
RUN_DURATION="${RUN_DURATION:-2m}"
WARMUP="${WARMUP:-30s}"
ORDER_SEED="${ORDER_SEED:-42}"
WORKLOAD="${WORKLOAD:-short}"
MAX_TOKENS="${MAX_TOKENS:-256}"
RESTART_ENGINE="${RESTART_ENGINE:-1}"

# Append-mode: repeated invocations with the same EXPERIMENT accumulate
# reps in one order file (a second set should re-shuffle via ORDER_SEED).
exp_dir="benchmark/results/${EXPERIMENT}"
mkdir -p "$exp_dir"
order_file="$exp_dir/order.txt"
if [ ! -f "$order_file" ]; then
  {
    echo "# randomized protocol — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "# rates=[$RATES] reps=$REPS duration=$RUN_DURATION warmup=$WARMUP order_seed=$ORDER_SEED workload=$WORKLOAD max_tokens=$MAX_TOKENS"
  } > "$order_file"
fi

# Build the (rate, rep) schedule and shuffle it with a recorded seed: random
# execution order (H7) but reproducible after the fact.
schedule=$(python3 - "$ORDER_SEED" "$REPS" $RATES <<'PY'
import random, sys
seed, reps, rates = int(sys.argv[1]), int(sys.argv[2]), [int(r) for r in sys.argv[3:]]
items = [(r, rep) for r in rates for rep in range(1, reps + 1)]
random.Random(seed).shuffle(items)
for r, rep in items:
    print(r, rep)
PY
)

for entry in $schedule; do
  rate=$(echo "$entry" | cut -d' ' -f1)
  rep=$(echo "$entry" | cut -d' ' -f2)
  echo "=== run: rate=$rate rep=$rep ($(date -u '+%H:%M:%S'))"

  make redis-flush

  # Engine restart before the first rep of each rate: vLLM keeps prefix-cache
  # and KV state across runs; the review flags cross-run contamination (H7).
  if [ "$RESTART_ENGINE" = "1" ] && ! grep -q "rate=$rate " "$order_file"; then
    echo "--- restarting vllm engine (first rep of rate $rate)"
    kubectl -n mastori rollout restart deployment/vllm deployment/vllmworker
    kubectl -n mastori rollout status deployment/vllm --timeout=10m
    kubectl -n mastori rollout status deployment/vllmworker --timeout=10m
  fi

  make aws-run-traffic \
    EXPERIMENT="$EXPERIMENT" \
    TRAFFIC_WORKLOAD="$WORKLOAD" \
    TRAFFIC_RATE="$rate" \
    TRAFFIC_DURATION="$RUN_DURATION" \
    TRAFFIC_WARMUP="$WARMUP" \
    TRAFFIC_MAX_TOKENS="$MAX_TOKENS" \
    TRAFFIC_IGNORE_EOS=true \
    TRAFFIC_EXPECTED_WORKERS=1 \
    TRAFFIC_MAX_INFLIGHT=3000

  runid=$(make cluster-traffic-results | tail -1 | sed 's|.*/||;s|\.jsonl.*||')
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') rate=$rate rep=$rep run_id=$runid" >> "$order_file"
  echo "--- done: rate=$rate rep=$rep run_id=$runid"
done

echo "=== protocol complete"
cat "$order_file"
