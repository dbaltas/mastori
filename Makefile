CLUSTER_NAME := mastori

AWS_DIR := infra/aws

.PHONY: build test test-integration redis-flush vet fmt site run docker-build docker-ghcr-build docker-ghcr-push cluster-up cluster-down cluster-status cluster-deploy cluster-redeploy-api cluster-deploy-inference cluster-run-traffic cluster-traffic-results kustomize-verify aws-init aws-plan aws-apply aws-destroy aws-kubeconfig

build:
	go build ./...

test:
	go test ./...

# Starts a throwaway Redis on 7099, runs the integration tests against it, then removes it.
test-integration:
	-@docker rm -f mastori-it-redis >/dev/null 2>&1
	docker run -d --rm --name mastori-it-redis -p 7099:6379 redis:7-alpine >/dev/null
	@sleep 1
	INTEGRATION_REDIS_ADDR=localhost:7099 go test -tags integration -race -count=1 ./test/integration/...; \
	status=$$?; docker rm -f mastori-it-redis >/dev/null; exit $$status

# FLUSHDB against the Redis pod in the kind cluster. Only run between
# experiments: it kills in-flight jobs and consumer groups.
redis-flush:
	kubectl -n mastori exec deploy/redis -- redis-cli FLUSHDB

vet:
	go vet ./...

# Renders the static benchmark-stories site from the benchmark submodule into site/.
site:
	go run ./cmd/sitegen -benchmark benchmark -out site

fmt:
	gofmt -l .

run: build
	go run ./cmd/api

docker-build:
	docker build -t mastori/api:latest .

GHCR_IMAGE := ghcr.io/dbaltas/mastori
GIT_SHA := $(shell git rev-parse --short=7 HEAD)

docker-ghcr-build:
	docker build -t $(GHCR_IMAGE):$(GIT_SHA) -t $(GHCR_IMAGE):latest .

docker-ghcr-push: docker-ghcr-build
	docker push $(GHCR_IMAGE):$(GIT_SHA)
	docker push $(GHCR_IMAGE):latest

cluster-up:
	kind create cluster --config infra/kind/kind-config.yaml
	@# kind cannot set taints in its config; taint the gpu-pool workers here.
	@nodes=$$(kubectl get nodes -l mastori/pool=gpu -o name) && \
		for n in $$nodes; do kubectl taint node $$n mastori/gpu=true:NoSchedule; done

cluster-down:
	kind delete cluster --name $(CLUSTER_NAME)

cluster-status:
	kubectl get nodes -o wide

cluster-deploy: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl apply -k deploy/kubernetes/overlays/local

cluster-redeploy-api: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl -n mastori set image deployment/api api=mastori/api:latest
	kubectl -n mastori rollout restart deployment/api deployment/scheduler deployment/mockworker

cluster-deploy-inference: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl apply -k deploy/kubernetes/overlays/inference
	kubectl -n mastori rollout restart deployment/inference-adapter deployment/vllmworker

# Renders every overlay; fails on malformed kustomizations or templates.
kustomize-verify:
	kubectl kustomize deploy/kubernetes/overlays/local >/dev/null
	kubectl kustomize deploy/kubernetes/overlays/inference >/dev/null

# AWS EKS (Milestone 1, eu-north-1). Applies run from infra/aws with local state;
# see infra/aws/README.md for access steps and cost guardrails.
aws-init:
	cd $(AWS_DIR) && terraform init

aws-plan:
	cd $(AWS_DIR) && terraform plan

aws-apply:
	cd $(AWS_DIR) && terraform apply

aws-destroy:
	cd $(AWS_DIR) && terraform destroy

aws-kubeconfig:
	aws eks update-kubeconfig --name $(CLUSTER_NAME) --region eu-north-1

# Traffic run parameters for cluster-run-traffic; override on the command line,
# e.g. `make cluster-run-traffic TRAFFIC_WORKLOAD=short TRAFFIC_RATE=5 TRAFFIC_DURATION=5s`
# or `make cluster-run-traffic TRAFFIC_STEPS="10:60s,50:60s"`. Empty variables are
# placeholders that keep the in-code defaults (steps, concurrency, warmup,
# short-percentage).
TRAFFIC_WORKLOAD ?= mixed
TRAFFIC_RATE ?= 10
TRAFFIC_ARRIVAL ?= constant
TRAFFIC_DURATION ?= 5m
TRAFFIC_STEPS ?=
TRAFFIC_CONCURRENCY ?=
TRAFFIC_WARMUP ?=
TRAFFIC_SHORT_PERCENTAGE ?=
TRAFFIC_SEED ?= 42
TRAFFIC_WAIT_TIMEOUT ?= 30m
# Path to the dataset manifest (the benchmark submodule). The traffic Job
# fetches its in-cluster cache from this file, so changing datasets.yaml is
# enough to change what a run replays.
DATASETS_MANIFEST ?= benchmark/workloads/datasets.yaml

# Runs the traffic generator inside the kind cluster as a Job: rebuilds the
# image, replaces the traffic-config ConfigMap, applies the Job template, and
# waits for completion. Records are streamed to stdout by the container
# (TRAFFIC_RECORDS_STDOUT) and recovered from the pod logs with
# cluster-traffic-results — the distroless image has no shell or tar.
cluster-run-traffic: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	@envfile=$$(mktemp) && trap 'rm -f "$$envfile"' EXIT && \
	{ echo 'TRAFFIC_URL=http://api.mastori.svc.cluster.local:80'; \
	  echo 'REDIS_ADDR=redis.mastori.svc.cluster.local:6379'; \
	  echo 'TRAFFIC_CACHE=/data/cache'; \
	  echo 'TRAFFIC_OUTPUT=/data/results/run.jsonl'; \
	  echo 'TRAFFIC_RECORDS_STDOUT=true'; \
	  echo 'TRAFFIC_MANIFEST=/etc/mastori/datasets.yaml'; } > $$envfile && \
	for kv in \
	  'TRAFFIC_WORKLOAD=$(TRAFFIC_WORKLOAD)' \
	  'TRAFFIC_RATE=$(TRAFFIC_RATE)' \
	  'TRAFFIC_ARRIVAL=$(TRAFFIC_ARRIVAL)' \
	  'TRAFFIC_DURATION=$(TRAFFIC_DURATION)' \
	  'TRAFFIC_STEPS=$(TRAFFIC_STEPS)' \
	  'TRAFFIC_CONCURRENCY=$(TRAFFIC_CONCURRENCY)' \
	  'TRAFFIC_WARMUP=$(TRAFFIC_WARMUP)' \
	  'TRAFFIC_SHORT_PERCENTAGE=$(TRAFFIC_SHORT_PERCENTAGE)' \
	  'TRAFFIC_SEED=$(TRAFFIC_SEED)'; \
	do echo "$$kv" >> $$envfile; done && \
	kubectl -n mastori apply -f deploy/kubernetes/base/traffic-config.yaml && \
	if test ! -f "$(DATASETS_MANIFEST)"; then \
	  echo "dataset manifest missing at $(DATASETS_MANIFEST): run git submodule update --init --recursive" >&2; exit 1; fi && \
	kubectl -n mastori create configmap datasets-manifest --from-file=datasets.yaml=$(DATASETS_MANIFEST) --dry-run=client -o yaml | kubectl apply -f - -n mastori && \
	kubectl -n mastori create configmap traffic-config --from-env-file=$$envfile --dry-run=client -o yaml | kubectl apply -f - -n mastori && \
	kubectl -n mastori delete job traffic --ignore-not-found && \
	kubectl -n mastori apply -f deploy/kubernetes/traffic-job.yaml && \
	echo "waiting for the traffic job (timeout $(TRAFFIC_WAIT_TIMEOUT))..." && \
	if ! kubectl -n mastori wait --for=condition=complete --timeout=$(TRAFFIC_WAIT_TIMEOUT) job/traffic; then \
	  echo "traffic job did not complete:" && \
	  kubectl -n mastori get pods -l app=traffic && \
	  kubectl -n mastori logs job/traffic --tail=50; exit 1; fi && \
	log=$$(mktemp) && trap 'rm -f "$$envfile" "$$log"' EXIT && \
	kubectl -n mastori logs job/traffic > $$log && \
	grep '^run ' $$log && awk '/^\{$$/,/^\}$$/' $$log && \
	echo "(records saved to the pod logs; run make cluster-traffic-results to write the JSONL files)"

# Recovers the records and summary of the last traffic job from the pod logs
# into benchmark/results/cluster/.
cluster-traffic-results:
	@log=$$(mktemp) && trap 'rm -f "$$log"' EXIT && \
	kubectl -n mastori logs job/traffic > $$log && \
	runid=$$(sed -n 's/^run \([0-9A-Za-z-]*\):.*/\1/p' $$log | head -1) && \
	if test -z "$$runid"; then echo "no completed traffic run found in job/traffic logs" >&2; exit 1; fi && \
	mkdir -p benchmark/results/cluster && \
	grep -F '"schema_version":"' $$log > benchmark/results/cluster/$$runid.jsonl && \
	awk '/^\{$$/,/^\}$$/' $$log > benchmark/results/cluster/$$runid.summary.json && \
	echo "wrote benchmark/results/cluster/$$runid.jsonl and benchmark/results/cluster/$$runid.summary.json"
