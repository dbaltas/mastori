CLUSTER_NAME := mastori

AWS_DIR := infra/aws
AWS_ECR_DIR := infra/aws-ecr

.PHONY: build test test-integration redis-flush vet fmt site run docker-build docker-ghcr-build docker-ghcr-push cluster-up cluster-down cluster-status cluster-env-capture cluster-metrics-capture cluster-deploy cluster-redeploy-api cluster-deploy-inference cluster-run-traffic cluster-traffic-results kustomize-verify aws-init aws-plan aws-apply aws-destroy aws-ecr-init aws-ecr-plan aws-ecr-apply aws-ecr-destroy aws-mirror-images aws-kubeconfig aws-datasets-cache-config aws-run-traffic aws-deploy aws-nvidia aws-deploy-inference aws-gpu-scale aws-status traffic-configmaps traffic-wait

build:
	go build ./...

test:
	go test ./...

# Starts a throwaway Redis (7099) and LocalStack S3 (7098) on loopback, runs
# the integration tests against them, then removes both. LocalStack backs the
# dataset cache roundtrip test (S3 API compatible, no cloud spend; MinIO is
# no longer published to Docker Hub).
test-integration:
	-@docker rm -f mastori-it-redis mastori-it-s3 >/dev/null 2>&1
	docker run -d --rm --name mastori-it-redis -p 7099:6379 redis:7-alpine >/dev/null
	docker run -d --rm --name mastori-it-s3 -p 7098:4566 -e SERVICES=s3 localstack/localstack:3.8.1 >/dev/null
	@sleep 8
	INTEGRATION_REDIS_ADDR=localhost:7099 INTEGRATION_S3_ADDR=localhost:7098 \
	go test -tags integration -race -count=1 ./test/integration/...; \
	status=$$?; docker rm -f mastori-it-redis mastori-it-s3 >/dev/null; exit $$status

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
	kubectl kustomize deploy/kubernetes/overlays/aws >/dev/null
	kubectl kustomize deploy/kubernetes/overlays/aws/traffic >/dev/null
	kubectl kustomize deploy/kubernetes/overlays/aws-inference >/dev/null
	kubectl kustomize deploy/kubernetes/overlays/aws-nvidia >/dev/null

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

# Long-lived ECR layer (infra/aws-ecr, separate state): survives
# aws-destroy; only torn down deliberately after an experiment series.
aws-ecr-init:
	cd $(AWS_ECR_DIR) && terraform init

aws-ecr-plan:
	cd $(AWS_ECR_DIR) && terraform plan

aws-ecr-apply:
	cd $(AWS_ECR_DIR) && terraform apply

aws-ecr-destroy:
	cd $(AWS_ECR_DIR) && terraform destroy

# Refresh the ECR push token (12h) and (re)run the in-cluster mirror jobs
# (deploy/kubernetes/overlays/aws/mirror-jobs.yaml). Run after the long-lived
# ECR layer is applied; jobs fetch vllm + weights into mastori/vllm and
# mastori/weights once — later node pulls go over the VPC endpoints NAT-free.
aws-mirror-images:
	aws ecr get-login-password --region eu-north-1 > /tmp/kilo/ecr-token
	kubectl -n mastori create secret generic ecr-push-creds \
	  --from-literal=username=AWS \
	  --from-file=password=/tmp/kilo/ecr-token \
	  --dry-run=client -o yaml | kubectl apply -f -
	rm /tmp/kilo/ecr-token
	kubectl -n mastori delete job mirror-vllm bake-weights --ignore-not-found
	kubectl apply -f deploy/kubernetes/overlays/aws/mirror-jobs.yaml
	kubectl -n mastori get pods -l app=mastori-mirror

aws-kubeconfig:
	aws eks update-kubeconfig --name $(CLUSTER_NAME) --region eu-north-1

# Prints the S3 dataset-cache values to paste into
# deploy/kubernetes/overlays/aws/traffic/{service-account,s3}.yaml (the
# IRSA role ARN and DATASETS_S3_BUCKET/AWS_REGION). Requires aws-apply.
aws-datasets-cache-config:
	@echo "service-account.yaml eks.amazonaws.com/role-arn:" && \
	  terraform -chdir=$(AWS_DIR) output -raw datasets_cache_role_arn && echo "" && \
	echo "s3.yaml DATASETS_S3_BUCKET:" && \
	  terraform -chdir=$(AWS_DIR) output -raw datasets_cache_bucket && echo "" && \
	echo "s3.yaml AWS_REGION:" && \
	  terraform -chdir=$(AWS_DIR) output -raw region && echo ""

# Applies the aws overlay (base workloads, GHCR images) to the current
# kubectl context. Requires the node label fix in infra/aws/eks.tf (already
# in the module): every workload node-selects mastori/pool: cpu.
aws-deploy:
	kubectl apply -k deploy/kubernetes/overlays/aws

# NVIDIA stack for the GPU node group (Milestone 2 prep): Prometheus RBAC
# glue, then the NVIDIA device plugin (advertises nvidia.com/gpu, enables
# GPU Feature Discovery labels) and dcgm-exporter (Prometheus telemetry)
# via Helm charts. Helm tolerations include the mastori/gpu node taint from
# Terraform; nodeSelector keeps the DaemonSets on the GPU pool. Requires
# aws-kubeconfig + aws-deploy (prometheus SA) first. The DaemonSets show 0
# desired pods while the GPU node group is scaled to zero (normal).
NVIDIA_TOLERATIONS := [{"key":"mastori/gpu","operator":"Exists","effect":"NoSchedule"},{"key":"nvidia.com/gpu","operator":"Exists","effect":"NoSchedule"}]
aws-nvidia:
	kubectl apply -k deploy/kubernetes/overlays/aws-nvidia
	helm repo add nvdp https://nvidia.github.io/k8s-device-plugin 2>/dev/null || true
	helm repo add nvidia-dcgm https://nvidia.github.io/dcgm-exporter/helm-charts 2>/dev/null || true
	helm repo update >/dev/null
	helm upgrade --install nvdp nvdp/nvidia-device-plugin -n nvidia \
	  --set gfd.enabled=true \
	  --set-json 'tolerations=$(NVIDIA_TOLERATIONS)' \
	  --set-json 'nodeSelector={"mastori/pool":"gpu"}' \
	  --set-json 'nfd.worker.tolerations=[{"key":"mastori/gpu","operator":"Exists","effect":"NoSchedule"},{"operator":"Exists"}]'
	helm upgrade --install dcgm-exporter nvidia-dcgm/dcgm-exporter -n nvidia \
	  --set-json 'tolerations=$(NVIDIA_TOLERATIONS)' \
	  --set-json 'nodeSelector={"mastori/pool":"gpu"}' \
	  --set serviceMonitor.enabled=false
	kubectl -n nvidia get ds

# Applies the GPU inference workloads (adapter + vllmworker, GHCR images) to
# the current kubectl context. vllmworker stays Pending until the GPU node
# group is scaled up (aws-gpu-scale) and the device plugin advertises GPUs.
aws-deploy-inference:
	kubectl apply -k deploy/kubernetes/overlays/aws-inference
	kubectl -n mastori get pods -l app=inference-adapter -l app=vllmworker

# Scale the GPU node group for a run (and back to 0 after). Scale to zero is
# the cost guardrail (decision 8): `make aws-gpu-scale GPU_NODES=0` when done.
GPU_NODES ?= 1
aws-gpu-scale:
	aws eks update-nodegroup-config --cluster-name $(CLUSTER_NAME) --region eu-north-1 \
	  --nodegroup-name $(CLUSTER_NAME)-gpu \
	  --scaling-config minSize=$(GPU_NODES),maxSize=$(GPU_NODES),desiredSize=$(GPU_NODES)

# Rollout + pod state for the Milestone 1 acceptance check (all pods healthy
# before the first traffic run).
aws-status:
	kubectl -n mastori rollout status deployment/api --timeout=5m
	kubectl -n mastori rollout status deployment/scheduler --timeout=5m
	kubectl -n mastori rollout status deployment/mockworker --timeout=5m
	kubectl -n mastori get pods -o wide
# Traffic run parameters for cluster-run-traffic and aws-run-traffic; override
# on the command line, e.g. `make cluster-run-traffic TRAFFIC_WORKLOAD=short
# TRAFFIC_RATE=5 TRAFFIC_DURATION=5s` or
# `make cluster-run-traffic TRAFFIC_STEPS="10:60s,50:60s"`.
#
# Empty variables are skipped by the /traffic env loader (cmd/traffic/config.go
# skips empty values), so the in-code defaults apply (steps, concurrency,
# warmup, short-percentage, and rate/duration when no steps are given).
#
# Steps decide the total duration themselves (sum of segment durations);
# setting TRAFFIC_DURATION together with TRAFFIC_STEPS is rejected here because
# an explicit duration silently extends the run past the last segment (it held
# the final segment's rate for 5m on 2026-11-18, invalidating a 9-run
# experiment). The runner still supports the combination via local flags.
TRAFFIC_WORKLOAD ?= mixed
TRAFFIC_RATE ?=
TRAFFIC_ARRIVAL ?= constant
TRAFFIC_DURATION ?=
TRAFFIC_STEPS ?=
TRAFFIC_CONCURRENCY ?=
TRAFFIC_WARMUP ?=
TRAFFIC_SHORT_PERCENTAGE ?=
TRAFFIC_SEED ?= 42
# Inflight ceiling for open-loop runs. The in-code default is 4x --rate, which
# throttles burst step profiles (a 500 rps segment with ~0.6s latency needs
# ~300 in flight). 1000 matches the published 2026-11-15 methodology.
TRAFFIC_MAX_INFLIGHT ?= 1000
TRAFFIC_WAIT_TIMEOUT ?= 30m
# Path to the dataset manifest (the benchmark submodule). The traffic Job
# fetches its in-cluster cache from this file, so changing datasets.yaml is
# enough to change what a run replays.
DATASETS_MANIFEST ?= benchmark/workloads/datasets.yaml

# (Re)creates the datasets-manifest and traffic-config ConfigMaps from the
# benchmark submodule and the TRAFFIC_* variables above. Shared by
# cluster-run-traffic (kind) and aws-run-traffic (EKS).
traffic-configmaps:
	@test -f "$(DATASETS_MANIFEST)" || { \
	  echo "dataset manifest missing at $(DATASETS_MANIFEST): run git submodule update --init --recursive" >&2; exit 1; }
	@test -z "$(TRAFFIC_STEPS)" -o -z "$(TRAFFIC_DURATION)" || { \
	  echo "ERROR: TRAFFIC_STEPS and TRAFFIC_DURATION are both set." >&2; \
	  echo "The steps' durations already define the total run; an explicit duration would extend the run past the last segment (it holds the final rate)." >&2; \
	  echo "Leave TRAFFIC_DURATION empty, or drop the steps for a fixed-rate run." >&2; exit 1; }
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
	  'TRAFFIC_SEED=$(TRAFFIC_SEED)' \
	  'TRAFFIC_MAX_INFLIGHT=$(TRAFFIC_MAX_INFLIGHT)'; \
	do echo "$$kv" >> $$envfile; done && \
	kubectl -n mastori apply -f deploy/kubernetes/base/traffic-config.yaml && \
	kubectl -n mastori create configmap datasets-manifest --from-file=datasets.yaml=$(DATASETS_MANIFEST) --dry-run=client -o yaml | kubectl apply -f - -n mastori && \
	kubectl -n mastori create configmap traffic-config --from-env-file=$$envfile --dry-run=client -o yaml | kubectl apply -f - -n mastori

# Waits for the traffic Job to complete, prints the summary, and points at
# the results-recovery target. Shared by cluster-run-traffic and
# aws-run-traffic; cluster-traffic-results works on either cluster because it
# only talks to the current kubectl context.
traffic-wait:
	@echo "waiting for the traffic job (timeout $(TRAFFIC_WAIT_TIMEOUT))..." && \
	if ! kubectl -n mastori wait --for=condition=complete --timeout=$(TRAFFIC_WAIT_TIMEOUT) job/traffic; then \
	  failed=$$(kubectl -n mastori get job traffic -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null); \
	  if [ "$$failed" = "True" ]; then \
	    echo "traffic job failed (init or run error):" && \
	    kubectl -n mastori logs job/traffic --all-containers --tail=50 2>&1; exit 1; \
	  fi; \
	  echo "traffic job did not complete:" && \
	  kubectl -n mastori get pods -l app=traffic && \
	  kubectl -n mastori logs job/traffic --tail=50; exit 1; fi && \
	log=$$(mktemp) && trap 'rm -f "$$log"' EXIT && \
	kubectl -n mastori logs job/traffic > $$log && \
	grep '^run ' $$log && awk '/^\{$$/,/^\}$$/' $$log && \
	echo "(records saved to the pod logs; run make cluster-traffic-results to write the JSONL files)"

# Captures the live cluster environment into
# benchmark/results/<experiment>/environment/ before the first traffic run of
# an experiment: node dump (instance type, AZ, GPU chip/count, allocatable,
# labels, taints, kubelet/containerd versions), all mastori pods (images,
# image digests, env, resources), configmaps (api-config, traffic-config,
# datasets-manifest), workload list, kubectl version, host info, and a
# PROVENANCE.md skeleton to
# hand-finish with context (co-location caveats, Redis state notes, image
# build source).
# Usage: make cluster-env-capture EXPERIMENT=20261120-eks-mockworker-knee
# cluster-run-traffic and aws-run-traffic call this automatically when
# EXPERIMENT is set and no PROVENANCE.md exists yet (capture once per
# experiment; re-running the target by hand overwrites the snapshot).
cluster-env-capture:
	@test -n "$(EXPERIMENT)" || { echo "ERROR: set EXPERIMENT=<name>, e.g. make cluster-env-capture EXPERIMENT=20261120-eks-mockworker-knee" >&2; exit 1; }
	@mkdir -p benchmark/results/$(EXPERIMENT)/environment
	@echo "capturing environment into benchmark/results/$(EXPERIMENT)/environment (context: $$(kubectl config current-context 2>/dev/null))"
	kubectl version > benchmark/results/$(EXPERIMENT)/environment/kubectl-version.txt
	kubectl get nodes -o wide > benchmark/results/$(EXPERIMENT)/environment/nodes-wide.txt
	kubectl get nodes -o yaml > benchmark/results/$(EXPERIMENT)/environment/nodes.yaml
	kubectl -n mastori get pods -o wide > benchmark/results/$(EXPERIMENT)/environment/pods-wide.txt
	kubectl -n mastori get pods -o yaml > benchmark/results/$(EXPERIMENT)/environment/pods.yaml
	kubectl -n mastori get configmap -o yaml > benchmark/results/$(EXPERIMENT)/environment/configmaps.yaml
	kubectl -n mastori get deploy,statefulset,job -o wide > benchmark/results/$(EXPERIMENT)/environment/workloads-wide.txt
	kubectl -n mastori get pods -o jsonpath='{range .items[*]}{.metadata.name}{"  "}{range .status.containerStatuses[*]}{.image}{"  "}{.imageID}{"\n"}{end}{end}' | sort > benchmark/results/$(EXPERIMENT)/environment/image-digests.txt
	shasum -a 256 $(DATASETS_MANIFEST) | cut -d' ' -f1 > benchmark/results/$(EXPERIMENT)/environment/dataset-manifest.sha256
	@{ uname -a; docker info 2>/dev/null | grep -E '^(Server Version|CPUs|Total Memory):'; true; } > benchmark/results/$(EXPERIMENT)/environment/host.txt
	@{ \
	  echo '# Environment provenance — $(EXPERIMENT)'; \
	  echo; \
	  echo "- Captured: $$(date -u '+%Y-%m-%dT%H:%M:%SZ')"; \
	  echo "- kubectl context: $$(kubectl config current-context 2>/dev/null)"; \
	  echo "- mastori commit: $$(git rev-parse --short=7 HEAD)"; \
	  echo "- benchmark submodule: $$(git -C benchmark rev-parse --short=7 HEAD)"; \
	  echo "- dataset manifest SHA-256: $$(cat benchmark/results/$(EXPERIMENT)/environment/dataset-manifest.sha256)"; \
	  echo; \
	  echo 'Node summary (name, instance type, AZ, GPU chip+count, allocatable CPU/mem):'; \
	  kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"  "}{.metadata.labels.node\.kubernetes\.io/instance-type}{"  az="}{.metadata.labels.topology\.kubernetes\.io/zone}{"  gpu="}{.metadata.labels.nvidia\.com/gpu\.product}{" x"}{.status.allocatable.nvidia\.com/gpu}{"  "}{.status.allocatable.cpu}{"  "}{.status.allocatable.memory}{"\n"}{end}' 2>/dev/null | sed 's/^/  /'; \
	  echo; \
	  echo 'Pool topology (cross-AZ pools mean AZ-transfer cost/latency in the measured path):'; \
	  echo "  cpu zones: $$(kubectl get nodes -l mastori/pool=cpu -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' 2>/dev/null | sort -u | paste -sd, -)"; \
	  echo "  gpu zones: $$(kubectl get nodes -l mastori/pool=gpu -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' 2>/dev/null | sort -u | paste -sd, -)"; \
	  echo; \
	  echo 'Workloads in the mastori namespace (deploy/statefulset/job):'; \
	  kubectl -n mastori get deploy,statefulset,job -o wide 2>/dev/null | sed 's/^/  /'; \
	  echo; \
	  echo 'Raw captures in this folder:'; \
	  echo '  - nodes.yaml / nodes-wide.txt (instance type, capacity/allocatable, labels, taints, kubelet + containerd versions, kernel, AMI)'; \
	  echo '  - pods.yaml / pods-wide.txt (per-pod images, image digests, env, resources, initContainers)'; \
	  echo '  - configmaps.yaml (api-config, traffic-config, datasets-manifest)'; \
	  echo '  - workloads-wide.txt, image-digests.txt, kubectl-version.txt, host.txt, dataset-manifest.sha256'; \
	  echo; \
	  echo '<!-- Hand-finish with context: co-location/caveats, Redis state notes, image -->'; \
	  echo '<!-- build source (commit the image was built from), workload config rationale. -->'; \
	} > benchmark/results/$(EXPERIMENT)/environment/PROVENANCE.md
	@echo "PROVENANCE.md skeleton written; hand-finish the context notes before publishing"

# Runs the traffic generator inside the kind cluster as a Job: rebuilds the
# image, replaces the traffic-config ConfigMap, applies the Job template, and
# waits for completion. Records are streamed to stdout by the container
# (TRAFFIC_RECORDS_STDOUT) and recovered from the pod logs with
# cluster-traffic-results — the distroless image has no shell or tar.
# Set EXPERIMENT=<name> to auto-capture the environment snapshot before the
# first run (skipped once benchmark/results/<name>/environment/PROVENANCE.md
# exists).
cluster-run-traffic: docker-build
	@if [ -n "$(EXPERIMENT)" ] && [ ! -f benchmark/results/$(EXPERIMENT)/environment/PROVENANCE.md ]; then $(MAKE) cluster-env-capture EXPERIMENT=$(EXPERIMENT); fi
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	$(MAKE) traffic-configmaps
	kubectl -n mastori delete job traffic --ignore-not-found
	kubectl -n mastori apply -f deploy/kubernetes/overlays/aws/traffic/traffic-job.yaml
	$(MAKE) traffic-wait

# Runs the traffic generator inside the EKS cluster: the Job is applied
# through a temp-dir copy of the aws/traffic overlay whose dataset-cache
# values are rendered live from Terraform outputs — the committed overlay
# keeps placeholder values (single source of truth is Terraform), so the
# run never depends on the working tree (concurrent sessions may hold or
# restore those files). Results recovery is the same cluster-traffic-results
# target — it only uses the current kubectl context. Set EXPERIMENT=<name>
# for the same auto-capture hook as cluster-run-traffic.
aws-run-traffic:
	@if [ -n "$(EXPERIMENT)" ] && [ ! -f benchmark/results/$(EXPERIMENT)/environment/PROVENANCE.md ]; then $(MAKE) cluster-env-capture EXPERIMENT=$(EXPERIMENT); fi
	$(MAKE) traffic-configmaps
	kubectl -n mastori delete job traffic --ignore-not-found
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
	cp -R deploy/kubernetes/overlays/aws/traffic/. $$tmp/ && \
	bucket=$$(terraform -chdir=$(AWS_DIR) output -raw datasets_cache_bucket) && \
	role=$$(terraform -chdir=$(AWS_DIR) output -raw datasets_cache_role_arn) && \
	region=$$(terraform -chdir=$(AWS_DIR) output -raw region) && \
	sed -i.bak1 -e "s/REPLACE_DATASETS_CACHE_BUCKET/$$bucket/" $$tmp/s3.yaml && \
	sed -i.bak2 -e "s/REPLACE_DATASETS_CACHE_REGION/$$region/" $$tmp/s3.yaml && \
	sed -i.bak3 -e "s|arn:aws:iam::000000000000:role/REPLACE_ME|$$role|" $$tmp/service-account.yaml && \
	rm -f $$tmp/*.bak* && \
	kubectl -n mastori apply -k $$tmp
	$(MAKE) traffic-wait

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

# Exports the Prometheus series for an experiment window into
# benchmark/results/<EXPERIMENT>/metrics/, the committed replacement for the
# hand-run export behind benchmark/results/20261122-eks-gpu-vllm-knee/metrics/.
# One query_range per window, grouped by metric name; the default match keeps
# the vllm:* engine counters (prompt/generation token totals, TTFT/TPOT
# histograms) — the authoritative cross-check against client-side tok/s —
# plus the DCGM and API series. Run BEFORE teardown; Prometheus retention is
# 24h. Usage:
#   make cluster-metrics-capture EXPERIMENT=<name> [START=...] [END=...] [STEP=15s] [MATCH='vllm:|DCGM_|mastori_']
# START/END are RFC3339 UTC (END defaults to now, START to END-1h). In a run
# recipe, pass both explicitly so the window spans the whole experiment.
cluster-metrics-capture:
	@test -n "$(EXPERIMENT)" || { echo "ERROR: set EXPERIMENT=<name>, e.g. make cluster-metrics-capture EXPERIMENT=20261123-eks-gpu-vllm" >&2; exit 1; }
	@mkdir -p benchmark/results/$(EXPERIMENT)/metrics
	@kubectl -n mastori get deploy/prometheus >/dev/null 2>&1 || { echo "ERROR: prometheus deployment not found in namespace mastori" >&2; exit 1; }
	@kubectl -n mastori port-forward deploy/prometheus 9091:9090 >/dev/null 2>&1 & \
	pfpid=$$!; trap 'kill $$pfpid 2>/dev/null' EXIT; \
	sleep 2; \
	go run ./cmd/promcapture \
	  -url http://localhost:9091 \
	  -out benchmark/results/$(EXPERIMENT)/metrics \
	  $(if $(START),-start $(START)) $(if $(END),-end $(END)) \
	  -step $(if $(STEP),$(STEP),15s) \
	  -match "$(if $(MATCH),$(MATCH),vllm:|DCGM_|mastori_)"
