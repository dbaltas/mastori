CLUSTER_NAME := mastori

AWS_DIR := infra/aws

.PHONY: build test test-integration redis-flush vet fmt site run docker-build docker-ghcr-build docker-ghcr-push cluster-up cluster-down cluster-status cluster-deploy cluster-redeploy-api cluster-deploy-inference kustomize-verify aws-init aws-plan aws-apply aws-destroy aws-kubeconfig

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
