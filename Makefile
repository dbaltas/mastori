CLUSTER_NAME := mastori

.PHONY: build test test-integration vet fmt site run docker-build cluster-up cluster-down cluster-status cluster-deploy cluster-redeploy-api cluster-deploy-inference kustomize-verify

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
