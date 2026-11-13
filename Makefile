CLUSTER_NAME := mastori

.PHONY: build test test-integration vet fmt run docker-build compose-up compose-up-inference compose-down cluster-up cluster-down cluster-status cluster-deploy cluster-redeploy-api cluster-deploy-inference

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

fmt:
	gofmt -l .

run: build
	go run ./cmd/api

docker-build:
	docker build -t mastori/api:latest .

compose-up:
	docker compose up --build -d

compose-up-inference:
	docker compose --profile inference up --build -d
	docker compose exec ollama ollama pull $${INFERENCE_MODEL:-qwen2.5:0.5b}

compose-down:
	docker compose down -v

cluster-up:
	kind create cluster --config infra/kind/kind-config.yaml

cluster-down:
	kind delete cluster --name $(CLUSTER_NAME)

cluster-status:
	kubectl get nodes -o wide

cluster-deploy: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl apply -f deploy/kubernetes/
	kubectl -n mastori create configmap grafana-dashboards --from-file=deploy/grafana/dashboards/ --dry-run=client -o yaml | kubectl apply -f -

cluster-redeploy-api: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl -n mastori set image deployment/api api=mastori/api:latest
	kubectl -n mastori rollout restart deployment/api deployment/scheduler deployment/mockworker

cluster-deploy-inference: docker-build
	kind load docker-image mastori/api:latest --name $(CLUSTER_NAME)
	kubectl apply -f deploy/inference/
	kubectl -n mastori rollout restart deployment/inference-adapter deployment/vllmworker
