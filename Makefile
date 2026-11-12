CLUSTER_NAME := mastori

.PHONY: build test vet fmt run docker-build compose-up compose-down cluster-up cluster-down cluster-status

build:
	go build ./...

test:
	go test ./...

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

compose-down:
	docker compose down -v

cluster-up:
	kind create cluster --config infra/kind/kind-config.yaml

cluster-down:
	kind delete cluster --name $(CLUSTER_NAME)

cluster-status:
	kubectl get nodes -o wide
