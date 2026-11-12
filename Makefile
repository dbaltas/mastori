CLUSTER_NAME := mastori

.PHONY: cluster-up cluster-down cluster-status

cluster-up:
	kind create cluster --config infra/kind/kind-config.yaml

cluster-down:
	kind delete cluster --name $(CLUSTER_NAME)

cluster-status:
	kubectl get nodes -o wide
