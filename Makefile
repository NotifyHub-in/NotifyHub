COMPOSE_FILE := deployments/docker/compose.yml

.PHONY: test fmt run-api run-worker run-callback up down logs ps config load-test

test:
	go test ./...

fmt:
	go fmt ./...

run-api:
	go run ./apps/api/cmd/api

run-worker:
	go run ./apps/worker/cmd/worker

run-callback:
	go run ./apps/callback-gateway/cmd/callback-gateway

up:
	docker compose -f $(COMPOSE_FILE) up --build -d

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

ps:
	docker compose -f $(COMPOSE_FILE) ps

config:
	docker compose -f $(COMPOSE_FILE) config

load-test:
	go run ./tests/load/cmd/loadgen
