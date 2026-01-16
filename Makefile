.PHONY: help build run run-origin run-edge test clean

help:
	@echo "Available commands:"
	@echo "  make build        - Build the project"
	@echo "  make run          - Run origin server locally"
	@echo "  make run-origin   - Run origin server with Docker"
	@echo "  make run-edge     - Run edge node with Docker"
	@echo "  make test         - Run tests"
	@echo "  make clean        - Clean build artifacts"

build:
	cargo build --release

run:
	@echo "Starting origin server..."
	@export $$(cat .env.origin | xargs) && cargo run --release

run-origin:
	docker-compose up origin

run-edge:
	docker-compose up edge-us-east

test:
	cargo test

clean:
	cargo clean
	docker-compose down -v