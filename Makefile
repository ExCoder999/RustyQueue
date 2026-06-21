DB_URL ?= postgres://rustyqueue:rustyqueue@localhost:5432/rustyqueue

.PHONY: dev build test test-db fmt lint check migrate migrate-revert \
        docker-build docker-up docker-down docker-logs

dev:
	RUST_LOG=debug cargo run

build:
	cargo build --release

# Fast unit tests — no database required
test:
	cargo test

# Full test suite — requires a running PostgreSQL instance
test-db:
	DATABASE_URL=$(DB_URL) cargo test -- --include-ignored

fmt:
	cargo fmt

lint:
	cargo clippy -- -D warnings

# Run all static checks (CI equivalent)
check: fmt lint
	cargo check

migrate:
	sqlx migrate run --database-url $(DB_URL)

migrate-revert:
	sqlx migrate revert --database-url $(DB_URL)

docker-build:
	docker build -t rustyqueue:latest .

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

docker-logs:
	docker compose logs -f rustyqueue
