.PHONY: fmt fmt-fix lint test acceptance acceptance-docker build check

fmt:
	cargo fmt -- --check

fmt-fix:
	cargo fmt

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

acceptance:
	cargo test --workspace

acceptance-docker:
	@name="ratelimit-valkey-test-$$(date +%s)-$$$$"; \
	docker run -d --rm --name "$$name" -p 127.0.0.1:6380:6379 valkey/valkey:8 >/dev/null; \
	trap 'docker rm -f "$$name" >/dev/null 2>&1 || true' EXIT; \
	for i in $$(seq 1 50); do \
		if docker exec "$$name" valkey-cli ping >/dev/null 2>&1; then break; fi; \
		sleep 0.1; \
	done; \
	VALKEY_URL=redis://127.0.0.1:6380 $(MAKE) acceptance

build:
	cargo build --release

check: fmt lint test
