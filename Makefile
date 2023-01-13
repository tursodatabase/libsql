test: libsql-test packages-test
.PHONY: test

libsql-test:
	@cargo build
	@./testing/run
.PHONY: libsq-test

packages-test:
	@cd packages/libsql-client && npm i && npm run build && npm test
.PHONY: packages-test
