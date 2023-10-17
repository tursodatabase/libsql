all:	debug

debug:	bottomless.c src/lib.rs
	cargo build -p bottomless && clang -Wall -fPIC -shared -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c -I${LIBSQL_DIR} ../target/debug/libbottomless.a -o ../target/debug/bottomless.so

release:	bottomless.c src/lib.rs
	cargo build -p bottomless -j1	--quiet --release && \
		clang -fPIC -shared -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c -I${LIBSQL_DIR} ../target/release/libbottomless.a \
		-o ../target/release/bottomless.so

.PHONY: test
test:	debug
	( cd test && ./smoke_test.sh )
