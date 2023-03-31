all: release

release:	src/lib.rs embeddings.c
	$(CC) -I../.. -Wall -c embeddings.c && ar -crs libembeddings.a embeddings.o && cargo build --release && cp target/release/liblibsql_embeddings.so embeddings.so

test:
	cargo test

.PHONY:	clean
clean:
	rm embeddings.o libembeddings.a embeddings.so || :
