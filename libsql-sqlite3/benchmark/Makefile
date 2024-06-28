C_INCLUDE_PATH := ../
all: simple_workload no_vectors

diskanntest: diskanntest.c
	@cc diskanntest.c -o diskanntest -L ../.libs/ -llibsql

simple_workload.sql: 
	@python3 gen_workload.py simple_workload 64 100 100 > simple_workload.sql
simple_workload: diskanntest simple_workload.sql
	@rm test.db || exit 0
	@LD_LIBRARY_PATH=../.libs/ ./diskanntest simple_workload.sql test.db

no_vectors.sql: 
	@python3 gen_workload.py no_vectors 100 100 > no_vectors.sql
no_vectors: diskanntest no_vectors.sql
	@rm test.db || exit 0
	@LD_LIBRARY_PATH=../.libs/ ./diskanntest no_vectors.sql test.db
clear:
	-rm *.sql
	-rm diskanntest
	-rm *.db
	-rm *.db-shm
	-rm *.db-wal
