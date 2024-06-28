LD_LIBRARY_PATH := ../.libs/
C_INCLUDE_PATH := ../
diskanntest: diskanntest.c
	cc diskanntest.c -o diskanntest -L ../.libs/ -llibsql

simple_workload.sql: 
	python3 gen_workload.py simple_workload 64 100 100 > simple_workload.sql
simple_workload: diskanntest simple_workload.sql
	-rm test.db
	LD_LIBRARY_PATH=../.libs/ ./diskanntest simple_workload.sql test.db
clear:
	-rm *.sql
	-rm diskanntest
	-rm test.db
