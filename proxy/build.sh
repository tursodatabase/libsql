gcc -Wall -fpic -c proxy.c
gcc -shared -o libproxy.so proxy.o
