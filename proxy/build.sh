gcc -c -Wall -Werror -fpic proxy.c
gcc -shared -o libproxy.so proxy.o
