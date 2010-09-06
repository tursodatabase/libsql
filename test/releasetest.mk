########################################################
TOP=/home/drh/sqlite/sqlite

TCL_FLAGS=-I/home/drh/tcltk/86linux
LIBTCL=/home/drh/tcltk/86linux/libtcl8.6.a -lm -ldl -lpthread

BCC = gcc
TCC = gcc -ansi -g $(CFLAGS)
NAWK   = awk
AR     = ar cr
RANLIB = ranlib
THREADLIB = -lpthread -ldl -lz
include $(TOP)/main.mk
########################################################
