#!/usr/bin/make
#
# This is a temporary makefile for use during experimental development.
# Replace with something more portable, if the experiments actually work out.
#
CC     = gcc
CFLAGS =-g -fPIC -Wall -I. -I/home/drh/sqlite/bld

LSMOBJ    = \
  lsm_ckpt.o \
  lsm_file.o \
  lsm_log.o \
  lsm_main.o \
  lsm_mem.o \
  lsm_mutex.o \
  lsm_shared.o \
  lsm_sorted.o \
  lsm_str.o \
  lsm_tree.o \
  lsm_unix.o \
  lsm_varint.o \
  lsm_vtab.o

LSMHDR   = \
  lsm.h \
  lsmInt.h

all: lsm.so

lsm.so:	$(LSMOBJ)
	$(CC) $(CFLAGS) -shared -o lsm.so $(LSMOBJ)

%.o:	%.c $(LSMHDR)
	$(CC) $(CFLAGS) -c $<
