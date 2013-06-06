INCLUDE=-I/usr/include -I/usr/local/openresty/luajit/include -I./
LIBDIR=-L/usr/lib64 -L/usr/local/openresty/luajit/lib

CC=gcc
LIBS=-llua -lcouchbase -levent
COPTS=-c -fpic -ggdb
LOPTS=-shared

sources:=$(shell find ./ -name "*.c")
objs:=$(patsubst %.c, %.o, $(sources))

%.o: %.c
	$(CC) $(OPTS) $(COPTS) $(LIBDIR) $(INCLUDE) $< -o $@

libcbase.so: $(objs)
	$(CC) $(OPTS) $(LOPTS) $(LIBDIR) $(INCLUDE) $(LIBS) $(objs) -o $@

clean:
	rm $(shell ls *.o)

all: libcbase.so

