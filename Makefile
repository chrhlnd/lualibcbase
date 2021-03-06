INCLUDE=-I/usr/include -I/usr/local/openresty/luajit/include -I./
LIBDIR=-L/usr/lib64 -L/usr/local/openresty/luajit/lib

CC=gcc
LIBS=-llua -lcouchbase -l:libevent-1.4.so.2
#COPTS=-c -fpic -ggdb
COPTS=-c -fpic -O2
LOPTS=-shared

sources:=$(shell find ./ -name "*.c")
objs:=$(patsubst %.c, %.o, $(sources))

%.o: %.c
	$(CC) $(OPTS) $(COPTS) $(LIBDIR) $(INCLUDE) $< -o $@

libcbase.so: $(objs)
	$(CC) $(LOPTS) $(LIBDIR) $(LIBS) $(objs) -o $@

clean:
	rm $(shell ls *.o)

all: clean libcbase.so

