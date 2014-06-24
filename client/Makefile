CFLAGS = -g -Wall -O3 #-pg

CC = g++
LIBS = 
INCLUDES = -I. -I/usr/include/libmemcached/memcached/

all: client_ascii client_binary latency_test test_stats

client_ascii: client_ascii.cpp
	${CC} ${CFLAGS} ${INCLUDES} -o $@ client_ascii.cpp

client_binary: client_binary.cpp
	${CC} ${CFLAGS} ${INCLUDES} -o $@ client_binary.cpp

latency_test: latency_test.cpp
	${CC} ${CFLAGS} ${INCLUDES} -o $@ latency_test.cpp

test_stats: test_stats.cpp
	${CC} ${CFLAGS} ${INCLUDES} -o $@ test_stats.cpp

clean:
	rm -f client_ascii latency_test client_binary test_stats

install:
	 mkdir -p ${DESTDIR}/../hsproxy-client/usr/bin/
	 cp client_ascii ${DESTDIR}/../hsproxy-client/usr/bin/hsclient
