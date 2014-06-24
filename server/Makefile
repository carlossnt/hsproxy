CFLAGS = -g -Wall -O3 #-pg

CC = g++
LIBS = -lpthread /usr/lib/libevent.a -lrt -lyaml-cpp /usr/lib/libdaemon.a #-ldaemon
INCLUDES = -I. -I/usr/include/libmemcached/memcached/
OBJS = hsproxy.o config_manager.o yaml_config_reader.o hs_pool.o mc_listener.o hs_request.o debug.o

all: hsproxy

hsproxy: main.o ${OBJS}
	${CC} ${CFLAGS} ${INCLUDES} -o $@ main.o ${OBJS} ${LIBS}

%.o: %.cpp 
	$(CC) -c -o $@ $< $(CFLAGS) $(INCLUDES)

clean:
	rm -f *.o hsproxy hsproxyd

install:
	mkdir -p ${DESTDIR}/../hsproxy-server/usr/sbin/
	cp hsproxy ${DESTDIR}/../hsproxy-server/usr/sbin/
