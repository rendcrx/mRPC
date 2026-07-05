all: client server

client: client.c ../rpc.h
	cc -g -o $@ $<

server: server.c ../rpc.h
	cc -g -o $@ $<

clean:
	rm -f client server

.PHONY: all clean
