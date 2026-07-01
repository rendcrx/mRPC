all: client server

client: client.c ../rpc.h
	cc -o $@ $<

server: server.c ../rpc.h
	cc -o $@ $<

clean:
	rm -f client server

.PHONY: all clean
