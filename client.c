#define RPC_CLIENT_IMPLEMENT
#include "rpc.h"

#include <assert.h>
#include <stdio.h>

int main()
{
	void *target = rpc_client_init("/test");
	assert(target);

	char *func = rpc_malloc(64);
	strcpy(func, "add");
	printf("%p: %s\n", func, func);

	RPC_CLIENT_PUSH(target, 1);
	RPC_CLIENT_PUSH(target, 2);
	RPC_CLIENT_PUSH_POINTER(target, func);

	rpc_client_call(target);
	printf("%lld\n", (int64_t)RPC_GET_CALL_VALUE(target));

	rpc_client_destroy(target);
}
