#define RPC_SERVER_IMPLEMENT
#include "rpc.h"

#include <assert.h>
#include <stdio.h>

void *handle;

int add()
{
	int64_t arg1 = (int64_t)RPC_CALL_ARGS0(handle);
	int64_t arg2 = (int64_t)RPC_CALL_ARGS1(handle);
	RPC_CALL_PUSH_VALUE(handle, arg1 + arg2);
	return 0;
}

void my_clean()
{
	if (handle)
		rpc_server_destroy(handle, "/test");
	handle = NULL;
}

int main()
{
	signal(SIGINT, my_clean);
	atexit(my_clean);
	handle = rpc_server_init("/test");
	assert(handle);
	rpc_server_func_register("add", add);
	rpc_server_run(handle);
}
