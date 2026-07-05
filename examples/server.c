#define RPC_AUXILIARY_IMPLEMENT
#define RPC_SERVER_IMPLEMENT
#include "../rpc.h"

#include <assert.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>

handle_t handle;

void add(handle_t handle)
{
	struct _rpc_stack *s = (struct _rpc_stack *)handle;

	int arg1;
	int arg2;

	assert(rpc_pop_value(handle, (void *)&arg2) == 0);
	assert(rpc_pop_value(handle, (void *)&arg1) == 0);

	int ret = arg1 + arg2;
	assert(rpc_push_value(handle, (void *)(uintptr_t)(ret)) == 0);
}

void my_clean_void(void)
{
	if (handle)
		rpc_server_destroy(handle, "/test");
	handle = NULL;
}

void my_clean(int sig)
{
	if (handle)
		rpc_server_destroy(handle, "/test");
	handle = NULL;
}

funclist_t funclist;

void doit(handle_t handle)
{
	void *ret;
	assert(rpc_server_pop_pointer(handle, &ret) == 0);
	void (*f)(handle_t) = rpcL_get_funcptr(funclist, (const char *)ret);
	assert(f);
	f(handle);
	rpc_server_return(handle);
}

int main(void)
{
	signal(SIGINT, my_clean);
	atexit(my_clean_void);
	funclist = rpcL_funclist_init();
	rpcL_funcregister(funclist, "add", add);
	handle = rpc_server_init("/test", doit);
	assert(handle);

	/* Do other things or sleep */
	while (handle) ;
}
