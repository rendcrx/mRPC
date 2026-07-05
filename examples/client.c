#define RPC_CLIENT_IMPLEMENT
#include "../rpc.h"

#include <assert.h>
#include <stdio.h>

int main(void)
{
	handle_t handle = rpc_client_init("/test");
	assert(handle);

	void *ptr = rpc_alloc_page(handle);
	assert(ptr);
	
	assert(rpc_push_value(handle, (void *)1) == 0);
	assert(rpc_push_value(handle, (void *)2) == 0);

	char *str = (char *)ptr;
	strcpy(str, "add");

	assert(rpc_push_pointer(handle, str) == 0);

	rpc_client_call(handle);

	rpc_client_spin(handle);

	void *ret;
	assert(rpc_pop_value(handle, &ret) == 0);

	assert(rpc_free(handle, ptr) == 0);

	printf("%lld\n", (int64_t)ret);

	rpc_client_destroy(handle);
}
