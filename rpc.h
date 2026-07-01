#ifndef __RPC_SERVER_H__
#define __RPC_SERVER_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

void *rpc_client_init(const char *shm_name);
void  rpc_client_call(void *rpc_server_ptr);
void *rpc_malloc(size_t size);
void  rpc_free(void *ptr);
void  rpc_client_destroy(void *rpc_server_ptr);

void *rpc_server_init(const char *shm_name);
void  rpc_server_func_register(const char *func_name, void *func_ptr);
void  rpc_server_run(void *rpc_shared_ptr);
void  rpc_server_destroy(void *rpc_shared_ptr, const char *shm_name);

#if defined(RPC_CLIENT_IMPLEMENT) || defined(RPC_SERVER_IMPLEMENT)

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define MAX_ARGS_COUNT 6 
#define RPC_PAGE_SIZE 4096

enum _rpc_stack_state {
	OK,
	ERR_ARGSNO,
	ERR_FUNCNAME,
};

struct _rpc_stack {
	int errno;
	int top;
	int call_flag;
	enum _rpc_stack_state state;
	void *data[MAX_ARGS_COUNT+1];
};

#define RPC_STATE_IS_NORMAL(ptr) (((struct _rpc_stack *)ptr)->state == OK)
#define RPC_CALL_IS_NORMAL(ptr)  (((struct _rpc_stack *)ptr)->errno == 0)
#define RPC_GET_CALL_VALUE(ptr)  (((struct _rpc_stack *)ptr)->data[0])

static void *_rpc_shared_memory_pool;

void _rpc_shared_memory_init(void *target)
{
	_rpc_shared_memory_pool = (void *)(((struct _rpc_stack *)target) + 1);
	uint8_t *start = (uint8_t *)_rpc_shared_memory_pool;
	uint8_t *end = (uint8_t *)target + RPC_PAGE_SIZE;
	for (; start + 64 < end; start = start + 64)
		*(void **)start = (void *)(start + 64);
	*(void **)start = NULL;
}

void *rpc_malloc(size_t size)
{
	void *ret = NULL;
	if (_rpc_shared_memory_pool) {
		ret = _rpc_shared_memory_pool;
		_rpc_shared_memory_pool = *(void **)_rpc_shared_memory_pool;
	}
	return ret;
}

void rpc_free(void *ptr)
{
	*(void **)ptr = _rpc_shared_memory_pool;
	_rpc_shared_memory_pool = ptr;
}

#endif

#ifdef RPC_CLIENT_IMPLEMENT

#define RPC_CLIENT_PUSH_POINTER(ptr, value)								\
	do {                                                            				\
		struct _rpc_stack *stack = (struct _rpc_stack *)(ptr);					\
		if (stack->top > MAX_ARGS_COUNT)                        				\
			stack->state = ERR_ARGSNO;                      				\
		else                                                    				\
			stack->data[stack->top++] = (void *)((uintptr_t)value - (uintptr_t)ptr);	\
	} while (0)

#define RPC_CLIENT_PUSH(ptr, value)									\
	do {                                                            				\
		struct _rpc_stack *stack = (struct _rpc_stack *)(ptr);					\
		if (stack->top > MAX_ARGS_COUNT)                        				\
			stack->state = ERR_ARGSNO;                      				\
		else                                                    				\
			stack->data[stack->top++] = (void *)(value);    				\
	} while (0)

void *rpc_client_init(const char *shm_name)
{
	int fd;
	void *ptr;
	fd = shm_open(shm_name, O_RDWR);
	if (fd < 0)
		return NULL;
	ptr = mmap(NULL, RPC_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (!ptr)
		return NULL;
	_rpc_shared_memory_init(ptr);
	return ptr;
}

void rpc_client_call(void *rpc_server_ptr)
{
	struct _rpc_stack *ptr = (struct _rpc_stack *)(rpc_server_ptr);
	ptr->call_flag = 1;
	while (ptr->call_flag)
		;
}

void rpc_client_destroy(void *rpc_server_ptr)
{
	munmap(rpc_server_ptr, RPC_PAGE_SIZE);
}

#endif /* RPC_CLIENT_IMPLEMENT */

#ifdef RPC_SERVER_IMPLEMENT

#define RPC_CLEAR_STACK_ERROR(stack) (((struct _rpc_stack *)(stack))->state = OK)
#define RPC_STACK_STATE_NORMAL(stack) (((struct _rpc_stack *)(stack))->state == OK)
#define RPC_CLEAR_STACK_DATA(stack) (((struct _rpc_stack *)(stack))->top = 0)
#define RPC_CALL_ARGS0(stack) (((struct _rpc_stack *)(stack))->data[0])
#define RPC_CALL_ARGS1(stack) (((struct _rpc_stack *)(stack))->data[1])
#define RPC_CALL_ARGS2(stack) (((struct _rpc_stack *)(stack))->data[2])
#define RPC_CALL_ARGS3(stack) (((struct _rpc_stack *)(stack))->data[3])
#define RPC_CALL_ARGS4(stack) (((struct _rpc_stack *)(stack))->data[4])
#define RPC_CALL_ARGS5(stack) (((struct _rpc_stack *)(stack))->data[5])
#define RPC_CALL_ARGS6(stack) (((struct _rpc_stack *)(stack))->data[6])
#define RPC_CALL_PUSH_VALUE(stack, value) (((struct _rpc_stack *)(stack))->data[0] = (void *)(value))

#define RPC_SERVER_POP_POINTER(stack)								\
	({                                                                              	\
		 void *ret = NULL;                                                      	\
		 if ((stack)->top <= 0)                                                 	\
			 (stack)->state = ERR_ARGSNO;                                   	\
		 else                                                                   	\
			 ret = (ptrdiff_t)((stack)->data[--(stack)->top]) + (void *)stack;	\
		 ret;										\
	 })

#define RPC_SERVER_POP(stack)									\
	({                                                   					\
		void *ret = NULL;                            					\
		if ((stack)->top <= 0)                       					\
			(stack)->state = ERR_ARGSNO;         					\
		else                                         					\
			ret = (stack)->data[--(stack)->top];					\
		ret;                                            				\
	})

struct rpc_func_unit {
	const char *func_name;
	const void *func_ptr;
	uint32_t func_object;
};

struct _rpc_funcs {
	size_t size;
	size_t len;
	struct rpc_func_unit *funcs;
};

#define FUNCS_INITIAL_SIZE 8

static struct _rpc_funcs *_rpc_funcs_list;

void *rpc_server_init(const char *shm_name)
{
	int fd;
	struct _rpc_stack *ptr;
	fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
	ftruncate(fd, RPC_PAGE_SIZE);
	if (fd < 0)
		return NULL;
	ptr = (struct _rpc_stack *)mmap(NULL, RPC_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (ptr == MAP_FAILED)
		return NULL;
	ptr->errno = 0;
	ptr->top = 0;
	ptr->call_flag = 0;
	ptr->state = OK;
	_rpc_shared_memory_init(ptr);
	return ptr;
}

static uint32_t _func_object(const char *func_name)
{
	uint32_t hash;
	const uint8_t *bytes;
	uint8_t byte;
	hash = 0x811c9dc5UL;
	bytes = (const uint8_t *)func_name;
	do {
		byte = *bytes++;
		if (byte) {
			hash ^= byte;
			hash *= 0x01000193UL;
		} else
			break;
	} while (1);
	return hash;
}

void rpc_server_func_register(const char *func_name, void *func_ptr)
{
	size_t size;
	uint32_t object;
	struct rpc_func_unit *unit;
	if (!_rpc_funcs_list) {
		_rpc_funcs_list = (struct _rpc_funcs *)malloc(sizeof *_rpc_funcs_list);
		_rpc_funcs_list->size = FUNCS_INITIAL_SIZE;
		_rpc_funcs_list->len = 0;
		_rpc_funcs_list->funcs = (struct rpc_func_unit *)
			malloc(sizeof(struct rpc_func_unit) * FUNCS_INITIAL_SIZE);
	}

	if (_rpc_funcs_list->len == _rpc_funcs_list->size) {
		_rpc_funcs_list->size <<= 1;
		_rpc_funcs_list->funcs = (struct rpc_func_unit *)
			realloc(_rpc_funcs_list->funcs,
				sizeof(struct rpc_func_unit) * _rpc_funcs_list->size);
	}
	object = _func_object(func_name);
	unit = &_rpc_funcs_list->funcs[_rpc_funcs_list->len++];
	unit->func_name = func_name;
	unit->func_ptr = func_ptr;
	unit->func_object = object;
}

static const void *_rpc_func_getptr(const char *func_name)
{
	uint32_t object;
	struct rpc_func_unit *unit;
	object = _func_object(func_name);
	int i;
	for (i = 0; i < _rpc_funcs_list->len; ++i) {
		unit = &_rpc_funcs_list->funcs[i];
		if (object == unit->func_object && strcmp(func_name, unit->func_name) == 0)
			return unit->func_ptr;
	}
	return NULL;
}

void rpc_server_run(void *rpc_shared_ptr)
{
	int (*fn)();
	struct _rpc_stack *stack = (struct _rpc_stack *)rpc_shared_ptr;
	for (;;) {
		while (stack->call_flag && RPC_STACK_STATE_NORMAL(stack)) {
			fn = (int (*)())_rpc_func_getptr((const char *)RPC_SERVER_POP_POINTER(stack));
			if (fn) {
				stack->errno = fn();
				RPC_CLEAR_STACK_DATA(stack);
			} else
				stack->state = ERR_FUNCNAME;
			stack->call_flag = 0;
		}
	}
}

void rpc_server_destroy(void *rpc_shared_ptr, const char *shm_name)
{
	free(_rpc_funcs_list->funcs);
	free(_rpc_funcs_list);
	_rpc_funcs_list = NULL;
	munmap(rpc_shared_ptr, RPC_PAGE_SIZE);
	shm_unlink(shm_name);
}

#endif /* RPC_SERVER_IMPLEMENT */

#ifdef __cplusplus
}
#endif

#endif // __RPC_SERVER_H__
