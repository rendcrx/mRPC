#ifndef __RPC_H__
#define __RPC_H__

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
#include <errno.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define MAX_ARGS_COUNT 6 
#define RPC_PAGE_SIZE 4096
#define RPC_REGION_MAGIC 0x4d525043u
#define RPC_REGION_VERSION 1u

struct _rpc_shared_region {
	void *base;
	size_t size;
};

struct _rpc_region_header {
	uint32_t magic;
	uint32_t version;
	uint64_t region_size;
	uint64_t call_slot_offset;
	uint64_t pool_offset;
	uint64_t free_list_offset;
};

enum _rpc_stack_state {
	OK,
	ERR_ARGSNO,
	ERR_FUNCNAME,
};

enum _rpc_call_state {
	RPC_CALL_EMPTY,
	RPC_CALL_READY,
	RPC_CALL_RUNNING,
	RPC_CALL_DONE,
	RPC_CALL_ERROR,
};

struct _rpc_call_slot {
	int rpc_errno;
	int top;
	enum _rpc_call_state call_state;
	enum _rpc_stack_state state;
	uint64_t data[MAX_ARGS_COUNT+1];
};

#define RPC_CALL_SLOT(ptr) ((struct _rpc_call_slot *)((uint8_t *)(ptr) + ((struct _rpc_region_header *)(ptr))->call_slot_offset))
#define RPC_STATE_IS_NORMAL(ptr) (RPC_CALL_SLOT(ptr)->state == OK)
#define RPC_CALL_IS_NORMAL(ptr)  (RPC_CALL_SLOT(ptr)->rpc_errno == 0)
#define RPC_GET_CALL_VALUE(ptr)  ((void *)((uintptr_t)RPC_CALL_SLOT(ptr)->data[0]))

static void *_rpc_region_base;
static sem_t *_rpc_request_sem;
static sem_t *_rpc_response_sem;

/*
 * Local backend for the shared-memory region.
 *
 * The RPC protocol only needs a byte-addressable region visible to both peers.
 * POSIX shm is the local test backend; a CXL/UB backend should provide the same
 * base/size contract after mapping a cross-host memory window.
 */
static int _rpc_shared_region_create(struct _rpc_shared_region *region, const char *region_name, size_t size)
{
	int fd;

	shm_unlink(region_name);
	fd = shm_open(region_name, O_CREAT | O_RDWR, 0666);
	if (fd < 0)
		return -1;
	if (ftruncate(fd, size) < 0) {
		close(fd);
		return -1;
	}
	region->base = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (region->base == MAP_FAILED)
		return -1;
	region->size = size;
	return 0;
}

static int _rpc_shared_region_open(struct _rpc_shared_region *region, const char *region_name, size_t size)
{
	int fd;

	fd = shm_open(region_name, O_RDWR);
	if (fd < 0)
		return -1;
	region->base = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (region->base == MAP_FAILED)
		return -1;
	region->size = size;
	return 0;
}

static void _rpc_shared_region_close(struct _rpc_shared_region *region)
{
	if (region->base && region->base != MAP_FAILED)
		munmap(region->base, region->size);
	region->base = NULL;
	region->size = 0;
}

static void _rpc_shared_region_unlink(const char *region_name)
{
	shm_unlink(region_name);
}

static void _rpc_sem_name(char *buf, size_t size, const char *shm_name, const char *suffix)
{
	snprintf(buf, size, "%s_%s", shm_name, suffix);
}

static int _rpc_sem_wait(sem_t *sem)
{
	int ret;
	do {
		ret = sem_wait(sem);
	} while (ret < 0 && errno == EINTR);
	return ret;
}

static void _rpc_sem_close_pair(void)
{
	if (_rpc_request_sem) {
		sem_close(_rpc_request_sem);
		_rpc_request_sem = NULL;
	}
	if (_rpc_response_sem) {
		sem_close(_rpc_response_sem);
		_rpc_response_sem = NULL;
	}
}

static void _rpc_region_format(void *target)
{
	struct _rpc_region_header *header = (struct _rpc_region_header *)target;
	struct _rpc_call_slot *slot;

	header->magic = RPC_REGION_MAGIC;
	header->version = RPC_REGION_VERSION;
	header->region_size = RPC_PAGE_SIZE;
	header->call_slot_offset = sizeof(struct _rpc_region_header);
	header->pool_offset = header->call_slot_offset + sizeof(struct _rpc_call_slot);

	slot = RPC_CALL_SLOT(target);
	slot->rpc_errno = 0;
	slot->top = 0;
	slot->call_state = RPC_CALL_EMPTY;
	slot->state = OK;
	memset(slot->data, 0, sizeof slot->data);
}

static int _rpc_region_is_valid(void *target)
{
	struct _rpc_region_header *header = (struct _rpc_region_header *)target;

	if (header->magic != RPC_REGION_MAGIC || header->version != RPC_REGION_VERSION)
		return 0;
	if (header->region_size != RPC_PAGE_SIZE)
		return 0;
	if (header->call_slot_offset + sizeof(struct _rpc_call_slot) > header->pool_offset)
		return 0;
	if (header->pool_offset >= header->region_size)
		return 0;
	return 1;
}

void _rpc_shared_memory_format(void *target)
{
	struct _rpc_region_header *header = (struct _rpc_region_header *)target;
	uint64_t offset;

	_rpc_region_base = target;
	header->free_list_offset = header->pool_offset;
	for (offset = header->pool_offset; offset + 64 < header->region_size; offset += 64)
		*(uint64_t *)((uint8_t *)target + offset) = offset + 64;
	*(uint64_t *)((uint8_t *)target + offset) = 0;
}

void _rpc_shared_memory_attach(void *target)
{
	_rpc_region_base = target;
}

void *rpc_malloc(size_t size)
{
	struct _rpc_region_header *header = (struct _rpc_region_header *)_rpc_region_base;
	uint64_t offset;

	if (!header || header->free_list_offset == 0)
		return NULL;
	offset = header->free_list_offset;
	header->free_list_offset = *(uint64_t *)((uint8_t *)_rpc_region_base + offset);
	return (uint8_t *)_rpc_region_base + offset;
}

void rpc_free(void *ptr)
{
	struct _rpc_region_header *header = (struct _rpc_region_header *)_rpc_region_base;
	uint64_t offset;

	if (!header || !ptr)
		return;
	offset = (uint64_t)((uintptr_t)ptr - (uintptr_t)_rpc_region_base);
	*(uint64_t *)ptr = header->free_list_offset;
	header->free_list_offset = offset;
}

#endif

#ifdef RPC_CLIENT_IMPLEMENT

#define RPC_CLIENT_PUSH_POINTER(ptr, value)								\
	do {                                                            				\
		struct _rpc_call_slot *stack = RPC_CALL_SLOT(ptr);					\
		if (stack->top > MAX_ARGS_COUNT)                        				\
			stack->state = ERR_ARGSNO;                      				\
		else                                                    				\
			stack->data[stack->top++] = (uint64_t)((uintptr_t)value - (uintptr_t)ptr);	\
	} while (0)

#define RPC_CLIENT_PUSH(ptr, value)									\
	do {                                                            				\
		struct _rpc_call_slot *stack = RPC_CALL_SLOT(ptr);					\
		if (stack->top > MAX_ARGS_COUNT)                        				\
			stack->state = ERR_ARGSNO;                      				\
		else                                                    				\
			stack->data[stack->top++] = (uint64_t)(uintptr_t)(value);    			\
	} while (0)

void *rpc_client_init(const char *shm_name)
{
	struct _rpc_shared_region region;
	char request_sem_name[256];
	char response_sem_name[256];

	if (_rpc_shared_region_open(&region, shm_name, RPC_PAGE_SIZE) < 0)
		return NULL;
	if (!_rpc_region_is_valid(region.base)) {
		_rpc_shared_region_close(&region);
		return NULL;
	}
	_rpc_sem_name(request_sem_name, sizeof request_sem_name, shm_name, "request");
	_rpc_sem_name(response_sem_name, sizeof response_sem_name, shm_name, "response");
	_rpc_request_sem = sem_open(request_sem_name, 0);
	_rpc_response_sem = sem_open(response_sem_name, 0);
	if (_rpc_request_sem == SEM_FAILED || _rpc_response_sem == SEM_FAILED) {
		if (_rpc_request_sem == SEM_FAILED)
			_rpc_request_sem = NULL;
		if (_rpc_response_sem == SEM_FAILED)
			_rpc_response_sem = NULL;
		_rpc_sem_close_pair();
		_rpc_shared_region_close(&region);
		return NULL;
	}
	_rpc_shared_memory_attach(region.base);
	return region.base;
}

void rpc_client_call(void *rpc_server_ptr)
{
	struct _rpc_call_slot *slot = RPC_CALL_SLOT(rpc_server_ptr);

	slot->call_state = RPC_CALL_READY;
	sem_post(_rpc_request_sem);
	_rpc_sem_wait(_rpc_response_sem);
	if (slot->call_state == RPC_CALL_DONE)
		slot->call_state = RPC_CALL_EMPTY;
}

void rpc_client_destroy(void *rpc_server_ptr)
{
	struct _rpc_shared_region region = { rpc_server_ptr, RPC_PAGE_SIZE };

	_rpc_sem_close_pair();
	_rpc_shared_region_close(&region);
}

#endif /* RPC_CLIENT_IMPLEMENT */

#ifdef RPC_SERVER_IMPLEMENT

#define RPC_CLEAR_STACK_ERROR(stack) (RPC_CALL_SLOT(stack)->state = OK)
#define RPC_STACK_STATE_NORMAL(stack) (RPC_CALL_SLOT(stack)->state == OK)
#define RPC_CLEAR_STACK_DATA(stack) (RPC_CALL_SLOT(stack)->top = 0)
#define RPC_CALL_ARGS0(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[0]))
#define RPC_CALL_ARGS1(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[1]))
#define RPC_CALL_ARGS2(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[2]))
#define RPC_CALL_ARGS3(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[3]))
#define RPC_CALL_ARGS4(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[4]))
#define RPC_CALL_ARGS5(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[5]))
#define RPC_CALL_ARGS6(stack) ((void *)(uintptr_t)(RPC_CALL_SLOT(stack)->data[6]))
#define RPC_CALL_PUSH_VALUE(stack, value) (RPC_CALL_SLOT(stack)->data[0] = (uint64_t)(uintptr_t)(value))

#define RPC_SERVER_POP_POINTER(stack)								\
	({                                                                              	\
		 struct _rpc_call_slot *slot = RPC_CALL_SLOT(stack);                   	\
		 void *ret = NULL;                                                      	\
		 if (slot->top <= 0)                                                    	\
			 slot->state = ERR_ARGSNO;                                      	\
		 else                                                                   	\
			 ret = (void *)((uintptr_t)stack + (uintptr_t)(slot->data[--slot->top]));	\
		 ret;										\
	 })

#define RPC_SERVER_POP(stack)									\
	({                                                   					\
		struct _rpc_call_slot *slot = RPC_CALL_SLOT(stack);					\
		void *ret = NULL;                            					\
		if (slot->top <= 0)                       					\
			slot->state = ERR_ARGSNO;         					\
		else                                         					\
			ret = (void *)(uintptr_t)(slot->data[--slot->top]);				\
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
	struct _rpc_shared_region region;
	char request_sem_name[256];
	char response_sem_name[256];

	if (_rpc_shared_region_create(&region, shm_name, RPC_PAGE_SIZE) < 0)
		return NULL;
	_rpc_region_format(region.base);
	_rpc_sem_name(request_sem_name, sizeof request_sem_name, shm_name, "request");
	_rpc_sem_name(response_sem_name, sizeof response_sem_name, shm_name, "response");
	sem_unlink(request_sem_name);
	sem_unlink(response_sem_name);
	_rpc_request_sem = sem_open(request_sem_name, O_CREAT, 0666, 0);
	_rpc_response_sem = sem_open(response_sem_name, O_CREAT, 0666, 0);
	if (_rpc_request_sem == SEM_FAILED || _rpc_response_sem == SEM_FAILED) {
		if (_rpc_request_sem == SEM_FAILED)
			_rpc_request_sem = NULL;
		if (_rpc_response_sem == SEM_FAILED)
			_rpc_response_sem = NULL;
		_rpc_sem_close_pair();
		_rpc_shared_region_close(&region);
		_rpc_shared_region_unlink(shm_name);
		return NULL;
	}
	_rpc_shared_memory_format(region.base);
	return region.base;
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
	struct _rpc_call_slot *slot = RPC_CALL_SLOT(rpc_shared_ptr);
	for (;;) {
		if (_rpc_sem_wait(_rpc_request_sem) < 0)
			continue;
		if (slot->call_state == RPC_CALL_READY && RPC_STACK_STATE_NORMAL(rpc_shared_ptr)) {
			slot->call_state = RPC_CALL_RUNNING;
			fn = (int (*)())_rpc_func_getptr((const char *)RPC_SERVER_POP_POINTER(rpc_shared_ptr));
			if (fn) {
				slot->rpc_errno = fn();
				RPC_CLEAR_STACK_DATA(rpc_shared_ptr);
			} else
				slot->state = ERR_FUNCNAME;
			slot->call_state = RPC_CALL_DONE;
			sem_post(_rpc_response_sem);
		} else if (slot->call_state == RPC_CALL_READY) {
			slot->call_state = RPC_CALL_ERROR;
			sem_post(_rpc_response_sem);
		}
	}
}

void rpc_server_destroy(void *rpc_shared_ptr, const char *shm_name)
{
	struct _rpc_shared_region region = { rpc_shared_ptr, RPC_PAGE_SIZE };
	char request_sem_name[256];
	char response_sem_name[256];
	free(_rpc_funcs_list->funcs);
	free(_rpc_funcs_list);
	_rpc_funcs_list = NULL;
	_rpc_sem_name(request_sem_name, sizeof request_sem_name, shm_name, "request");
	_rpc_sem_name(response_sem_name, sizeof response_sem_name, shm_name, "response");
	_rpc_sem_close_pair();
	sem_unlink(request_sem_name);
	sem_unlink(response_sem_name);
	_rpc_shared_region_close(&region);
	_rpc_shared_region_unlink(shm_name);
}

#endif /* RPC_SERVER_IMPLEMENT */

#ifdef __cplusplus
}
#endif

#endif /* __RPC_H__ */
