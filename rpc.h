#ifndef __RPC_H__
#define __RPC_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef void *handle_t;
typedef struct _rpc_funcs *funclist_t;

/* Client */
handle_t  rpc_client_init(const char *shm_name);
void     *rpc_alloc_page(handle_t handle);
int       rpc_free(handle_t handle, void *ptr);
void      rpc_client_call(handle_t handle);
void      rpc_client_spin(handle_t);
void      rpc_client_wait(handle_t);
void      rpc_client_destroy(handle_t handle);

/* Server */
handle_t  rpc_server_init(const char *shm_name, void (*handler)(handle_t));
int       rpc_server_push_pointer(handle_t handle, void *v);
int       rpc_server_pop_pointer(handle_t handle, void **v);
void      rpc_server_return(handle_t handle);
void      rpc_server_destroy(handle_t handle, const char *shm_name);

/* Stack */
int       rpc_push_value(handle_t handle, void *v);
int       rpc_push_pointer(handle_t handle, void *v);
int       rpc_pop_value(handle_t handle, void **r);
int       rpc_pop_pointer(handle_t handle, void **r);

/* Auxiliary */
funclist_t rpcL_funclist_init();
void       rpcL_funcregister(funclist_t funclist, const char *func_name, void (*func_ptr)(handle_t));
void       (*rpcL_get_funcptr(funclist_t funclist, const char *func_name))(handle_t);
void       rpcL_funclist_destroy(funclist_t funclist);

#if defined(RPC_CLIENT_IMPLEMENT) || defined(RPC_SERVER_IMPLEMENT)

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdatomic.h>

#define RPC_STACK_DEFAULT       6
#define RPC_MEMO_SIZE           (4 * RPC_PAGE_SIZE)
#define RPC_STACKS_SIZE         RPC_PAGE_SIZE
#define RPC_PAGE_SIZE           4096

#define RPC_STACK_VALID_IDLE    0
#define RPC_STACK_VALID_WORK    1

#define RPC_STACK_STATE_IDLE    0
#define RPC_STACK_STATE_CALL    1
#define RPC_STACK_STATE_ALLOC   2
#define RPC_STACK_STATE_FREE    3
#define RPC_STACK_STATE_RETURN  4
#define RPC_STACK_STATE_PROCESS 5

struct _rpc_stack {
	atomic_int valid;
	void *origin_ptr;
	int state;
	int top;
	void *stack[RPC_STACK_DEFAULT];
};

int rpc_push_value(handle_t handle, void *v)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top >= RPC_STACK_DEFAULT)
		return 1;
	else {
		s->stack[s->top++] = v;
		return 0;
	}
}

int rpc_push_pointer(handle_t handle, void *v)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top >= RPC_STACK_DEFAULT)
		return 1;
	else {
		s->stack[s->top++] = (void *)((uintptr_t)v - (uintptr_t)s->origin_ptr);
		return 0;
	}
}

int rpc_pop_value(handle_t handle, void **r)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top <= 0)
		return 1;
	else {
		*r = s->stack[--s->top];
		return 0;
	}
}

int rpc_pop_pointer(handle_t handle, void **r)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top <= 0)
		return 1;
	else {
		*r = (void *)((ptrdiff_t)(s->stack[--s->top]) + (uintptr_t)s->origin_ptr);
		return 0;
	}
}

#endif /* RPC_CLIENT_IMPLEMENT || RPC_SERVER_IMPLEMENT */

#ifdef RPC_SERVER_IMPLEMENT

#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

struct task_t {
	void *arg;
	void (*func)(handle_t);
	struct task_t *next;
};

struct pthread_pool_t {
	int stop;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct task_t *tasks_head;
	struct task_t *tasks_tail;
	pthread_t threads[0];
};

static struct pthread_pool_t *pool;
static long cores;

static void *worker(void *arg)
{
	struct task_t *task;
	while (1) {
		pthread_mutex_lock(&pool->lock);
		if (pool->stop) {
			pthread_mutex_unlock(&pool->lock);
			break;
		}
		while (pool->tasks_head == NULL && !pool->stop)
			pthread_cond_wait(&pool->cond, &pool->lock);
		if (pool->stop) {
			pthread_mutex_unlock(&pool->lock);
			break;
		}
		task = pool->tasks_head;
		pool->tasks_head = task->next;
		if (!pool->tasks_head)
			pool->tasks_tail = NULL;
		pthread_mutex_unlock(&pool->lock);
		task->func(task->arg);
		free(task);
	}
	return NULL;
}

static void pthread_pool_create()
{
	int i;
	cores = sysconf(_SC_NPROCESSORS_ONLN) - 1;
	if (cores <= 0) cores = 8;
	pool = malloc(sizeof *pool + sizeof(pthread_t) * cores);
	pool->stop = 0;
	pthread_mutex_init(&pool->lock, NULL);;
	pthread_cond_init(&pool->cond, NULL);;
	pool->tasks_head = NULL;
	pool->tasks_tail = NULL;
	for (i = 0; i < cores; ++i)
		pthread_create(&pool->threads[i], NULL, worker, NULL);
}

void task_commit(void *arg, void (*f)(handle_t))
{
	struct task_t *task;
	task = malloc(sizeof *task);
	task->arg = arg;
	task->func = f;
	task->next = NULL;
	pthread_mutex_lock(&pool->lock);
	if (pool->tasks_head == NULL)
		pool->tasks_head = pool->tasks_tail = task;
	else {
		pool->tasks_tail->next = task;
		pool->tasks_tail = task;
	}
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->lock);
}

static void pthread_pool_destroy()
{
	int i;
	struct task_t *task, *tmp;
	pthread_mutex_lock(&pool->lock);
	pool->stop = 1;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->lock);
	for (i = 0; i < cores; ++i)
		pthread_join(pool->threads[i], NULL);
	for (task = pool->tasks_head; task;) {
		tmp = task;
		task = task->next; 
		free(tmp);
	}
	free(pool);
	pool = NULL;
}

static pthread_mutex_t freelist_lock = PTHREAD_MUTEX_INITIALIZER;
static void *freelist, *head, *tail;
static pthread_t master;
static int workstop;

static void _rpc_alloc(handle_t handle)
{
	void *ptr;
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	pthread_mutex_lock(&freelist_lock);
	ptr = freelist;
	if (freelist)
		freelist = *(void **)freelist;
	pthread_mutex_unlock(&freelist_lock);
	rpc_push_value(s, (void *)((uintptr_t)ptr - (uintptr_t)head));
	rpc_server_return(handle);
}

void rpc_server_return(handle_t handle)
{
	((struct _rpc_stack *)handle)->state = RPC_STACK_STATE_RETURN;
	// msync((void *)handle, sizeof(struct _rpc_stack), MS_ASYNC);
}

static void _rpc_free(handle_t handle)
{
	void *ptr;
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	rpc_pop_value(s, &ptr);
	ptr = (void *)((uintptr_t)ptr + (uintptr_t)head);
	pthread_mutex_lock(&freelist_lock);
	*(void **)ptr = freelist;
	freelist = ptr;
	pthread_mutex_unlock(&freelist_lock);
	rpc_server_return(handle);
}

static void *coordinate(void *f_handler)
{
	void *queue_ptr;
	struct _rpc_stack *s;
	pthread_pool_create();
	for (;;) {
		for (queue_ptr = head;
		     queue_ptr < tail;
		     queue_ptr = (void *)((uintptr_t)queue_ptr + sizeof(struct _rpc_stack))) {
			if (workstop)
				goto end;
			s = (struct _rpc_stack *)queue_ptr;
			switch (s->state) {
			case RPC_STACK_STATE_IDLE:
				break;
			case RPC_STACK_STATE_CALL:
				s->state = RPC_STACK_STATE_PROCESS;
				task_commit((void *)s, f_handler); 
				break;
			case RPC_STACK_STATE_ALLOC:
				s->state = RPC_STACK_STATE_PROCESS;
				task_commit((void *)s, _rpc_alloc); 
				break;
			case RPC_STACK_STATE_FREE:
				s->state = RPC_STACK_STATE_PROCESS;
				task_commit((void *)s, _rpc_free); 
				break;
			}
		}
	}
end:
	pthread_pool_destroy();
	return NULL;
}

handle_t rpc_server_init(const char *shm_name, void (*f_handler)(handle_t))
{
	int fd;
	struct _rpc_stack *ptr;
	void *start, *end;
	fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
	ftruncate(fd, RPC_MEMO_SIZE);
	if (fd < 0)
		return NULL;
	ptr = (struct _rpc_stack *)mmap(NULL, RPC_MEMO_SIZE, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (ptr == MAP_FAILED)
		return NULL;
	freelist = NULL;
	head = ptr;
	start = (void *)((uintptr_t)ptr + RPC_STACKS_SIZE);
	end = (void *)((uintptr_t)ptr + RPC_MEMO_SIZE);
	for (; (void *)(ptr + 1) <= start; ++ptr) {
		atomic_init(&ptr->valid, RPC_STACK_VALID_IDLE);
		ptr->state = RPC_STACK_STATE_IDLE;
		ptr->top = 0;
	}
	tail = (void *)ptr;
	for (; (void *)((uintptr_t)start + RPC_PAGE_SIZE) <= end; start = (void *)((uintptr_t)start + RPC_PAGE_SIZE)) {
		*(void **)start = freelist;
		freelist = start;
	}
	workstop = 0;
	pthread_create(&master, NULL, coordinate, (void *)f_handler);
	return ptr;
}

// void rpc_server_run(void *rpc_shared_ptr)
// {
// 	void (*fn)(void);
// 	struct _rpc_stack *stack = (struct _rpc_stack *)rpc_shared_ptr;
// 	for (;;) {
// 		while (stack->call_flag && RPC_STACK_STATE_NORMAL(stack)) {
// 			fn = _rpc_func_getptr(stack->func);
// 			if (fn) {
// 				stack->errno = fn();
// 				RPC_CLEAR_STACK_DATA(stack);
// 			} else
// 				stack->state = ERR_FUNCNAME;
// 			stack->call_flag = 0;
// 		}
// 	}
// }

void rpc_server_destroy(handle_t handle, const char *shm_name)
{
	workstop = 1;
	pthread_join(master, NULL);
	munmap(((struct _rpc_stack *)handle)->origin_ptr, RPC_MEMO_SIZE);
	shm_unlink(shm_name);
}

int rpc_server_push_pointer(handle_t handle, void *v)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top >= RPC_STACK_DEFAULT)
		return 1;
	else {
		s->stack[s->top++] = (void *)((uintptr_t)v - (uintptr_t)head);
		return 0;
	}
}

int rpc_server_pop_pointer(handle_t handle, void **v)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->top <= 0)
		return 1;
	else {
		*v = (void *)((ptrdiff_t)(s->stack[--s->top]) + (uintptr_t)head);
		return 0;
	}
}

#endif /* RPC_SERVER_IMPLEMENT */

#ifdef RPC_CLIENT_IMPLEMENT

handle_t rpc_client_init(const char *shm_name)
{
	int fd;
	void *ptr;
	fd = shm_open(shm_name, O_RDWR);
	if (fd < 0)
		return NULL;
	ptr = mmap(NULL, RPC_MEMO_SIZE, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0);
	close(fd);
	if (ptr == MAP_FAILED)
		return NULL;
	for (struct _rpc_stack *s = (struct _rpc_stack *)ptr, *e = (struct _rpc_stack *)((uintptr_t)ptr + RPC_MEMO_SIZE);
	     s + 1 <=e;
	     ++s) {
		if (atomic_exchange(&s->valid, RPC_STACK_VALID_WORK) == RPC_STACK_VALID_IDLE) {
			s->origin_ptr = ptr;
			return s;
		}
	}
	munmap(ptr, RPC_MEMO_SIZE);
	return NULL;
}

void *rpc_alloc_page(handle_t handle)
{
	void *ret;
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->state != RPC_STACK_STATE_IDLE)
		return NULL;
	s->state = RPC_STACK_STATE_ALLOC;
	while (s->state != RPC_STACK_STATE_RETURN)
		;
	s->state = RPC_STACK_STATE_IDLE;
	if (rpc_pop_pointer(handle, &ret) != 0)
		return NULL;
	return ret;
}

int rpc_free(handle_t handle, void *ptr)
{
	void *ret;
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->state != RPC_STACK_STATE_IDLE)
		return 1;
	rpc_push_pointer(s, ptr);
	s->state = RPC_STACK_STATE_FREE;
	while (s->state != RPC_STACK_STATE_RETURN)
		;
	s->state = RPC_STACK_STATE_IDLE;
	return 0;
}

void rpc_client_call(handle_t handle)
{
	struct _rpc_stack *s;
	s = (struct _rpc_stack *)handle;
	if (s->state != RPC_STACK_STATE_IDLE)
		return;
	s->state = RPC_STACK_STATE_CALL;
}

void rpc_client_spin(handle_t handle)
{
	while (((struct _rpc_stack *)handle)->state != RPC_STACK_STATE_RETURN)
		;
	((struct _rpc_stack *)handle)->state = RPC_STACK_STATE_IDLE;
}

void rpc_client_wait(handle_t handle)
{
	// TODO
}

void rpc_client_destroy(handle_t handle)
{
	munmap(((struct _rpc_stack *)handle)->origin_ptr, RPC_MEMO_SIZE);
}

#endif /* RPC_CLIENT_IMPLEMENT */

#ifdef RPC_AUXILIARY_IMPLEMENT

struct rpc_func_unit {
	const char *func_name;
	void (*func_ptr)(handle_t);
	uint32_t func_object;
};

struct _rpc_funcs {
	size_t size;
	size_t len;
	struct rpc_func_unit *funcs;
};

funclist_t rpcL_funclist_init()
{
	funclist_t funclist;
	size_t size = 8;
	funclist = (funclist_t)malloc(sizeof *funclist);
	funclist->size = size;
	funclist->len = 0;
	funclist->funcs = (struct rpc_func_unit *)malloc(size * sizeof(struct rpc_func_unit));
	return funclist;
}

static uint32_t _rpcL_get_funcobject(const char *func_name)
{
	uint32_t hash;
	const uint8_t *bytes;
	uint8_t byte;
	bytes = (const uint8_t *)func_name;
	hash = 0x811c9dc5UL;
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

void rpcL_funcregister(funclist_t funclist, const char *func_name, void (*func_ptr)(handle_t))
{
	size_t size;
	uint32_t object;
	struct rpc_func_unit *unit;
	if (!funclist)
		return;
	if (funclist->len == funclist->size) {
		funclist->size <<= 1;
		funclist->funcs = (struct rpc_func_unit *)realloc(funclist->funcs, sizeof *unit * funclist->size);
	}
	object = _rpcL_get_funcobject(func_name);
	unit = &funclist->funcs[funclist->len++];
	unit->func_name = func_name;
	unit->func_ptr = func_ptr;
	unit->func_object = object;
}

void (*rpcL_get_funcptr(funclist_t funclist, const char *func_name))(handle_t)
{
	uint32_t object;
	struct rpc_func_unit *unit;
	object = _rpcL_get_funcobject(func_name);
	int i;
	for (i = 0; i < funclist->len; ++i) {
		unit = &funclist->funcs[i];
		if (object == unit->func_object && strcmp(func_name, unit->func_name) == 0)
			return unit->func_ptr;
	}
	return NULL;
}

void rpcL_funclist_destroy(funclist_t funclist)
{
	if (funclist) {
		free(funclist->funcs);
		free(funclist);
	}
}

#endif /* RPC_AUXILIARY_IMPLEMENT */

#ifdef __cplusplus
}
#endif

#endif /* __RPC_H__ */
