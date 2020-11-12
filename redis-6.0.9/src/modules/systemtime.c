#include <time.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include "../redismodule.h"

static int setup_io_multiplexer();
static int setup_timer();
static int start_sys_time_collector_thread();

static int s_epoll_fd = 0;
static int s_timer_fd = 0;
static const int NANO_IN_SEC = 1000000000;
static const int MILLI_IN_SEC = 1000;
static long s_system_time_usec = 0;
static double jiffi_in_seconds = 0.0;
#ifdef __USE_MUTEX
static pthread_mutex_t lock;
#endif

/*
* DRIVENETS.GET.TIME 
* this is an implementation of an efficient get-system-time mechanism
* the idea is to use an internal thread that will "collect" the time
* passing into a static register. once a time sample is required, the
* command api will sample _the register_ without calling an expensive
* system call, or probing some hardware clock.
* 
* as there is an inherant race on the above register,
* there are 2 modes with which this can be compiled:
* 1. -D__USE_MUTEX: uses a pthreads mutex.
* 2. -D__USE_ATOMICS: uses a gcc builtin atomic operation
*
* the user can choose the format of the output:
* 1. -D__USE_HUMAN_TIME: will format the timestamp to a humanly readable
*    string. _note_: this option is expensive and "losy" as it looses percision
*    when it cuts the timestamp down to seconds (the base needed by the
*    formating functions)
* 2. none: will return the full timestamp in nanos
*
* profiling numbers are given for each build permutation in: profile_results
*/
int DrivenetsGetTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
	
	// dont hold the lock for the ipc
	long system_time_usec = 0;
#ifdef __USE_MUTEX
	pthread_mutex_lock(&lock);
	system_time_usec = s_system_time_usec;
	pthread_mutex_unlock(&lock);
#elif __USE_ATOMICS
	__atomic_store(&system_time_usec, &s_system_time_usec, __ATOMIC_SEQ_CST);
#endif

	///// send the result back.
#ifdef __USE_HUMAN_TIME	
	time_t time = system_time_usec / NANO_IN_SEC;
	RedisModule_ReplyWithCString(ctx, asctime(localtime(&time)));
#else
	RedisModule_ReplyWithLongLong(ctx, system_time_usec);
#endif

	return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    // we first setup the timing mechanism, as registering with redis
    // means we are "open for business" which is not true yet

    // setup io multiplexer
    if (0 != setup_io_multiplexer()){
		printf("setting up the io multiplexer failed.\n");
        return REDISMODULE_ERR;
    }

    // setup time and timer related stuff
    // note: timer is _not_ armed here!
    if (0 != setup_timer()){
        printf("setting up timer related stuff failed.\n");
        return REDISMODULE_ERR;
    }

    // start the collector thread
    if (0 > start_sys_time_collector_thread()){
        printf("starting the collector thread failed.\n");
        return REDISMODULE_ERR;
    }

#ifdef __USE_MUTEX
	if (pthread_mutex_init(&lock, NULL) != 0) { 
        return REDISMODULE_ERR; 
    }
#endif

    if (RedisModule_Init(ctx,"drivenetsgettime",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR){
        printf("RedisModule_Init failed.\n");
		return REDISMODULE_ERR;
	}

	// registering with redis!!
	if (RedisModule_CreateCommand(ctx,"drivenets.get.time",
                                  DrivenetsGetTime_RedisCommand,
								  "readonly",0,0,0)              == REDISMODULE_ERR){

        printf("DrivenetsGetTime_RedisCommand failed.\n");
        return REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}

int setup_io_multiplexer()
{
    // we want a multiplexer of size one.
    // just one timeout to manage
    if (0 > (s_epoll_fd = epoll_create(1))){
		printf ("epoll_create failed. errno: %d\n", errno);
		return -1;
	}
 
	/* create timer */
    // CLOCK_MONOTONIC is a good clock to use here, as its
    // man states that it is _not_ sensetive to changes
    // we want a non-blocking fd, so that we can avoid blocking
    // on the read when we leave the epoll on a _non_ timer related event
	s_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
	if (0 > s_timer_fd){
		printf ("timerfd_create failed. errno: %d\n", errno);
	}
	
    // add the timer to the multiplexer
    struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = s_timer_fd;
	int sts = epoll_ctl(s_epoll_fd, EPOLL_CTL_ADD, s_timer_fd, &ev);
	if (0 > sts){
		printf ("epoll_ctl failed. errno: %d\n", errno);
		return -1;
	}
	return 0;
}

int setup_timer()
{
    struct itimerspec its;
	// according to time(7) the accuracy of systemcalls that set timers
	// like sleep(2) is given by a unit called jiffi. the size of
	// one jiffi is equal to a kernel constat that indicates number of
	// clock ticks per second.
	// take this constant from the system as it is coupled to the os / hw versions
	long hz = sysconf(_SC_CLK_TCK);
	if (0 > hz){
		printf ("sysconf failed. errno: %d\n", errno);
		return -1;
	}
	// one jiffi in seconds, is therefor:
	jiffi_in_seconds = 1.0 / hz;

	//printf("jiffi_in_seconds: %f\n", jiffi_in_seconds);
 
	its.it_interval.tv_sec = 0;
	its.it_interval.tv_nsec = jiffi_in_seconds * NANO_IN_SEC;
	its.it_value.tv_sec = 1; // we start aligned to a full second
	its.it_value.tv_nsec = 0;
	if (timerfd_settime(s_timer_fd, 0, &its, NULL) < 0)
		return -1;
	return 0;
}

void* collector_thread_work(void* user)
{
	REDISMODULE_NOT_USED(user);
	struct epoll_event events[1] = {0};
	// thread starting. take current time
	struct timespec ts;
	int sts = clock_gettime(CLOCK_REALTIME, &ts);
	if (-1 == sts){
		printf("clock_gettime(CLOCK_REALTIME) failed\n");
		return (void*)-1;
	}
	
	// this is the first time we take a time sample
	long curr_time = ts.tv_sec * NANO_IN_SEC + ts.tv_nsec;
#ifdef __USE_MUTEX
	pthread_mutex_lock(&lock);
	s_system_time_usec = curr_time;
	pthread_mutex_unlock(&lock);
#elif __USE_ATOMICS
	__atomic_store(&s_system_time_usec, &curr_time, __ATOMIC_SEQ_CST);
#endif

	while(1)
	{
		// if the first timer didnt start in 40 seconds. something is wrong.
		int fireEvents = epoll_wait(s_epoll_fd, events, 1, MILLI_IN_SEC * 40);
		if(fireEvents > 0){
			// the man states that timers can expire multiple times
			// between the calls to read. to make up for that, we 
			// use the number of events from the result to account for them
			uint64_t times_expired;
			ssize_t size = read(events[0].data.fd, &times_expired, sizeof(uint64_t));
			if(size != sizeof(uint64_t)) {
				printf("read error. errno: %d\n", errno);
				// TODO if not EAGAIN, break the loop
			}
#ifdef __USE_MUTEX
			pthread_mutex_lock(&lock);
			s_system_time_usec += times_expired * (jiffi_in_seconds * NANO_IN_SEC);
			pthread_mutex_unlock(&lock);
#elif __USE_ATOMICS
			// this is a tricky one. we need CAS to do this
			long system_time_usec = 0;
			long new_sys_time = 0;
			do{
				new_sys_time = system_time_usec = s_system_time_usec;
				new_sys_time += times_expired * (jiffi_in_seconds * NANO_IN_SEC);
			}while (0 == __sync_bool_compare_and_swap(&s_system_time_usec,
			                                          system_time_usec,
													  new_sys_time));
#endif
		}
		else{
			printf("no events happened. stopping multiplexer\n");
			return (void*)-1;
		}
	}
	return (void*)0;
}



int start_sys_time_collector_thread()
{
    pthread_t threadId = -1;
    int sts = pthread_create(&threadId, NULL, &collector_thread_work, 0);
    if (0 > sts){
	    printf ("pthread_create failed. errno: %d\n", errno);
	    return -1;
    }
    // TODO where do i join with this?
    return 0;
}