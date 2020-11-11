#include <time.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include "../redismodule.h"

static int set_io_multiplexer();
static int set_timer();
static int start_sys_time_collector_thread();

static int s_epoll_fd = 0;
static int s_timer_fd = 0;
static const int NANO_IN_SEC = 1000000000;
static long s_system_time_usec = 0;
static double jiffi_in_seconds = 0.0;
static pthread_mutex_t lock;

/*
* DRIVENETS.GET.TIME 
* this is an implementation of an efficient get-system-time mechanism
* TODO add more doc here ...
*/
int DrivenetsGetTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
	// dont hold the lock for the ipc
	pthread_mutex_lock(&lock);
	long system_time_usec = s_system_time_usec;
	pthread_mutex_unlock(&lock);
	RedisModule_ReplyWithLongLong(ctx, system_time_usec);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    // we first setup the timing mechanism, as registering with redis
    // means we are "open for business" which is not true yet

    // setup io multiplexer
    if (0 != set_io_multiplexer()){
        // TODO report error
        // return
    }

    // setup time and timer related stuff
    // note: timer is _not_ armed here!
    if (0 != set_timer()){
        // TODO report error
        // return
    }

    // start the collector thread
    if (0 > start_sys_time_collector_thread()){
        // TODO report error
        // return
    }

	// just for profiling against lock free stuff
	if (pthread_mutex_init(&lock, NULL) != 0) { 
        return REDISMODULE_ERR; 
    } 

    if (RedisModule_Init(ctx,"drivenetsgettime",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"drivenets.get.time",
        DrivenetsGetTime_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

}

int set_io_multiplexer()
{
    // we want a multiplexer of size one.
    // just one timeout to manage
    s_epoll_fd = epoll_create(1);
 
	/* create timer */
    // CLOCK_MONOTONIC is a good clock to use here, as its
    // man states that it is _not_ sensetive to changes
    // we want a non-blocking fd, so that we can avoid blocking
    // on the read when we leave the epoll on a _non_ timer related event
	s_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
	
    // add the timer to the multiplexer
    struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = s_timer_fd;
	epoll_ctl(s_epoll_fd, EPOLL_CTL_ADD, s_timer_fd, &ev);
}

int set_timer()
{
    struct itimerspec its;
	// according to time(7) the accuracy of systemcalls that set timers
	// like select(2) is given by a unit called jiffi. the size of
	// one jiffi is equal to a kernel constat that indicates number of
	// clock ticks per second.
	// take this constant from the system as it is coupled to the os / hw versions
	long hz = sysconf(_SC_CLK_TCK); // TODO check failure
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
		printf("clock_gettime(CLOCK_REALTIME) failed");
		return 0;
	}
	
	// this is an attempt at getting the best resolution possible
	pthread_mutex_lock(&lock);
	s_system_time_usec = ts.tv_sec * NANO_IN_SEC + ts.tv_nsec;
	pthread_mutex_unlock(&lock);

	while(1)
	{
		
		int fireEvents = epoll_wait(s_epoll_fd, events, 1, -1);
		if(fireEvents > 0){
			// the man states that timers can expire multiple times
			// between the calls to read. to make up for that, we 
			// use the number of events from the result to account for them
			uint64_t exp;
			ssize_t size = read(events[0].data.fd, &exp, sizeof(uint64_t));
			if(size != sizeof(uint64_t)) {
				printf("read error!\n");
				// TODO if not EAGAIN, break the loop
			}
			s_system_time_usec += exp * (jiffi_in_seconds * NANO_IN_SEC);
		}
		else{
			printf("fireEvents = %d", fireEvents);
			// TODO break out of loop
		}
	}
}



int start_sys_time_collector_thread()
{
    pthread_t threadId = -1;
	pthread_create(&threadId, NULL, &collector_thread_work, 0);
    // TODO where do i join with this?
}