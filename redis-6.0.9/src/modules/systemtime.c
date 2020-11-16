#include <time.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "../redismodule.h"

static int setup_io_multiplexer(RedisModuleCtx *ctx);
static int setup_timer(RedisModuleCtx *ctx);
static int start_sys_time_collector_thread(RedisModuleCtx *ctx);
static void convert_time_to_human_string(time_t seconds, char* buffer);

static int s_epoll_fd = 0;
static int s_timer_fd = 0;
static const int NANO_IN_SEC = 1000000000;
static const int MILLI_IN_SEC = 1000;
static long s_system_time_usec = 0;
static double jiffi_in_seconds = 0.0;
#ifdef __USE_MUTEX
static pthread_mutex_t lock;
static char s_formated_time_buffer [32];
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
	char buffer[32] = {0};
	long system_time_usec = 0;
#ifdef __USE_MUTEX
	pthread_mutex_lock(&lock);
	system_time_usec = s_system_time_usec;
	memcpy(buffer, s_formated_time_buffer, 32);
	pthread_mutex_unlock(&lock);
#elif __USE_ATOMICS
	__atomic_store(&system_time_usec, &s_system_time_usec, __ATOMIC_SEQ_CST);
#ifdef __USE_HUMAN_TIME
	convert_time_to_human_string(system_time_usec / NANO_IN_SEC, buffer);
#endif
#endif

	///// send the result back.
#ifdef __USE_HUMAN_TIME
	
	RedisModule_ReplyWithCString(ctx, buffer);
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
    if (0 != setup_io_multiplexer(ctx)){
		RedisModule_Log(ctx, "error", "setting up the io multiplexer failed");
        return REDISMODULE_ERR;
    }

    // setup time and timer related stuff
    // note: timer is _not_ armed here!
    if (0 != setup_timer(ctx)){
		RedisModule_Log(ctx, "error", "setting up timer related stuff failed");
        return REDISMODULE_ERR;
    }

#ifdef __USE_MUTEX
	if (pthread_mutex_init(&lock, NULL) != 0) {
		RedisModule_Log(ctx, "error", "pthread_mutex_init failed");
        return REDISMODULE_ERR; 
    }
#endif


    // start the collector thread
    if (0 > start_sys_time_collector_thread(ctx)){
		RedisModule_Log(ctx, "error", "starting the collector thread failed");
        return REDISMODULE_ERR;
    }

    if (RedisModule_Init(ctx,"drivenetsgettime",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "error", "RedisModule_Init failed");
		return REDISMODULE_ERR;
	}

	// registering with redis!!
	if (RedisModule_CreateCommand(ctx,"drivenets.get.time",
                                  DrivenetsGetTime_RedisCommand,
								  "readonly",0,0,0)              == REDISMODULE_ERR){

		RedisModule_Log(ctx, "error", "DrivenetsGetTime_RedisCommand failed");
        return REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}

int setup_io_multiplexer(RedisModuleCtx *ctx)
{
    // we want a multiplexer of size one.
    // just one timeout to manage
    if (0 > (s_epoll_fd = epoll_create(1))){
		RedisModule_Log(ctx, "error", "epoll_create failed. errno: %d", errno);
		return -1;
	}
 
	/* create timer */
    // CLOCK_MONOTONIC is a good clock to use here, as its
    // man states that it is _not_ sensetive to changes
    // we want a non-blocking fd, so that we can avoid blocking
    // on the read when we leave the epoll on a _non_ timer related event
	s_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
	if (0 > s_timer_fd){
		RedisModule_Log(ctx, "error", "timerfd_create failed. errno: %d", errno);
		return -1;
	}
	
    // add the timer to the multiplexer
    struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = s_timer_fd;
	int sts = epoll_ctl(s_epoll_fd, EPOLL_CTL_ADD, s_timer_fd, &ev);
	if (0 > sts){
		RedisModule_Log(ctx, "error", "epoll_ctl failed. errno: %d", errno);
		return -1;
	}
	return 0;
}

int setup_timer(RedisModuleCtx *ctx)
{
    struct itimerspec its;
	// according to time(7) the accuracy of systemcalls that set timers
	// like sleep(2) is given by a unit called jiffi. the size of
	// one jiffi is equal to a kernel constat that indicates number of
	// clock ticks per second.
	// take this constant from the system as it is coupled to the os / hw versions
	long hz = sysconf(_SC_CLK_TCK);
	if (0 > hz){
		RedisModule_Log(ctx, "error", "sysconf failed. errno: %d", errno);
		return -1;
	}
	// one jiffi in seconds, is therefor:
	jiffi_in_seconds = 1.0 / hz;

	//printf("jiffi_in_seconds: %f\n", jiffi_in_seconds);
 
	its.it_interval.tv_sec = 0;
	its.it_interval.tv_nsec = jiffi_in_seconds * NANO_IN_SEC;
	its.it_value.tv_sec = 1; // we start aligned to a full second
	its.it_value.tv_nsec = 0;
	if (timerfd_settime(s_timer_fd, 0, &its, NULL) < 0){
		RedisModule_Log(ctx, "error", "timerfd_settime failed. errno: %d", errno);
		return -1;
	}
	return 0;
}

void* collector_thread_work(void* user)
{
	RedisModuleCtx *ctx = (RedisModuleCtx*) user;
	struct epoll_event events[1] = {0};
	// thread starting. take current time
	struct timespec ts;
	int sts = clock_gettime(CLOCK_REALTIME, &ts);
	if (-1 == sts){
		RedisModule_Log(ctx, "error", "clock_gettime(CLOCK_REALTIME) failed");
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
				// the man says not reading a full uint64_t is an error
				// report an error and terminate, unless its these cases
				if(errno != EAGAIN && errno != EINTR){
					RedisModule_Log(ctx, "error", "read error. errno: %d", errno);
					return (void*)-1;
				}else{
					// spurious wakeup of some sort ... retry
					continue;
				}
			}
#ifdef __USE_MUTEX
			pthread_mutex_lock(&lock);
			s_system_time_usec += times_expired * (jiffi_in_seconds * NANO_IN_SEC);
			convert_time_to_human_string(s_system_time_usec / NANO_IN_SEC, s_formated_time_buffer);
			pthread_mutex_unlock(&lock);
#elif __USE_ATOMICS			
			int delta = times_expired * (jiffi_in_seconds * NANO_IN_SEC);
			__sync_add_and_fetch(&s_system_time_usec, delta);
#endif
		}
		else{
			// epoll_wait failed. only EINTR is recoverable
			if(errno != EINTR){
				RedisModule_Log(ctx, "error", "no events happened. stopping multiplexer. errno: %d", errno);
				return (void*)-1;
			}
		}
	}
	return (void*)0;
}



int start_sys_time_collector_thread(RedisModuleCtx *ctx)
{
    pthread_t threadId = -1;
    int sts = pthread_create(&threadId, NULL, &collector_thread_work, (void*)ctx);
    if (0 > sts){
		RedisModule_Log(ctx, "error", "pthread_create failed. errno: %d", errno);
	    return -1;
    }
    // TODO where do i join with this?
    return 0;
}

void convert_time_to_human_string(time_t time, char* buffer)
{
	uint32_t seconds, minutes, hours, days, year, month;

	seconds = time;

	/* calculate minutes */
	minutes  = seconds / 60;
	seconds -= minutes * 60;
	/* calculate hours */
	hours    = minutes / 60;
	minutes -= hours   * 60;
	/* calculate days */
	days     = hours   / 24;
	hours   -= days    * 24;

	/* Unix time starts in 1970 on a Thursday */
	year      = 1970;

	while(1)
	{
		int leapYear   = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
		uint16_t daysInYear = leapYear ? 366 : 365;
		if (days >= daysInYear)
		{
			days -= daysInYear;
			++year;
		}
		else
		{
			/* calculate the month and day */
			static const uint8_t daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
			for(month = 0; month < 12; ++month)
			{
				uint8_t dim = daysInMonth[month];

				/* add a day to feburary if this is a leap year */
				if (month == 1 && leapYear)
					++dim;

				if (days >= dim)
					days -= dim;
				else
					break;
			}
			break;
		}
	}

	sprintf(buffer, "%02d-%02d-%d %02d:%02d", days+1, month+1, year, hours+2, minutes);
}