#include <time.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include "../redismodule.h"

static int setup_io_multiplexer(RedisModuleCtx *ctx);
static int setup_timer(RedisModuleCtx *ctx);
static int start_sys_time_collector_thread(RedisModuleCtx *ctx);
static uint64_t init_time_components(time_t seconds);
static void collector_handle_elapsed_time();

static int s_epoll_fd = 0;
static int s_timer_fd = 0;
static const int NANO_IN_SEC = 1000000000;
static const int MILLI_IN_SEC = 1000;
//static long s_system_time_usec = 0;
//static double jiffi_in_seconds = 0.0;
static const uint8_t daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static uint64_t s_time_components = 0;

uint64_t SECONDS_MASK =   0x00000000000000FF; // 8bits
uint64_t MINUTES_MASK =   0x000000000000FF00; // 8bits
uint64_t HOURS_MASK   =   0x0000000000FF0000; // 8bits
uint64_t DAYS_MASK    =   0x00000000FF000000; // 8bits
uint64_t MONTHS_MASK  =   0x0000000F00000000; // 4bits
uint64_t YEARS_MASK   =   0xFFFFFFF000000000; // rest of bits

int SECONDS_OFFSET = 0;
int MINUTES_OFFSET = 8;
int HOURS_OFFSET   = 16;
int DAYS_OFFSET    = 24;
int MONTHS_OFFSET  = 32;
int YEARS_OFFSET   = 36;


/*
* DRIVENETS.GET.TIME 
* this is an implementation of an efficient get-system-time mechanism
* the idea is to use an internal thread that will "collect" the time
* passing into a static register. once a time sample is required, the
* command api will sample _the register_ without calling an expensive
* system call, or probing some hardware clock.
* 
* profiling numbers are given for each build permutation in: profile_results
*/
int DrivenetsGetTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
	
	
	uint64_t time_components = 0;
	__atomic_load(&s_time_components, &time_components, __ATOMIC_SEQ_CST);

	int minutes = time_components | (MINUTES_MASK>>MINUTES_OFFSET);
	int hours =   time_components | (HOURS_MASK>>HOURS_OFFSET);
	int days =    time_components | (DAYS_MASK>>DAYS_OFFSET);
	int months =  time_components | (MONTHS_MASK>>MONTHS_MASK);
	int years =   time_components | (YEARS_MASK>>YEARS_MASK);

	///// send the result back.

	char buffer [128] = {0};
	sprintf(buffer, "%02d-%02d-%d %02d:%02d", days, months, years, hours, minutes);
	RedisModule_ReplyWithCString(ctx, buffer);

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
	//long hz = sysconf(_SC_CLK_TCK);
	//if (0 > hz){
	//	RedisModule_Log(ctx, "error", "sysconf failed. errno: %d", errno);
	//	return -1;
	//}
	// one jiffi in seconds, is therefor:
	//jiffi_in_seconds = 1.0 / hz;

	//printf("jiffi_in_seconds: %f\n", jiffi_in_seconds);
 
	its.it_interval.tv_sec = 1;
	its.it_interval.tv_nsec = 0;//jiffi_in_seconds * NANO_IN_SEC;
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
	uint64_t time_components = init_time_components(curr_time / NANO_IN_SEC);
	__atomic_store(&s_time_components, &time_components, __ATOMIC_SEQ_CST);

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

			// TODO account for times_expired here
			collector_handle_elapsed_time();
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


// calculates broken down time. im using this as 
uint64_t init_time_components(time_t time)
{
	uint64_t seconds, minutes, hours, days, year, month;

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

	uint64_t current_time = 0;
	current_time |= seconds;
	current_time |= (minutes<<MINUTES_OFFSET);
	current_time |= ((hours+2)<<HOURS_OFFSET);
	current_time |= ((days+1)<<DAYS_OFFSET);
	current_time |= ((month+1)<<MONTHS_OFFSET);
	current_time |= (year<<YEARS_OFFSET);

	printf("bla bla %ld\n", seconds);
	printf("bla bla %ld\n", minutes);
	printf("bla bla %ld\n", hours+2);
	printf("bla bla %ld\n", days+1);
	printf("bla bla %ld\n", month+1);
	printf("bla bla %ld\n", year);

	printf ("curr time: %ld\n", current_time);
	
	return current_time;
}

int inc_time_component(uint64_t* time_components, uint64_t mask, int offset, uint64_t limit)
{
	uint64_t val = (*time_components) & mask;
	int spill_over = 0;
	val = val>>offset;
	if (limit == val) {
		val = 0;
		spill_over = 1;
	}else{
		val++;
	}
	val = val<<offset;
	(*time_components) |= val;
	return spill_over;
}


// this function is called with a single second resolution
void collector_handle_elapsed_time()
{
	uint64_t current_time = 0;
	__atomic_load(&s_time_components, &current_time, __ATOMIC_SEQ_CST);
	
	int spill_over = inc_time_component(&current_time, SECONDS_MASK, SECONDS_OFFSET, 59);
	if(spill_over){
		spill_over = inc_time_component(&current_time, MINUTES_MASK, MINUTES_OFFSET, 59);
	}else {return;}

	if(spill_over){
		spill_over = inc_time_component(&current_time, HOURS_MASK, HOURS_OFFSET, 23);
	}else {return;}

	if(spill_over){
		int month = (current_time & MONTHS_MASK) >> MONTHS_OFFSET;
		int mum_of_days_in_month = daysInMonth[month -1];
		spill_over = inc_time_component(&current_time, DAYS_MASK, DAYS_OFFSET, mum_of_days_in_month);
	}else {return;}

	if(spill_over){
		spill_over = inc_time_component(&current_time, MONTHS_MASK, MONTHS_OFFSET, 12);
	}else {return;}

	if(spill_over){
		// handle leap years
		spill_over = inc_time_component(&current_time, YEARS_MASK, YEARS_OFFSET, 3000);
	}
	__atomic_store(&current_time, &s_time_components, __ATOMIC_SEQ_CST);
}