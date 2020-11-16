

DRIVENETS.GET.TIME
this is an implementation of an efficient get-system-time mechanism
the idea is to use an internal thread that will "collect" the time
passing into a static register. once a time sample is required, the
command api will sample _the register_ without calling an expensive
system call, or probing some hardware clock.

the main idea is to move all the heavy lifting into the collector thread.
the thread executing the command will simply copy a - pre formatted - buffer
and send it via the redis internal ipc
profiling numbers are given for each build permutation in: profile_results
