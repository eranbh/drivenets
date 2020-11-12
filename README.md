
DRIVENETS.GET.TIME 

this is an implementation of an efficient get-system-time mechanism
the idea is to use an internal thread that will "collect" the time
passing into a static register. once a time sample is required, the
command api will sample _the register_ without calling an expensive
system call, or probing some hardware clock.

as there is an inherant race on the above register,
there are 2 modes with which this can be compiled:
1. -D__USE_MUTEX: uses a pthreads mutex.
2. -D__USE_ATOMICS: uses a gcc builtin atomic operation

the user can choose the format of the output:
1. -D__USE_HUMAN_TIME: will format the timestamp to a humanly readable
   string. _note_: this option is expensive and "losy" as it looses percision
   when it cuts the timestamp down to seconds (the base needed by the
   formating functions)
2. none: will return the full timestamp in nanos

profiling numbers are given for each build permutation in: profile_results


open issues:
1. task shutdown. should i implement a mode of the command to shutdown the
   time collection mechanism?
2. portability:
   2.1 there seems to be issues with using the high resolution clock apis when
    compiling with -std=99. i did not get to the bottom of this yet.
   2.2 for sync issues i am using gcc builtins (in the __use_atomics mode) that
       are _not_ portable.
3. logging goes to the console. need to log to a file.
4. the formating option seems very expensive. need to investigate that.
   if the baseline is hset, the current profiling shows this is twice as slow (on avarage)