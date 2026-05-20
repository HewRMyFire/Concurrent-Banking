#ifndef TIMER_H
#define TIMER_H

#include "utils.h"

extern volatile int global_tick;
extern pthread_mutex_t tick_lock;
extern pthread_cond_t tick_changed;
extern _Atomic bool simulation_running;

void* timer_thread(void* arg);
void wait_until_tick(int target_tick);

#endif // TIMER_H
