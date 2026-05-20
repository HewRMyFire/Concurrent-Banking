#include "timer.h"

volatile int global_tick = 0;
pthread_mutex_t tick_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t tick_changed = PTHREAD_COND_INITIALIZER;
_Atomic bool simulation_running = true;

void * timer_thread ( void * arg ) {
    (void)arg;
    while ( simulation_running ) {
        usleep ( tick_interval_ms * 1000) ; // Sleep to simulate a tick
        pthread_mutex_lock (& tick_lock ) ;
        global_tick ++;
        pthread_mutex_lock(&log_mutex);
        printf("\nTick %d:\n", global_tick);
        fflush(stdout);
        pthread_mutex_unlock(&log_mutex);
        pthread_cond_broadcast (& tick_changed ) ; // Wake waiting
        pthread_mutex_unlock (& tick_lock ) ;
    }
    return NULL ;
}

void wait_until_tick (int target_tick ) {
    pthread_mutex_lock (& tick_lock ) ;
    while ( global_tick < target_tick ) {
        pthread_cond_wait (& tick_changed , & tick_lock ) ;
    }
    pthread_mutex_unlock (& tick_lock ) ;
}
