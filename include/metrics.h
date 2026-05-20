#ifndef METRICS_H
#define METRICS_H

#include "utils.h"

extern _Atomic uint64_t total_lock_wait_ns;
extern _Atomic uint64_t max_lock_wait_ns;
extern _Atomic int successful_tx_count;
extern _Atomic int failed_tx_count;
extern _Atomic uint64_t total_wait_ticks;
extern _Atomic int max_wait_ticks;

void print_summary(int workload_size, double real_elapsed_sec, int sim_clock, int initial_total, int final_total);

#endif // METRICS_H
