#ifndef UTILS_H
#define UTILS_H

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <string.h>
#include <stdatomic.h>
#include <ctype.h>

#define MAX_ACCOUNTS 100
#define BUFFER_SIZE 50
#define NUM_WORKERS 4
#define MAX_WORKLOAD_SIZE 10000
#define BUFFER_POOL_SIZE 5

// Globals
extern const char* deadlock_strategy_global;
extern pthread_mutex_t log_mutex;
extern FILE* log_file;
extern int tick_interval_ms;
extern bool verbose;

uint64_t get_time_ns();

#endif // UTILS_H
