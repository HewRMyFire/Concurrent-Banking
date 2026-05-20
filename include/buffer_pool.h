#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include "utils.h"
#include "bank.h"

typedef struct {
    int account_id ;
    Account * data ;
    bool in_use ;
} BufferSlot ;

typedef struct {
    BufferSlot slots [ BUFFER_POOL_SIZE ];
    sem_t empty_slots ;
    sem_t full_slots ;
    pthread_mutex_t pool_lock ;
    _Atomic int total_loads ;
    _Atomic int total_unloads ;
    _Atomic int current_usage ;
    _Atomic int peak_usage ;
    _Atomic int blocked_ops ;
} BufferPool ;

extern BufferPool buffer_pool;
extern pthread_mutex_t load_phase_lock;

void init_buffer_pool ( BufferPool * pool );
void destroy_buffer_pool ( BufferPool * pool );
void load_account ( BufferPool * pool , int account_id );
void unload_account ( BufferPool * pool , int account_id );

#endif // BUFFER_POOL_H
