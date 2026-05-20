#include "buffer_pool.h"

BufferPool buffer_pool;
pthread_mutex_t load_phase_lock = PTHREAD_MUTEX_INITIALIZER;

void init_buffer_pool ( BufferPool * pool ) {
    sem_init (& pool -> empty_slots , 0 , BUFFER_POOL_SIZE ) ;
    sem_init (& pool -> full_slots , 0 , 0) ;
    pthread_mutex_init (& pool -> pool_lock , NULL ) ;
    atomic_store_explicit(& pool -> total_loads, 0, memory_order_relaxed);
    atomic_store_explicit(& pool -> total_unloads, 0, memory_order_relaxed);
    atomic_store_explicit(& pool -> current_usage, 0, memory_order_relaxed);
    atomic_store_explicit(& pool -> peak_usage, 0, memory_order_relaxed);
    atomic_store_explicit(& pool -> blocked_ops, 0, memory_order_relaxed);
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        pool->slots[i].in_use = false;
        pool->slots[i].account_id = -1;
        pool->slots[i].data = NULL;
    }
}

void destroy_buffer_pool(BufferPool* pool) {
    sem_destroy(&pool->empty_slots);
    sem_destroy(&pool->full_slots);
    pthread_mutex_destroy(&pool->pool_lock);
}

void load_account ( BufferPool * pool , int account_id ) {
    // Try non-blocking first to detect pool-full blocks
    if (sem_trywait (& pool -> empty_slots ) != 0) {
        atomic_fetch_add_explicit(& pool -> blocked_ops, 1, memory_order_relaxed);
        sem_wait (& pool -> empty_slots ) ; // Block until slot available
    }

    pthread_mutex_lock (& pool -> pool_lock ) ;

    // Find empty slot and load account
    for (int i = 0; i < BUFFER_POOL_SIZE ; i ++) {
        if (! pool -> slots [ i ]. in_use ) {
            pool -> slots [ i ]. account_id = account_id ;
            pool -> slots [ i ]. data = & bank . accounts [ account_id ];
            pool -> slots [ i ]. in_use = true ;
            break ;
        }
    }

    // Update stats
    atomic_fetch_add_explicit(& pool -> total_loads, 1, memory_order_relaxed);
    int usage = atomic_fetch_add_explicit(& pool -> current_usage, 1, memory_order_relaxed) + 1;
    int cur_peak = atomic_load_explicit(& pool -> peak_usage, memory_order_relaxed);
    while (usage > cur_peak) {
        if (atomic_compare_exchange_weak_explicit(& pool -> peak_usage, & cur_peak, usage,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            break;
        }
    }

    pthread_mutex_unlock (& pool -> pool_lock ) ;

    sem_post (& pool -> full_slots ) ; // Signal slot is full
}

void unload_account ( BufferPool * pool , int account_id ) {
    sem_wait (& pool -> full_slots ) ; // Wait for full slot

    pthread_mutex_lock (& pool -> pool_lock ) ;

    // Find and unload account
    for (int i = 0; i < BUFFER_POOL_SIZE ; i ++) {
        if ( pool -> slots [ i ]. in_use &&
             pool -> slots [ i ]. account_id == account_id ) {
            pool -> slots [ i ]. in_use = false ;
            pool -> slots [ i ]. account_id = -1;
            break ;
        }
    }

    // Update stats
    atomic_fetch_add_explicit(& pool -> total_unloads, 1, memory_order_relaxed);
    atomic_fetch_sub_explicit(& pool -> current_usage, 1, memory_order_relaxed);

    pthread_mutex_unlock (& pool -> pool_lock ) ;

    sem_post (& pool -> empty_slots ) ; // Signal slot is empty
}
