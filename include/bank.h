#ifndef BANK_H
#define BANK_H

#include "utils.h"

#ifdef USE_PLAIN_MUTEX
#define rwlock_t_type pthread_mutex_t
#define rwlock_init_func(l, a) pthread_mutex_init(l, a)
#define rwlock_destroy_func(l) pthread_mutex_destroy(l)
#define rwlock_rdlock_func(l) pthread_mutex_lock(l)
#define rwlock_wrlock_func(l) pthread_mutex_lock(l)
#define rwlock_unlock_func(l) pthread_mutex_unlock(l)
#else
#define rwlock_t_type pthread_rwlock_t
#define rwlock_init_func(l, a) pthread_rwlock_init(l, a)
#define rwlock_destroy_func(l) pthread_rwlock_destroy(l)
#define rwlock_rdlock_func(l) pthread_rwlock_rdlock(l)
#define rwlock_wrlock_func(l) pthread_rwlock_wrlock(l)
#define rwlock_unlock_func(l) pthread_rwlock_unlock(l)
#endif

typedef struct {
    int account_id ; // Account number
    int balance_centavos ; // Balance in centavos
    rwlock_t_type lock ; // Per - account lock
} Account ;

typedef struct {
    Account accounts [ MAX_ACCOUNTS ];
    int num_accounts ;
    pthread_mutex_t bank_lock ; // Protects bank metadata
} Bank ;

extern Bank bank;

void init_bank(const char* accounts_filepath);
int get_balance ( int account_id );
void deposit ( int account_id , int amount_centavos );
bool withdraw ( int account_id , int amount_centavos );
bool transfer ( int from_id , int to_id , int amount_centavos, int tx_idx );

#endif // BANK_H
