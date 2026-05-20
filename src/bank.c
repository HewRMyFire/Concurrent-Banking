#include "bank.h"
#include "lock_mgr.h"
#include "metrics.h"
#include "transaction.h"

Bank bank;

void init_bank(const char* accounts_filepath) {
    pthread_mutex_init(&bank.bank_lock, NULL);
    bank.num_accounts = MAX_ACCOUNTS;
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        bank.accounts[i].account_id = i;
        bank.accounts[i].balance_centavos = 0; // Initialize to 0
        rwlock_init_func(&bank.accounts[i].lock, NULL);
    }

    FILE* file = fopen(accounts_filepath, "r");
    if (!file) {
        perror("Error opening accounts file");
        exit(EXIT_FAILURE);
    }

    char line[256];
    while (fgets(line, sizeof(line), file)) {
        char* ptr = line;
        while (isspace((unsigned char)*ptr)) ptr++;
        if (*ptr == '\0' || *ptr == '#') continue;

        int account_id = 0;
        int balance_centavos = 0;
        if (sscanf(ptr, "%d %d", &account_id, &balance_centavos) == 2) {
            if (account_id >= 0 && account_id < MAX_ACCOUNTS) {
                bank.accounts[account_id].balance_centavos = balance_centavos;
            } else {
                fprintf(stderr, "Warning: Account ID %d out of bounds (max %d)\n", account_id, MAX_ACCOUNTS);
            }
        } else {
            fprintf(stderr, "Malformatted accounts line: %s\n", line);
        }
    }
    fclose(file);
}

int get_balance ( int account_id ) {
    Account * acc = & bank . accounts [ account_id ];

    rwlock_rdlock_func (& acc -> lock ) ;
    int balance = acc -> balance_centavos ;
    rwlock_unlock_func (& acc -> lock ) ;

    return balance ;
}

void deposit ( int account_id , int amount_centavos ) {
    Account * acc = & bank . accounts [ account_id ];

    rwlock_wrlock_func (& acc -> lock ) ;
    acc -> balance_centavos += amount_centavos ;
    rwlock_unlock_func (& acc -> lock ) ;
}

bool withdraw ( int account_id , int amount_centavos ) {
    Account * acc = & bank . accounts [ account_id ];

    rwlock_wrlock_func (& acc -> lock ) ;

    if ( acc -> balance_centavos < amount_centavos ) {
        rwlock_unlock_func (& acc -> lock ) ;
        return false ; // Insufficient funds
    }

    acc -> balance_centavos -= amount_centavos ;
    rwlock_unlock_func (& acc -> lock ) ;
    return true ;
}

bool transfer ( int from_id , int to_id , int amount_centavos, int tx_idx ) {
    int tx_id = workload[tx_idx].tx_id;
    if (from_id == to_id) {
        return true;
    }

    if (strcmp(deadlock_strategy_global, "prevention") == 0) {
        // Prevention strategy: lock ordering
        pthread_mutex_lock(&log_mutex);
        printf("[ DEADLOCK PREVENTED ] Lock ordering : T%d waiting for account %d\n", tx_id, from_id);
        printf("[ DEADLOCK PREVENTED ] Lock ordering : T%d waiting for account %d\n", tx_id, to_id);
        if (log_file) {
            fprintf(log_file, "[ DEADLOCK PREVENTED ] Lock ordering : T%d waiting for account %d\n", tx_id, from_id);
            fprintf(log_file, "[ DEADLOCK PREVENTED ] Lock ordering : T%d waiting for account %d\n", tx_id, to_id);
        }
        fflush(stdout);
        if (log_file) fflush(log_file);
        pthread_mutex_unlock(&log_mutex);

        // Acquire locks in consistent order to prevent deadlock
        int first = ( from_id < to_id ) ? from_id : to_id ;
        int second = ( from_id < to_id ) ? to_id : from_id ;

        Account * acc_first = & bank . accounts [ first ];
        Account * acc_second = & bank . accounts [ second ];

        uint64_t start_wait = get_time_ns();
        rwlock_wrlock_func (& acc_first -> lock ) ;
        rwlock_wrlock_func (& acc_second -> lock ) ;
        uint64_t end_wait = get_time_ns();
        uint64_t tx_wait_ns = end_wait - start_wait;
        
        atomic_fetch_add_explicit(&total_lock_wait_ns, tx_wait_ns, memory_order_relaxed);
        uint64_t cur_max = atomic_load_explicit(&max_lock_wait_ns, memory_order_relaxed);
        while (tx_wait_ns > cur_max) {
            if (atomic_compare_exchange_weak_explicit(&max_lock_wait_ns, &cur_max, tx_wait_ns,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        }

        // Check sufficient funds
        Account * from_acc = & bank . accounts [ from_id ];
        if ( from_acc -> balance_centavos < amount_centavos ) {
            rwlock_unlock_func (& acc_second -> lock ) ;
            rwlock_unlock_func (& acc_first -> lock ) ;
            return false ;
        }

        // Perform transfer
        bank . accounts [ from_id ]. balance_centavos -= amount_centavos ;
        bank . accounts [ to_id ]. balance_centavos += amount_centavos ;

        rwlock_unlock_func (& acc_second -> lock ) ;
        rwlock_unlock_func (& acc_first -> lock ) ;
        return true ;
    } else {
        // Detection strategy: naive lock ordering with wait-for graph cycle checking
        uint64_t start_wait = get_time_ns();

        if (!acquire_account_lock(from_id, tx_idx)) {
            return false;
        }

        pthread_mutex_lock(&log_mutex);
        printf("T%d acquired lock on account %d\n", tx_id, from_id);
        if (log_file) {
            fprintf(log_file, "T%d acquired lock on account %d\n", tx_id, from_id);
        }
        fflush(stdout);
        if (log_file) fflush(log_file);
        pthread_mutex_unlock(&log_mutex);

        if (!acquire_account_lock(to_id, tx_idx)) {
            release_account_lock(from_id, tx_idx);
            return false;
        }

        uint64_t end_wait = get_time_ns();
        uint64_t tx_wait_ns = end_wait - start_wait;
        
        atomic_fetch_add_explicit(&total_lock_wait_ns, tx_wait_ns, memory_order_relaxed);
        uint64_t cur_max = atomic_load_explicit(&max_lock_wait_ns, memory_order_relaxed);
        while (tx_wait_ns > cur_max) {
            if (atomic_compare_exchange_weak_explicit(&max_lock_wait_ns, &cur_max, tx_wait_ns,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        }

        // Check sufficient funds
        Account * from_acc = & bank . accounts [ from_id ];
        if ( from_acc -> balance_centavos < amount_centavos ) {
            release_account_lock(to_id, tx_idx);
            release_account_lock(from_id, tx_idx);
            return false ;
        }

        // Perform transfer
        bank . accounts [ from_id ]. balance_centavos -= amount_centavos ;
        bank . accounts [ to_id ]. balance_centavos += amount_centavos ;

        release_account_lock(to_id, tx_idx);
        release_account_lock(from_id, tx_idx);
        return true ;
    }
}
