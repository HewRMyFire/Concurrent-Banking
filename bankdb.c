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
#define TICK_INTERVAL_MS 10
#define BUFFER_POOL_SIZE 5

typedef enum {
    OP_DEPOSIT,    // Add money to account
    OP_WITHDRAW,   // Remove money from account
    OP_TRANSFER,   // Move money between two accounts
    OP_BALANCE,    // Read account balance
} OpType;

typedef struct {
    OpType type;
    int account_id;      // Primary account
    int amount_centavos; // Amount in centavos
    int target_account;  // For TRANSFER only
} Operation;

typedef enum {
    TX_RUNNING,
    TX_COMMITTED,
    TX_ABORTED
} TxStatus;

typedef struct {
    int tx_id;
    Operation ops[256];  // Max 256 operations per transaction
    int num_ops;
    int start_tick;      // When transaction should start
    pthread_t thread;

    // Timing (measured in ticks)
    int actual_start;
    int actual_end;
    int wait_ticks;

    // Status
    TxStatus status;
} Transaction;

typedef struct {
    int account_id ; // Account number
    int balance_centavos ; // Balance in centavos
    pthread_rwlock_t lock ; // Per - account lock
} Account ;

typedef struct {
    Account accounts [ MAX_ACCOUNTS ];
    int num_accounts ;
    pthread_mutex_t bank_lock ; // Protects bank metadata
} Bank ;

typedef struct {
    Transaction buffer[BUFFER_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    sem_t empty_slots;
    sem_t filled_slots;
} BoundedBuffer;

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
} BufferPool ;

// Globals
Bank bank;
BoundedBuffer tx_queue;
BufferPool buffer_pool;
pthread_mutex_t log_mutex;
FILE* log_file = NULL;

Transaction workload[MAX_WORKLOAD_SIZE];
int workload_size = 0;

volatile int global_tick = 0;
pthread_mutex_t tick_lock;
pthread_cond_t tick_changed;
_Atomic bool simulation_running = true;

sem_t worker_sem;
pthread_mutex_t load_phase_lock;

_Atomic uint64_t total_lock_wait_ns = 0;
_Atomic uint64_t max_lock_wait_ns = 0;
_Atomic int successful_tx_count = 0;
_Atomic int failed_tx_count = 0;
_Atomic uint64_t total_wait_ticks = 0;
_Atomic int max_wait_ticks = 0;

typedef struct {
    OpType type;
    int account_id;
    int amount_centavos;
    int target_account;
} CompletedOp;

uint64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

void init_buffer(BoundedBuffer* b) {
    b->head = 0; 
    b->tail = 0; 
    b->count = 0;
    pthread_mutex_init(&b->mutex, NULL);
    sem_init(&b->empty_slots, 0, BUFFER_SIZE);
    sem_init(&b->filled_slots, 0, 0);
}

void destroy_buffer(BoundedBuffer* b) {
    pthread_mutex_destroy(&b->mutex);
    sem_destroy(&b->empty_slots);
    sem_destroy(&b->filled_slots);
}

void submit_transaction(BoundedBuffer* b, Transaction tx) {
    sem_wait(&b->empty_slots);
    pthread_mutex_lock(&b->mutex);
    
    b->buffer[b->tail] = tx;
    b->tail = (b->tail + 1) % BUFFER_SIZE;
    b->count++;
    
    pthread_mutex_unlock(&b->mutex);
    sem_post(&b->filled_slots);
}

void fetch_transaction(BoundedBuffer* b, Transaction* tx) {
    sem_wait(&b->filled_slots);
    pthread_mutex_lock(&b->mutex);
    
    *tx = b->buffer[b->head];
    b->head = (b->head + 1) % BUFFER_SIZE;
    b->count--;
    
    pthread_mutex_unlock(&b->mutex);
    sem_post(&b->empty_slots);
}

void init_buffer_pool ( BufferPool * pool ) {
    sem_init (& pool -> empty_slots , 0 , BUFFER_POOL_SIZE ) ;
    sem_init (& pool -> full_slots , 0 , 0) ;
    pthread_mutex_init (& pool -> pool_lock , NULL ) ;
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

// Load account into buffer pool ( producer )
void load_account ( BufferPool * pool , int account_id ) {
    sem_wait (& pool -> empty_slots ) ; // Wait for empty slot

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

    pthread_mutex_unlock (& pool -> pool_lock ) ;

    sem_post (& pool -> full_slots ) ; // Signal slot is full
}

// Unload account from buffer pool ( consumer )
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

    pthread_mutex_unlock (& pool -> pool_lock ) ;

    sem_post (& pool -> empty_slots ) ; // Signal slot is empty
}

// Timer thread increments clock every TICK_INTERVAL_MS
void * timer_thread ( void * arg ) {
    (void)arg;
    while ( simulation_running ) {
        pthread_mutex_lock (& tick_lock ) ;
        usleep ( TICK_INTERVAL_MS * 1000) ; // Sleep to simulate a tick
        global_tick ++;
        pthread_cond_broadcast (& tick_changed ) ; // Wake waiting
        pthread_mutex_unlock (& tick_lock ) ;
        usleep(100); // Avoid starvation of other threads waiting for tick_lock
    }
    return NULL ;
}

// Transactions wait until their start_tick
void wait_until_tick (int target_tick ) {
    pthread_mutex_lock (& tick_lock ) ;
    while ( global_tick < target_tick ) {
        pthread_cond_wait (& tick_changed , & tick_lock ) ;
    }
    pthread_mutex_unlock (& tick_lock ) ;
}

int get_balance ( int account_id ) {
    Account * acc = & bank . accounts [ account_id ];

    pthread_rwlock_rdlock (& acc -> lock ) ;
    int balance = acc -> balance_centavos ;
    pthread_rwlock_unlock (& acc -> lock ) ;

    return balance ;
}

void deposit ( int account_id , int amount_centavos ) {
    Account * acc = & bank . accounts [ account_id ];

    pthread_rwlock_wrlock (& acc -> lock ) ;
    acc -> balance_centavos += amount_centavos ;
    pthread_rwlock_unlock (& acc -> lock ) ;
}

bool withdraw ( int account_id , int amount_centavos ) {
    Account * acc = & bank . accounts [ account_id ];

    pthread_rwlock_wrlock (& acc -> lock ) ;

    if ( acc -> balance_centavos < amount_centavos ) {
        pthread_rwlock_unlock (& acc -> lock ) ;
        return false ; // Insufficient funds
    }

    acc -> balance_centavos -= amount_centavos ;
    pthread_rwlock_unlock (& acc -> lock ) ;
    return true ;
}

bool transfer ( int from_id , int to_id , int amount_centavos ) {
    if (from_id == to_id) {
        return true;
    }

    // Acquire locks in consistent order to prevent deadlock
    int first = ( from_id < to_id ) ? from_id : to_id ;
    int second = ( from_id < to_id ) ? to_id : from_id ;

    Account * acc_first = & bank . accounts [ first ];
    Account * acc_second = & bank . accounts [ second ];

    uint64_t start_wait = get_time_ns();
    pthread_rwlock_wrlock (& acc_first -> lock ) ;
    pthread_rwlock_wrlock (& acc_second -> lock ) ;
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
        pthread_rwlock_unlock (& acc_second -> lock ) ;
        pthread_rwlock_unlock (& acc_first -> lock ) ;
        return false ;
    }

    // Perform transfer
    bank . accounts [ from_id ]. balance_centavos -= amount_centavos ;
    bank . accounts [ to_id ]. balance_centavos += amount_centavos ;

    pthread_rwlock_unlock (& acc_second -> lock ) ;
    pthread_rwlock_unlock (& acc_first -> lock ) ;
    return true ;
}

void * execute_transaction ( void * arg ) {
    Transaction * tx = ( Transaction *) arg ;

    // Collect all unique accounts accessed by this transaction
    int unique_accounts[256];
    int unique_count = 0;
    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        bool found = false;
        for (int j = 0; j < unique_count; j++) {
            if (unique_accounts[j] == op.account_id) {
                found = true;
                break;
            }
        }
        if (!found) {
            unique_accounts[unique_count++] = op.account_id;
        }

        if (op.type == OP_TRANSFER) {
            found = false;
            for (int j = 0; j < unique_count; j++) {
                if (unique_accounts[j] == op.target_account) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                unique_accounts[unique_count++] = op.target_account;
            }
        }
    }

    // Sort unique account IDs to prevent deadlocks in buffer pool loading
    for (int i = 0; i < unique_count - 1; i++) {
        for (int j = i + 1; j < unique_count; j++) {
            if (unique_accounts[i] > unique_accounts[j]) {
                int tmp = unique_accounts[i];
                unique_accounts[i] = unique_accounts[j];
                unique_accounts[j] = tmp;
            }
        }
    }

    // Wait until scheduled start time
    wait_until_tick ( tx -> start_tick ) ;

    // Load accounts into the bounded buffer pool atomically
    pthread_mutex_lock(&load_phase_lock);
    for (int i = 0; i < unique_count; i++) {
        load_account(&buffer_pool, unique_accounts[i]);
    }
    pthread_mutex_unlock(&load_phase_lock);

    pthread_mutex_lock(&tick_lock);
    tx -> actual_start = global_tick ;
    pthread_mutex_unlock(&tick_lock);

    CompletedOp completed_ops[256];
    int completed_count = 0;

    for (int i = 0; i < tx -> num_ops ; i ++) {
        Operation * op = & tx -> ops [ i ];

        pthread_mutex_lock(&tick_lock);
        int tick_before = global_tick ;
        pthread_mutex_unlock(&tick_lock);

        bool op_success = true;

        switch ( op -> type ) {
        case OP_DEPOSIT :
            deposit ( op -> account_id , op -> amount_centavos ) ;
            completed_ops[completed_count++] = (CompletedOp){OP_DEPOSIT, op->account_id, op->amount_centavos, 0};
            break ;

        case OP_WITHDRAW :
            if (! withdraw ( op -> account_id , op -> amount_centavos ) ) {
                op_success = false;
            } else {
                completed_ops[completed_count++] = (CompletedOp){OP_WITHDRAW, op->account_id, op->amount_centavos, 0};
            }
            break ;

        case OP_TRANSFER :
            if (! transfer ( op -> account_id , op -> target_account ,
                            op -> amount_centavos ) ) {
                op_success = false;
            } else {
                completed_ops[completed_count++] = (CompletedOp){OP_TRANSFER, op->account_id, op->amount_centavos, op->target_account};
            }
            break ;

        case OP_BALANCE : {
            int balance = get_balance ( op -> account_id ) ;
            pthread_mutex_lock(&log_mutex);
            printf ("T%d: Account %d balance = PHP %d .%02d\n",
                    tx -> tx_id , op -> account_id ,
                    balance / 100 , balance % 100) ;
            if (log_file) {
                fprintf(log_file, "T%d: Account %d balance = PHP %d .%02d\n",
                        tx -> tx_id , op -> account_id ,
                        balance / 100 , balance % 100) ;
            }
            pthread_mutex_unlock(&log_mutex);
            break ;
        }
        }

        pthread_mutex_lock(&tick_lock);
        int tick_after = global_tick;
        pthread_mutex_unlock(&tick_lock);

        tx -> wait_ticks += ( tick_after - tick_before ) ;

        if (!op_success) {
            // Rollback completed operations in reverse order
            for (int j = completed_count - 1; j >= 0; j--) {
                CompletedOp cop = completed_ops[j];
                if (cop.type == OP_DEPOSIT) {
                    withdraw(cop.account_id, cop.amount_centavos);
                } else if (cop.type == OP_WITHDRAW) {
                    deposit(cop.account_id, cop.amount_centavos);
                } else if (cop.type == OP_TRANSFER) {
                    transfer(cop.target_account, cop.account_id, cop.amount_centavos);
                }
            }

            tx -> status = TX_ABORTED ;

            pthread_mutex_lock(&tick_lock);
            tx->actual_end = global_tick;
            int clock_val = global_tick;
            pthread_mutex_unlock(&tick_lock);

            atomic_fetch_add_explicit(&total_wait_ticks, tx->wait_ticks, memory_order_relaxed);
            int cur_max_ticks = atomic_load_explicit(&max_wait_ticks, memory_order_relaxed);
            while (tx->wait_ticks > cur_max_ticks) {
                if (atomic_compare_exchange_weak_explicit(&max_wait_ticks, &cur_max_ticks, tx->wait_ticks,
                                                          memory_order_relaxed, memory_order_relaxed)) {
                    break;
                }
            }

            atomic_fetch_add_explicit(&failed_tx_count, 1, memory_order_relaxed);

            pthread_mutex_lock(&log_mutex);
            printf("[Clock: %4d] Worker %lu processed TX %3d -> \033[1;31mABORTED\033[0m [Sched:%d, Actual:%d-%d, Wait:%d ticks]\n",
                   clock_val, (unsigned long)pthread_self(), tx->tx_id, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks);
            if (log_file) {
                fprintf(log_file, "[Clock: %d] Worker %lu processed TX %d -> ABORTED [Sched:%d, Actual:%d-%d, Wait:%d ticks]\n",
                        clock_val, (unsigned long)pthread_self(), tx->tx_id, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks);
            }
            fflush(stdout);
            if (log_file) fflush(log_file);
            pthread_mutex_unlock(&log_mutex);

            // Unload accounts from the bounded buffer pool
            for (int i = 0; i < unique_count; i++) {
                unload_account(&buffer_pool, unique_accounts[i]);
            }

            sem_post(&worker_sem);
            return NULL ;
        }
    }

    pthread_mutex_lock(&tick_lock);
    tx -> actual_end = global_tick ;
    int clock_val = global_tick;
    pthread_mutex_unlock(&tick_lock);
    
    tx -> status = TX_COMMITTED ;

    atomic_fetch_add_explicit(&total_wait_ticks, tx->wait_ticks, memory_order_relaxed);
    int cur_max_ticks = atomic_load_explicit(&max_wait_ticks, memory_order_relaxed);
    while (tx->wait_ticks > cur_max_ticks) {
        if (atomic_compare_exchange_weak_explicit(&max_wait_ticks, &cur_max_ticks, tx->wait_ticks,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            break;
        }
    }

    atomic_fetch_add_explicit(&successful_tx_count, 1, memory_order_relaxed);

    pthread_mutex_lock(&log_mutex);
    char op_details[2048] = "";
    int offset = 0;
    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        if (op.type == OP_DEPOSIT) {
            offset += snprintf(op_details + offset, sizeof(op_details) - offset,
                              "DEP(%d, %d) ", op.account_id, op.amount_centavos);
        } else if (op.type == OP_WITHDRAW) {
            offset += snprintf(op_details + offset, sizeof(op_details) - offset,
                              "WDR(%d, %d) ", op.account_id, op.amount_centavos);
        } else if (op.type == OP_TRANSFER) {
            offset += snprintf(op_details + offset, sizeof(op_details) - offset,
                              "TRF(%d->%d, %d) ", op.account_id, op.target_account, op.amount_centavos);
        } else if (op.type == OP_BALANCE) {
            offset += snprintf(op_details + offset, sizeof(op_details) - offset,
                              "BAL(%d) ", op.account_id);
        }
    }

    printf("[Clock: %4d] Worker %lu processed TX %3d -> \033[1;32mCOMMITTED\033[0m [Sched:%d, Actual:%d-%d, Wait:%d ticks] [Ops: %s]\n",
           clock_val, (unsigned long)pthread_self(), tx->tx_id, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks, op_details);
    if (log_file) {
        fprintf(log_file, "[Clock: %d] Worker %lu processed TX %d -> COMMITTED [Sched:%d, Actual:%d-%d, Wait:%d ticks] [Ops: %s]\n",
                clock_val, (unsigned long)pthread_self(), tx->tx_id, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks, op_details);
    }
    fflush(stdout);
    if (log_file) fflush(log_file);
    pthread_mutex_unlock(&log_mutex);

    // Unload accounts from the bounded buffer pool
    for (int i = 0; i < unique_count; i++) {
        unload_account(&buffer_pool, unique_accounts[i]);
    }

    sem_post(&worker_sem);

    return NULL ;
}

void* dispatcher_thread(void* arg) {
    (void)arg;
    while (1) {
        Transaction tx;
        fetch_transaction(&tx_queue, &tx);
        if (tx.tx_id == -1) {
            break; // Poison pill exits dispatcher
        }
        
        // Find transaction inside the workload array
        int idx = -1;
        for (int i = 0; i < workload_size; i++) {
            if (workload[i].tx_id == tx.tx_id) {
                idx = i;
                break;
            }
        }
        if (idx != -1) {
            sem_wait(&worker_sem);
            pthread_create(&workload[idx].thread, NULL, execute_transaction, &workload[idx]);
        }
    }
    return NULL;
}

void init_bank(const char* accounts_filepath) {
    pthread_mutex_init(&bank.bank_lock, NULL);
    bank.num_accounts = MAX_ACCOUNTS;
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        bank.accounts[i].account_id = i;
        bank.accounts[i].balance_centavos = 0; // Initialize to 0
        pthread_rwlock_init(&bank.accounts[i].lock, NULL);
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

void load_trace(const char* filepath) {
    FILE* file = fopen(filepath, "r");
    if (!file) {
        perror("Error opening trace file");
        exit(EXIT_FAILURE);
    }

    char line[256];
    while (fgets(line, sizeof(line), file)) {
        char* ptr = line;
        while (isspace((unsigned char)*ptr)) ptr++;
        if (*ptr == '\0' || *ptr == '#') continue;

        int tx_id = 0;
        int start_tick = 0;
        char op_name[32];
        if (sscanf(ptr, "T%d %d %31s", &tx_id, &start_tick, op_name) == 3) {
            Operation op;
            memset(&op, 0, sizeof(op));
            bool op_valid = false;

            if (strcmp(op_name, "DEPOSIT") == 0) {
                op.type = OP_DEPOSIT;
                if (sscanf(ptr, "T%*d %*d %*s %d %d", &op.account_id, &op.amount_centavos) == 2) {
                    op_valid = true;
                }
            } else if (strcmp(op_name, "WITHDRAW") == 0) {
                op.type = OP_WITHDRAW;
                if (sscanf(ptr, "T%*d %*d %*s %d %d", &op.account_id, &op.amount_centavos) == 2) {
                    op_valid = true;
                }
            } else if (strcmp(op_name, "TRANSFER") == 0) {
                op.type = OP_TRANSFER;
                if (sscanf(ptr, "T%*d %*d %*s %d %d %d", &op.account_id, &op.target_account, &op.amount_centavos) == 3) {
                    op_valid = true;
                }
            } else if (strcmp(op_name, "BALANCE") == 0) {
                op.type = OP_BALANCE;
                if (sscanf(ptr, "T%*d %*d %*s %d", &op.account_id) == 1) {
                    op_valid = true;
                }
            } else {
                fprintf(stderr, "Unknown operation: %s\n", op_name);
            }

            if (op_valid) {
                // Find if transaction tx_id already exists in workload
                int idx = -1;
                for (int i = 0; i < workload_size; i++) {
                    if (workload[i].tx_id == tx_id) {
                        idx = i;
                        break;
                    }
                }

                if (idx != -1) {
                    // Transaction exists, append operation if we have space
                    Transaction* tx = &workload[idx];
                    if (tx->num_ops < 256) {
                        tx->ops[tx->num_ops++] = op;
                    } else {
                        fprintf(stderr, "Warning: TX %d exceeded max operation count (256). Operation ignored.\n", tx_id);
                    }
                } else {
                    // Create new transaction
                    if (workload_size >= MAX_WORKLOAD_SIZE) {
                        fprintf(stderr, "Workload limit reached (%d)\n", MAX_WORKLOAD_SIZE);
                        break;
                    }
                    Transaction* tx = &workload[workload_size];
                    tx->tx_id = tx_id;
                    tx->start_tick = start_tick;
                    tx->ops[0] = op;
                    tx->num_ops = 1;
                    tx->status = TX_RUNNING;
                    tx->thread = 0;
                    tx->actual_start = 0;
                    tx->actual_end = 0;
                    tx->wait_ticks = 0;
                    workload_size++;
                }
            }
        } else {
            fprintf(stderr, "Malformatted trace line: %s\n", line);
        }
    }

    fclose(file);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <trace_file_path> <accounts_file_path> [num_workers]\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char* trace_path = argv[1];
    const char* accounts_path = argv[2];
    int num_workers = NUM_WORKERS;
    if (argc >= 4) {
        num_workers = atoi(argv[3]);
        if (num_workers <= 0) {
            fprintf(stderr, "Error: Invalid number of workers specified. Using default (%d).\n", NUM_WORKERS);
            num_workers = NUM_WORKERS;
        }
    }

    log_file = fopen("bankdb.log", "w");
    if (!log_file) {
        perror("Failed to open bankdb.log");
    }

    pthread_mutex_init(&log_mutex, NULL);
    pthread_mutex_init(&tick_lock, NULL);
    pthread_cond_init(&tick_changed, NULL);
    sem_init(&worker_sem, 0, num_workers);
    pthread_mutex_init(&load_phase_lock, NULL);

    printf("=========================================\n");
    printf("Initializing Concurrent BankDB...\n");
    printf("  Workers     : %d\n", num_workers);
    printf("  Max Accounts: %d\n", MAX_ACCOUNTS);
    printf("  Buffer Size : %d\n", BUFFER_SIZE);
    printf("  Pool Size   : %d\n", BUFFER_POOL_SIZE);
    printf("=========================================\n");

    init_bank(accounts_path);
    init_buffer(&tx_queue);
    init_buffer_pool(&buffer_pool);

    printf("Loading workload from %s...\n", trace_path);
    load_trace(trace_path);

    uint64_t real_start_ns = get_time_ns();

    pthread_t timer;
    pthread_t dispatcher;

    pthread_create(&timer, NULL, timer_thread, NULL);
    pthread_create(&dispatcher, NULL, dispatcher_thread, NULL);

    // Submit workload transactions to queue immediately
    printf("Submitting workload transactions to bounded queue...\n");
    for (int i = 0; i < workload_size; i++) {
        submit_transaction(&tx_queue, workload[i]);
    }

    // Submit poison pill to terminate dispatcher thread
    Transaction pill;
    pill.tx_id = -1;
    pill.num_ops = 0;
    submit_transaction(&tx_queue, pill);

    // Join dispatcher thread
    pthread_join(dispatcher, NULL);

    // Join all spawned transaction threads
    for (int i = 0; i < workload_size; i++) {
        if (workload[i].thread != 0) {
            pthread_join(workload[i].thread, NULL);
        }
    }

    // Stop timer thread
    atomic_store_explicit(&simulation_running, false, memory_order_relaxed);
    
    pthread_mutex_lock(&tick_lock);
    pthread_cond_broadcast(&tick_changed);
    pthread_mutex_unlock(&tick_lock);
    
    pthread_join(timer, NULL);

    uint64_t real_end_ns = get_time_ns();
    double real_elapsed_sec = (real_end_ns - real_start_ns) / 1000000000.0;

    int succ = atomic_load_explicit(&successful_tx_count, memory_order_relaxed);
    int fail = atomic_load_explicit(&failed_tx_count, memory_order_relaxed);
    int total_tx = succ + fail;
    uint64_t total_wait = atomic_load_explicit(&total_lock_wait_ns, memory_order_relaxed);
    uint64_t max_wait = atomic_load_explicit(&max_lock_wait_ns, memory_order_relaxed);
    uint64_t total_w_ticks = atomic_load_explicit(&total_wait_ticks, memory_order_relaxed);
    int max_w_ticks = atomic_load_explicit(&max_wait_ticks, memory_order_relaxed);
    
    pthread_mutex_lock(&tick_lock);
    int sim_clock = global_tick;
    pthread_mutex_unlock(&tick_lock);

    printf("\n=========================================\n");
    printf("             SYSTEM METRICS              \n");
    printf("=========================================\n");
    printf("Total Transactions Processed : %d\n", total_tx);
    printf("  - Successful (Committed)   : %d\n", succ);
    printf("  - Failed (Aborted)         : %d\n", fail);
    printf("Real Execution Time          : %.6f seconds\n", real_elapsed_sec);
    printf("Real Throughput              : %.2f transactions/second\n", 
           real_elapsed_sec > 0 ? (total_tx / real_elapsed_sec) : 0);
    printf("Final Global Clock (Ticks)   : %d\n", sim_clock);
    printf("Simulated Throughput         : %.2f transactions/tick\n", 
           sim_clock > 0 ? (total_tx / (double)sim_clock) : 0);
    printf("Lock Wait Times:\n");
    printf("  - Total Lock Wait Time     : %.3f ms\n", total_wait / 1000000.0);
    printf("  - Average Lock Wait/Tx     : %.3f ms\n", total_tx > 0 ? (total_wait / (double)total_tx) / 1000000.0 : 0.0);
    printf("  - Maximum Lock Wait        : %.3f ms\n", max_wait / 1000000.0);
    printf("Transaction Delay (Wait Ticks):\n");
    printf("  - Average Wait Delay       : %.2f ticks\n", total_tx > 0 ? (total_w_ticks / (double)total_tx) : 0.0);
    printf("  - Maximum Wait Delay       : %d ticks\n", max_w_ticks);
    printf("=========================================\n\n");

    printf("Final Account Balances (Centavos):\n");
    int64_t total_bank_balance = 0;
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        if (bank.accounts[i].balance_centavos != 100000 || i < 10) {
            printf("  Account %2d: %10d centavos\n", i, bank.accounts[i].balance_centavos);
        }
        total_bank_balance += bank.accounts[i].balance_centavos;
    }
    printf("Total Money in Bank          : %ld centavos\n", total_bank_balance);
    printf("=========================================\n");

    // Clean up resources
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        pthread_rwlock_destroy(&bank.accounts[i].lock);
    }
    pthread_mutex_destroy(&bank.bank_lock);
    destroy_buffer(&tx_queue);
    destroy_buffer_pool(&buffer_pool);
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&tick_lock);
    pthread_cond_destroy(&tick_changed);
    sem_destroy(&worker_sem);
    pthread_mutex_destroy(&load_phase_lock);
    if (log_file) fclose(log_file);

    return EXIT_SUCCESS;
}