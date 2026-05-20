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

// Globals
Bank bank;
BoundedBuffer tx_queue;
pthread_mutex_t log_mutex;
FILE* log_file = NULL;

Transaction workload[MAX_WORKLOAD_SIZE];
int workload_size = 0;

volatile int global_tick = 0;
pthread_mutex_t tick_lock;
pthread_cond_t tick_changed;
_Atomic bool simulation_running = true;

_Atomic uint64_t total_lock_wait_ns = 0;
_Atomic uint64_t max_lock_wait_ns = 0;
_Atomic int successful_tx_count = 0;
_Atomic int failed_tx_count = 0;
_Atomic uint64_t total_wait_ticks = 0;
_Atomic int max_wait_ticks = 0;

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

typedef struct { 
    int id; 
    bool needs_write; 
} LockReq;

int compare_locks(const void* a, const void* b) {
    return ((LockReq*)a)->id - ((LockReq*)b)->id;
}

int get_local_index(LockReq* unique_reqs, int u_count, int account_id) {
    for (int i = 0; i < u_count; i++) {
        if (unique_reqs[i].id == account_id) {
            return i;
        }
    }
    return -1;
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

void execute_transaction(Transaction* tx) {
    // Wait until start_tick is reached
    wait_until_tick(tx->start_tick);

    pthread_mutex_lock(&tick_lock);
    tx->actual_start = global_tick;
    pthread_mutex_unlock(&tick_lock);

    tx->wait_ticks = tx->actual_start - tx->start_tick;
    tx->status = TX_RUNNING;
    tx->thread = pthread_self();

    LockReq reqs[256 * 2]; // 256 ops max, each can access at most 2 accounts (for TRANSFER)
    int num_reqs = 0;

    // Collect all lock requests needed by this transaction
    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        if (op.type == OP_DEPOSIT) {
            if (op.account_id >= 0 && op.account_id < MAX_ACCOUNTS) {
                reqs[num_reqs++] = (LockReq){op.account_id, true};
            }
        } else if (op.type == OP_WITHDRAW) {
            if (op.account_id >= 0 && op.account_id < MAX_ACCOUNTS) {
                reqs[num_reqs++] = (LockReq){op.account_id, true};
            }
        } else if (op.type == OP_TRANSFER) {
            if (op.account_id >= 0 && op.account_id < MAX_ACCOUNTS) {
                reqs[num_reqs++] = (LockReq){op.account_id, true};
            }
            if (op.target_account >= 0 && op.target_account < MAX_ACCOUNTS) {
                reqs[num_reqs++] = (LockReq){op.target_account, true};
            }
        } else if (op.type == OP_BALANCE) {
            if (op.account_id >= 0 && op.account_id < MAX_ACCOUNTS) {
                reqs[num_reqs++] = (LockReq){op.account_id, false};
            }
        }
    }

    // Deadlock Prevention: Sort locks by account ID (Resource Ordering)
    qsort(reqs, num_reqs, sizeof(LockReq), compare_locks);

    // Merge duplicate locks (escalating read locks to write locks if needed)
    LockReq unique_reqs[256 * 2];
    int u_count = 0;
    for (int i = 0; i < num_reqs; i++) {
        if (u_count == 0 || unique_reqs[u_count - 1].id != reqs[i].id) {
            unique_reqs[u_count++] = reqs[i];
        } else if (reqs[i].needs_write) {
            unique_reqs[u_count - 1].needs_write = true;
        }
    }

    // Acquire locks in sorted order
    uint64_t start_wait = get_time_ns();
    for (int i = 0; i < u_count; i++) {
        int id = unique_reqs[i].id;
        if (unique_reqs[i].needs_write) {
            pthread_rwlock_wrlock(&bank.accounts[id].lock);
        } else {
            pthread_rwlock_rdlock(&bank.accounts[id].lock);
        }
    }
    uint64_t end_wait = get_time_ns();
    uint64_t tx_wait_ns = end_wait - start_wait;

    // Track total lock wait time
    atomic_fetch_add_explicit(&total_lock_wait_ns, tx_wait_ns, memory_order_relaxed);

    // Track maximum lock wait time using CAS loop
    uint64_t cur_max = atomic_load_explicit(&max_lock_wait_ns, memory_order_relaxed);
    while (tx_wait_ns > cur_max) {
        if (atomic_compare_exchange_weak_explicit(&max_lock_wait_ns, &cur_max, tx_wait_ns,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            break;
        }
    }

    // Atomicity and Rollback: Copy balances to a local workspace
    int64_t local_balances[256 * 2];
    for (int i = 0; i < u_count; i++) {
        local_balances[i] = bank.accounts[unique_reqs[i].id].balance_centavos;
    }

    bool tx_success = true;
    char fail_reason[256] = "";

    // 1. Validation: check if all accounts in the transaction are valid
    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        if (op.type == OP_DEPOSIT || op.type == OP_WITHDRAW || op.type == OP_BALANCE) {
            if (op.account_id < 0 || op.account_id >= MAX_ACCOUNTS) {
                tx_success = false;
                snprintf(fail_reason, sizeof(fail_reason), "Invalid account %d", op.account_id);
                break;
            }
        } else if (op.type == OP_TRANSFER) {
            if (op.account_id < 0 || op.account_id >= MAX_ACCOUNTS) {
                tx_success = false;
                snprintf(fail_reason, sizeof(fail_reason), "Invalid src account %d", op.account_id);
                break;
            }
            if (op.target_account < 0 || op.target_account >= MAX_ACCOUNTS) {
                tx_success = false;
                snprintf(fail_reason, sizeof(fail_reason), "Invalid dest account %d", op.target_account);
                break;
            }
        }
    }

    // 2. Execution Simulation: perform operations on local workspace copies
    if (tx_success) {
        for (int i = 0; i < tx->num_ops; i++) {
            Operation op = tx->ops[i];
            if (op.type == OP_DEPOSIT) {
                int idx = get_local_index(unique_reqs, u_count, op.account_id);
                local_balances[idx] += op.amount_centavos;
            } else if (op.type == OP_WITHDRAW) {
                int idx = get_local_index(unique_reqs, u_count, op.account_id);
                if (local_balances[idx] < op.amount_centavos) {
                    tx_success = false;
                    snprintf(fail_reason, sizeof(fail_reason), "Insufficient funds on account %d (has %ld, needs %d)", 
                             op.account_id, local_balances[idx], op.amount_centavos);
                    break;
                }
                local_balances[idx] -= op.amount_centavos;
            } else if (op.type == OP_TRANSFER) {
                int idx_from = get_local_index(unique_reqs, u_count, op.account_id);
                int idx_to = get_local_index(unique_reqs, u_count, op.target_account);
                if (local_balances[idx_from] < op.amount_centavos) {
                    tx_success = false;
                    snprintf(fail_reason, sizeof(fail_reason), "Insufficient funds on account %d for transfer (has %ld, needs %d)", 
                             op.account_id, local_balances[idx_from], op.amount_centavos);
                    break;
                }
                local_balances[idx_from] -= op.amount_centavos;
                local_balances[idx_to] += op.amount_centavos;
            } else if (op.type == OP_BALANCE) {
                // Read balance (no restrictions)
            }
        }
    }

    // 3. Commit Phase: copy local balances back if successful
    if (tx_success) {
        for (int i = 0; i < u_count; i++) {
            bank.accounts[unique_reqs[i].id].balance_centavos = local_balances[i];
        }
        tx->status = TX_COMMITTED;
        atomic_fetch_add_explicit(&successful_tx_count, 1, memory_order_relaxed);
    } else {
        tx->status = TX_ABORTED;
        atomic_fetch_add_explicit(&failed_tx_count, 1, memory_order_relaxed);
    }

    pthread_mutex_lock(&tick_lock);
    tx->actual_end = global_tick;
    int clock_val = global_tick;
    pthread_mutex_unlock(&tick_lock);

    // Track wait ticks metrics
    atomic_fetch_add_explicit(&total_wait_ticks, tx->wait_ticks, memory_order_relaxed);
    int cur_max_ticks = atomic_load_explicit(&max_wait_ticks, memory_order_relaxed);
    while (tx->wait_ticks > cur_max_ticks) {
        if (atomic_compare_exchange_weak_explicit(&max_wait_ticks, &cur_max_ticks, tx->wait_ticks,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            break;
        }
    }

    // Thread-safe Logging (Done before release to maintain consistency)
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
            int idx = get_local_index(unique_reqs, u_count, op.account_id);
            int64_t bal = (idx != -1) ? local_balances[idx] : -1;
            offset += snprintf(op_details + offset, sizeof(op_details) - offset,
                              "BAL(%d: %ld) ", op.account_id, bal);
        }
    }

    const char* status_str = (tx->status == TX_COMMITTED) ? "\033[1;32mCOMMITTED\033[0m" : "\033[1;31mABORTED\033[0m";
    const char* status_file_str = (tx->status == TX_COMMITTED) ? "COMMITTED" : "ABORTED";

    printf("[Clock: %4d] Worker %lu processed TX %3d -> %s [Sched:%d, Actual:%d-%d, Wait:%d ticks] [Ops: %s]\n",
           clock_val, (unsigned long)tx->thread, tx->tx_id, status_str, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks, op_details);
    if (log_file) {
        fprintf(log_file, "[Clock: %d] Worker %lu processed TX %d -> %s [Sched:%d, Actual:%d-%d, Wait:%d ticks] [Ops: %s]\n",
                clock_val, (unsigned long)tx->thread, tx->tx_id, status_file_str, tx->start_tick, tx->actual_start, tx->actual_end, tx->wait_ticks, op_details);
    }
    fflush(stdout);
    if (log_file) fflush(log_file);
    pthread_mutex_unlock(&log_mutex);

    // Release locks in reverse order
    for (int i = u_count - 1; i >= 0; i--) {
        pthread_rwlock_unlock(&bank.accounts[unique_reqs[i].id].lock);
    }
}

void* worker_thread(void* arg) {
    (void)arg;
    Transaction tx;
    while (1) {
        fetch_transaction(&tx_queue, &tx);
        if (tx.tx_id == -1) {
            break; // Poison pill exits the worker
        }
        execute_transaction(&tx);
    }
    return NULL;
}

void init_bank() {
    pthread_mutex_init(&bank.bank_lock, NULL);
    bank.num_accounts = MAX_ACCOUNTS;
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        bank.accounts[i].account_id = i;
        bank.accounts[i].balance_centavos = 100000; // Default initial balance: 100,000 centavos ($1,000.00)
        pthread_rwlock_init(&bank.accounts[i].lock, NULL);
    }
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
        int num_ops = 0;
        int start_tick = 0;
        if (sscanf(ptr, "TX %d %d %d", &tx_id, &num_ops, &start_tick) == 3) {
            if (workload_size >= MAX_WORKLOAD_SIZE) {
                fprintf(stderr, "Workload limit reached (%d)\n", MAX_WORKLOAD_SIZE);
                break;
            }

            Transaction* tx = &workload[workload_size];
            tx->tx_id = tx_id;
            tx->num_ops = 0;
            tx->start_tick = start_tick;
            tx->status = TX_RUNNING;

            if (num_ops > 256) {
                fprintf(stderr, "Warning: TX %d has %d operations (max 256). Truncating.\n", tx_id, num_ops);
                num_ops = 256;
            }

            for (int i = 0; i < num_ops; i++) {
                char op_line[256];
                bool op_read = false;
                while (fgets(op_line, sizeof(op_line), file)) {
                    char* op_ptr = op_line;
                    while (isspace((unsigned char)*op_ptr)) op_ptr++;
                    if (*op_ptr == '\0' || *op_ptr == '#') continue;

                    char op_name[32];
                    Operation op;
                    memset(&op, 0, sizeof(op));

                    if (sscanf(op_ptr, "%31s", op_name) == 1) {
                        if (strcmp(op_name, "DEPOSIT") == 0) {
                            op.type = OP_DEPOSIT;
                            if (sscanf(op_ptr, "DEPOSIT %d %d", &op.account_id, &op.amount_centavos) == 2) {
                                tx->ops[tx->num_ops++] = op;
                                op_read = true;
                            }
                        } else if (strcmp(op_name, "WITHDRAW") == 0) {
                            op.type = OP_WITHDRAW;
                            if (sscanf(op_ptr, "WITHDRAW %d %d", &op.account_id, &op.amount_centavos) == 2) {
                                tx->ops[tx->num_ops++] = op;
                                op_read = true;
                            }
                        } else if (strcmp(op_name, "TRANSFER") == 0) {
                            op.type = OP_TRANSFER;
                            if (sscanf(op_ptr, "TRANSFER %d %d %d", &op.account_id, &op.target_account, &op.amount_centavos) == 3) {
                                tx->ops[tx->num_ops++] = op;
                                op_read = true;
                            }
                        } else if (strcmp(op_name, "BALANCE") == 0) {
                            op.type = OP_BALANCE;
                            if (sscanf(op_ptr, "BALANCE %d", &op.account_id) == 1) {
                                tx->ops[tx->num_ops++] = op;
                                op_read = true;
                            }
                        } else {
                            fprintf(stderr, "Unknown operation: %s\n", op_name);
                        }
                    }
                    if (op_read) break;
                }
            }
            workload_size++;
        }
    }

    fclose(file);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <trace_file_path> [num_workers]\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char* trace_path = argv[1];
    int num_workers = NUM_WORKERS;
    if (argc >= 3) {
        num_workers = atoi(argv[2]);
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

    printf("=========================================\n");
    printf("Initializing Concurrent BankDB...\n");
    printf("  Workers     : %d\n", num_workers);
    printf("  Max Accounts: %d\n", MAX_ACCOUNTS);
    printf("  Buffer Size : %d\n", BUFFER_SIZE);
    printf("=========================================\n");

    init_bank();
    init_buffer(&tx_queue);

    printf("Loading workload from %s...\n", trace_path);
    load_trace(trace_path);

    uint64_t real_start_ns = get_time_ns();

    pthread_t timer;
    pthread_t* workers = malloc(num_workers * sizeof(pthread_t));
    if (!workers) {
        perror("Failed to allocate worker threads");
        if (log_file) fclose(log_file);
        return EXIT_FAILURE;
    }

    pthread_create(&timer, NULL, timer_thread, NULL);

    for (int i = 0; i < num_workers; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    // Submit workload transactions to queue immediately
    printf("Submitting workload transactions to bounded queue...\n");
    for (int i = 0; i < workload_size; i++) {
        submit_transaction(&tx_queue, workload[i]);
    }

    // Submit poison pills to terminate worker threads
    for (int i = 0; i < num_workers; i++) {
        Transaction pill;
        pill.tx_id = -1;
        pill.num_ops = 0;
        submit_transaction(&tx_queue, pill);
    }

    // Join workers
    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }

    // Stop timer thread
    atomic_store_explicit(&simulation_running, false, memory_order_relaxed);
    
    // Broadcast one final time to wake up any threads blocked (though they should be done)
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
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&tick_lock);
    pthread_cond_destroy(&tick_changed);
    if (log_file) fclose(log_file);
    free(workers);

    return EXIT_SUCCESS;
}