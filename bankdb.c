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

#define MAX_ACCOUNTS 100
#define MAX_OPS_PER_TX 10
#define BUFFER_SIZE 50
#define NUM_WORKERS 4

typedef enum { OP_DEPOSIT, OP_WITHDRAW, OP_TRANSFER, OP_BALANCE } OpType;

typedef struct {
    OpType type;
    int account_from; 
    int account_to;   
    int64_t amount;   
} Operation;

typedef struct {
    int tx_id;
    Operation ops[MAX_OPS_PER_TX];
    int num_ops;
} Transaction;

typedef struct {
    int id;
    int64_t balance;
    pthread_rwlock_t lock;
} Account;

typedef struct {
    Transaction buffer[BUFFER_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    sem_t empty_slots;
    sem_t filled_slots;
} BoundedBuffer;

Account bank[MAX_ACCOUNTS];
BoundedBuffer tx_queue;
_Atomic bool end_of_workload = false;

_Atomic uint64_t global_clock_ms = 0;
_Atomic uint64_t total_lock_wait_ns = 0;
_Atomic int successful_tx_count = 0;
_Atomic int failed_tx_count = 0;

uint64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

void init_buffer(BoundedBuffer* b) {
    b->head = 0; b->tail = 0; b->count = 0;
    pthread_mutex_init(&b->mutex, NULL);
    sem_init(&b->empty_slots, 0, BUFFER_SIZE);
    sem_init(&b->filled_slots, 0, 0);
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

bool fetch_transaction(BoundedBuffer* b, Transaction* tx) {
    while (sem_trywait(&b->filled_slots) != 0) {
        if (atomic_load_explicit(&end_of_workload, memory_order_seq_cst)) {
            if (b->count == 0) return false; 
        }
        usleep(1000);
    }

    pthread_mutex_lock(&b->mutex);
    
    *tx = b->buffer[b->head];
    b->head = (b->head + 1) % BUFFER_SIZE;
    b->count--;
    
    pthread_mutex_unlock(&b->mutex);
    sem_post(&b->empty_slots);
    return true;
}


typedef struct { int id; bool needs_write; } LockReq;

int compare_locks(const void* a, const void* b) {
    return ((LockReq*)a)->id - ((LockReq*)b)->id;
}

void execute_transaction(Transaction* tx) {
    LockReq reqs[MAX_OPS_PER_TX * 2];
    int num_reqs = 0;

    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        if (op.type == OP_BALANCE || op.type == OP_WITHDRAW || op.type == OP_TRANSFER) {
            reqs[num_reqs++] = (LockReq){op.account_from, op.type != OP_BALANCE};
        }
        if (op.type == OP_DEPOSIT || op.type == OP_TRANSFER) {
            reqs[num_reqs++] = (LockReq){op.account_to, true};
        }
    }

    qsort(reqs, num_reqs, sizeof(LockReq), compare_locks);

    LockReq unique_reqs[MAX_OPS_PER_TX * 2];
    int u_count = 0;
    for (int i = 0; i < num_reqs; i++) {
        if (u_count == 0 || unique_reqs[u_count-1].id != reqs[i].id) {
            unique_reqs[u_count++] = reqs[i];
        } else if (reqs[i].needs_write) {
            unique_reqs[u_count-1].needs_write = true;
        }
    }

    uint64_t start_wait = get_time_ns();
    for (int i = 0; i < u_count; i++) {
        if (unique_reqs[i].needs_write) {
            pthread_rwlock_wrlock(&bank[unique_reqs[i].id].lock);
        } else {
            pthread_rwlock_rdlock(&bank[unique_reqs[i].id].lock);
        }
    }
    uint64_t end_wait = get_time_ns();
    atomic_fetch_add_explicit(&total_lock_wait_ns, (end_wait - start_wait), memory_order_relaxed);

    bool tx_success = true;
    for (int i = 0; i < tx->num_ops; i++) {
        Operation op = tx->ops[i];
        if (op.type == OP_DEPOSIT) {
            bank[op.account_to].balance += op.amount;
        } 
        else if (op.type == OP_WITHDRAW) {
            if (bank[op.account_from].balance >= op.amount) {
                bank[op.account_from].balance -= op.amount;
            } else { tx_success = false; break; }
        } 
        else if (op.type == OP_TRANSFER) {
            if (bank[op.account_from].balance >= op.amount) {
                bank[op.account_from].balance -= op.amount;
                bank[op.account_to].balance += op.amount;
            } else { tx_success = false; break; }
        }
        else if (op.type == OP_BALANCE) {
        }
    }

    for (int i = u_count - 1; i >= 0; i--) {
        pthread_rwlock_unlock(&bank[unique_reqs[i].id].lock);
    }

    if (tx_success) atomic_fetch_add_explicit(&successful_tx_count, 1, memory_order_relaxed);
    else atomic_fetch_add_explicit(&failed_tx_count, 1, memory_order_relaxed);

    printf("[Clock: %lu ms] Worker %lu processed TX %d -> %s\n", 
           atomic_load_explicit(&global_clock_ms, memory_order_relaxed),
           pthread_self(), tx->tx_id, tx_success ? "SUCCESS" : "INSUFFICIENT FUNDS");
}


void* worker_thread(void* arg) {
    (void)arg;
    Transaction tx;
    while (fetch_transaction(&tx_queue, &tx)) {
        execute_transaction(&tx);
    }
    return NULL;
}

void* timer_thread(void* arg) {
    (void)arg;
    while (!atomic_load_explicit(&end_of_workload, memory_order_seq_cst)) {
        usleep(10000);
        atomic_fetch_add_explicit(&global_clock_ms, 10, memory_order_relaxed);
    }
    return NULL;
}


void init_bank() {
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        bank[i].id = i;
        bank[i].balance = 100000;
        pthread_rwlock_init(&bank[i].lock, NULL);
    }
}

void load_trace_and_produce() {
    Transaction txs[5];
    
    txs[0].tx_id = 100; txs[0].num_ops = 1;
    txs[0].ops[0] = (Operation){OP_TRANSFER, 0, 1, 5000}; 

    txs[1].tx_id = 101; txs[1].num_ops = 1;
    txs[1].ops[0] = (Operation){OP_TRANSFER, 1, 0, 2000};

    txs[2].tx_id = 102; txs[2].num_ops = 3;
    txs[2].ops[0] = (Operation){OP_WITHDRAW, 2, -1, 10000};
    txs[2].ops[1] = (Operation){OP_DEPOSIT, -1, 3, 10000};
    txs[2].ops[2] = (Operation){OP_BALANCE, 4, -1, 0};

    txs[3].tx_id = 103; txs[3].num_ops = 1;
    txs[3].ops[0] = (Operation){OP_WITHDRAW, 0, -1, 9999999};

    for(int i=0; i<4; i++) {
        submit_transaction(&tx_queue, txs[i]);
    }
    
    atomic_store_explicit(&end_of_workload, true, memory_order_seq_cst); 
}

int main() {
    printf("Initializing Concurrent BankDB...\n");
    init_bank();
    init_buffer(&tx_queue);

    pthread_t timer;
    pthread_t workers[NUM_WORKERS];

    pthread_create(&timer, NULL, timer_thread, NULL);

    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    printf("Loading workloads...\n");
    load_trace_and_produce();

    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(workers[i], NULL);
    }
    
    pthread_join(timer, NULL);

    printf("\n--- SYSTEM METRICS ---\n");
    printf("Total Transactions: %d\n", successful_tx_count + failed_tx_count);
    printf("  Successful: %d\n", successful_tx_count);
    printf("  Failed:     %d\n", failed_tx_count);
    printf("Total Lock Wait Time: %lu ns (%.3f ms)\n", 
            total_lock_wait_ns, total_lock_wait_ns / 1000000.0);
    printf("Final Global Clock:   %lu ms\n", global_clock_ms);

    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        pthread_rwlock_destroy(&bank[i].lock);
    }
    pthread_mutex_destroy(&tx_queue.mutex);
    sem_destroy(&tx_queue.empty_slots);
    sem_destroy(&tx_queue.filled_slots);

    return 0;
}