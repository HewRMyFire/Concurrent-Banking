#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "utils.h"

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
    _Atomic TxStatus status;
} Transaction;

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
    OpType type;
    int account_id;
    int amount_centavos;
    int target_account;
} CompletedOp;

extern BoundedBuffer tx_queue;
extern sem_t worker_sem;
extern Transaction workload[MAX_WORKLOAD_SIZE];
extern int workload_size;

void init_buffer(BoundedBuffer* b);
void destroy_buffer(BoundedBuffer* b);
void submit_transaction(BoundedBuffer* b, Transaction tx);
void fetch_transaction(BoundedBuffer* b, Transaction* tx);
void* execute_transaction(void* arg);
void* dispatcher_thread(void* arg);
void load_trace(const char* filepath);

#endif // TRANSACTION_H
