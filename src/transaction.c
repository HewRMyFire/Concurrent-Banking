#include "transaction.h"
#include "bank.h"
#include "timer.h"
#include "buffer_pool.h"
#include "metrics.h"
#include "lock_mgr.h"

BoundedBuffer tx_queue;
sem_t worker_sem;
Transaction workload[MAX_WORKLOAD_SIZE];
int workload_size = 0;

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

    tx -> wait_ticks = 0;
    int current_target_tick = tx -> actual_start;

    CompletedOp completed_ops[256];
    int completed_count = 0;

    for (int i = 0; i < tx -> num_ops ; i ++) {
        Operation * op = & tx -> ops [ i ];

        pthread_mutex_lock(&log_mutex);
        if (op->type == OP_DEPOSIT) {
            printf("T%d started : DEPOSIT account %d amount PHP %d.%02d\n",
                   tx->tx_id, op->account_id, op->amount_centavos / 100, op->amount_centavos % 100);
            if (log_file) {
                fprintf(log_file, "T%d started : DEPOSIT account %d amount PHP %d.%02d\n",
                        tx->tx_id, op->account_id, op->amount_centavos / 100, op->amount_centavos % 100);
            }
        } else if (op->type == OP_WITHDRAW) {
            printf("T%d started : WITHDRAW account %d amount PHP %d.%02d\n",
                   tx->tx_id, op->account_id, op->amount_centavos / 100, op->amount_centavos % 100);
            if (log_file) {
                fprintf(log_file, "T%d started : WITHDRAW account %d amount PHP %d.%02d\n",
                        tx->tx_id, op->account_id, op->amount_centavos / 100, op->amount_centavos % 100);
            }
        } else if (op->type == OP_TRANSFER) {
            printf("T%d started : TRANSFER from %d to %d amount PHP %d.%02d\n",
                   tx->tx_id, op->account_id, op->target_account, op->amount_centavos / 100, op->amount_centavos % 100);
            if (log_file) {
                fprintf(log_file, "T%d started : TRANSFER from %d to %d amount PHP %d.%02d\n",
                        tx->tx_id, op->account_id, op->target_account, op->amount_centavos / 100, op->amount_centavos % 100);
            }
        } else if (op->type == OP_BALANCE) {
            printf("T%d started : BALANCE account %d\n",
                   tx->tx_id, op->account_id);
            if (log_file) {
                fprintf(log_file, "T%d started : BALANCE account %d\n",
                        tx->tx_id, op->account_id);
            }
        }
        fflush(stdout);
        if (log_file) fflush(log_file);
        pthread_mutex_unlock(&log_mutex);

        pthread_mutex_lock(&tick_lock);
        int lock_start = global_tick ;
        pthread_mutex_unlock(&tick_lock);

        bool op_success = true;

        switch ( op -> type ) {
        case OP_DEPOSIT :
            deposit ( op -> account_id , op -> amount_centavos ) ;
            completed_ops[completed_count++] = (CompletedOp){OP_DEPOSIT, op->account_id, op->amount_centavos, 0};
            
            pthread_mutex_lock(&tick_lock);
            int lock_end_dep = global_tick;
            pthread_mutex_unlock(&tick_lock);
            tx->wait_ticks += (lock_end_dep - lock_start);
            current_target_tick += 1;

            wait_until_tick(current_target_tick + tx->wait_ticks);
            pthread_mutex_lock(&log_mutex);
            printf("T%d completed : DEPOSIT successful\n", tx->tx_id);
            if (log_file) {
                fprintf(log_file, "T%d completed : DEPOSIT successful\n", tx->tx_id);
            }
            fflush(stdout);
            if (log_file) fflush(log_file);
            pthread_mutex_unlock(&log_mutex);
            break ;

        case OP_WITHDRAW :
            if (! withdraw ( op -> account_id , op -> amount_centavos ) ) {
                op_success = false;
                
                pthread_mutex_lock(&tick_lock);
                int lock_end_wdr = global_tick;
                pthread_mutex_unlock(&tick_lock);
                tx->wait_ticks += (lock_end_wdr - lock_start);
                current_target_tick += 1;

                wait_until_tick(current_target_tick + tx->wait_ticks);
                pthread_mutex_lock(&log_mutex);
                printf("T%d completed : WITHDRAW failed\n", tx->tx_id);
                if (log_file) {
                    fprintf(log_file, "T%d completed : WITHDRAW failed\n", tx->tx_id);
                }
                fflush(stdout);
                if (log_file) fflush(log_file);
                pthread_mutex_unlock(&log_mutex);
            } else {
                completed_ops[completed_count++] = (CompletedOp){OP_WITHDRAW, op->account_id, op->amount_centavos, 0};

                pthread_mutex_lock(&tick_lock);
                int lock_end_wdr = global_tick;
                pthread_mutex_unlock(&tick_lock);
                tx->wait_ticks += (lock_end_wdr - lock_start);
                current_target_tick += 1;

                wait_until_tick(current_target_tick + tx->wait_ticks);
                pthread_mutex_lock(&log_mutex);
                printf("T%d completed : WITHDRAW successful\n", tx->tx_id);
                if (log_file) {
                    fprintf(log_file, "T%d completed : WITHDRAW successful\n", tx->tx_id);
                }
                fflush(stdout);
                if (log_file) fflush(log_file);
                pthread_mutex_unlock(&log_mutex);
            }
            break ;

        case OP_TRANSFER :
            if (! transfer ( op -> account_id , op -> target_account ,
                            op -> amount_centavos, tx - workload ) ) {
                op_success = false;

                pthread_mutex_lock(&tick_lock);
                int lock_end_trf = global_tick;
                pthread_mutex_unlock(&tick_lock);
                tx->wait_ticks += (lock_end_trf - lock_start);
                current_target_tick += 1;

                wait_until_tick(current_target_tick + tx->wait_ticks);
                pthread_mutex_lock(&log_mutex);
                printf("T%d completed : TRANSFER failed\n", tx->tx_id);
                if (log_file) {
                    fprintf(log_file, "T%d completed : TRANSFER failed\n", tx->tx_id);
                }
                fflush(stdout);
                if (log_file) fflush(log_file);
                pthread_mutex_unlock(&log_mutex);
            } else {
                completed_ops[completed_count++] = (CompletedOp){OP_TRANSFER, op->account_id, op->amount_centavos, op->target_account};

                pthread_mutex_lock(&tick_lock);
                int lock_end_trf = global_tick;
                pthread_mutex_unlock(&tick_lock);
                tx->wait_ticks += (lock_end_trf - lock_start);
                current_target_tick += 1;

                wait_until_tick(current_target_tick + tx->wait_ticks);
                pthread_mutex_lock(&log_mutex);
                printf("T%d completed : TRANSFER successful\n", tx->tx_id);
                if (log_file) {
                    fprintf(log_file, "T%d completed : TRANSFER successful\n", tx->tx_id);
                }
                fflush(stdout);
                if (log_file) fflush(log_file);
                pthread_mutex_unlock(&log_mutex);
            }
            break ;

        case OP_BALANCE : {
            int balance = get_balance ( op -> account_id ) ;

            pthread_mutex_lock(&tick_lock);
            int lock_end_bal = global_tick;
            pthread_mutex_unlock(&tick_lock);
            tx->wait_ticks += (lock_end_bal - lock_start);

            pthread_mutex_lock(&log_mutex);
            printf ("T%d : Account %d balance = PHP %d.%02d\n",
                    tx -> tx_id , op -> account_id ,
                    balance / 100 , balance % 100) ;
            if (log_file) {
                fprintf(log_file, "T%d : Account %d balance = PHP %d.%02d\n",
                        tx -> tx_id , op -> account_id ,
                        balance / 100 , balance % 100) ;
            }
            fflush(stdout);
            if (log_file) fflush(log_file);
            pthread_mutex_unlock(&log_mutex);
            break ;
        }
        }

        if (!op_success) {
            // Rollback completed operations in reverse order
            for (int j = completed_count - 1; j >= 0; j--) {
                CompletedOp cop = completed_ops[j];
                if (cop.type == OP_DEPOSIT) {
                    withdraw(cop.account_id, cop.amount_centavos);
                } else if (cop.type == OP_WITHDRAW) {
                    deposit(cop.account_id, cop.amount_centavos);
                } else if (cop.type == OP_TRANSFER) {
                    transfer(cop.target_account, cop.account_id, cop.amount_centavos, tx - workload);
                }
            }

            tx -> status = TX_ABORTED ;

            pthread_mutex_lock(&tick_lock);
            tx->actual_end = global_tick;
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
