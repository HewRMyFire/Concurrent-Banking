#include "lock_mgr.h"
#include "bank.h"
#include "transaction.h"

pthread_mutex_t deadlock_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t deadlock_cond = PTHREAD_COND_INITIALIZER;
int owner_tx_idx[MAX_ACCOUNTS];
int waiting_for[MAX_WORKLOAD_SIZE];

bool acquire_account_lock(int acc_id, int tx_idx) {
    if (strcmp(deadlock_strategy_global, "prevention") == 0) {
        rwlock_wrlock_func(&bank.accounts[acc_id].lock);
        return true;
    }

    pthread_mutex_lock(&deadlock_lock);
    while (owner_tx_idx[acc_id] != -1 && owner_tx_idx[acc_id] != tx_idx) {
        waiting_for[tx_idx] = acc_id;

        // Cycle check:
        int current_idx = tx_idx;
        bool cycle = false;
        while (true) {
            int needed_acc = waiting_for[current_idx];
            if (needed_acc == -1) break;
            int owner_idx = owner_tx_idx[needed_acc];
            if (owner_idx == -1) break;
            if (owner_idx == tx_idx) {
                cycle = true;
                break;
            }
            current_idx = owner_idx;
        }

        if (cycle) {
            pthread_mutex_lock(&log_mutex);
            printf("[ DEADLOCK DETECTED ] Cycle detected: T%d waiting for account %d (owned by T%d). Aborting T%d.\n",
                   workload[tx_idx].tx_id, acc_id, workload[owner_tx_idx[acc_id]].tx_id, workload[tx_idx].tx_id);
            if (log_file) {
                fprintf(log_file, "[ DEADLOCK DETECTED ] Cycle detected: T%d waiting for account %d (owned by T%d). Aborting T%d.\n",
                        workload[tx_idx].tx_id, acc_id, workload[owner_tx_idx[acc_id]].tx_id, workload[tx_idx].tx_id);
            }
            fflush(stdout);
            if (log_file) fflush(log_file);
            pthread_mutex_unlock(&log_mutex);

            waiting_for[tx_idx] = -1;
            pthread_mutex_unlock(&deadlock_lock);
            return false; // Signal deadlock / abort
        }

        pthread_cond_wait(&deadlock_cond, &deadlock_lock);
    }

    owner_tx_idx[acc_id] = tx_idx;
    waiting_for[tx_idx] = -1;
    pthread_mutex_unlock(&deadlock_lock);

    rwlock_wrlock_func(&bank.accounts[acc_id].lock);
    return true;
}

void release_account_lock(int acc_id, int tx_idx) {
    if (strcmp(deadlock_strategy_global, "prevention") == 0) {
        rwlock_unlock_func(&bank.accounts[acc_id].lock);
        return;
    }

    rwlock_unlock_func(&bank.accounts[acc_id].lock);

    pthread_mutex_lock(&deadlock_lock);
    if (owner_tx_idx[acc_id] == tx_idx) {
        owner_tx_idx[acc_id] = -1;
        pthread_cond_broadcast(&deadlock_cond);
    }
    pthread_mutex_unlock(&deadlock_lock);
}
