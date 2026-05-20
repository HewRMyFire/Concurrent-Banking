#ifndef LOCK_MGR_H
#define LOCK_MGR_H

#include "utils.h"

extern pthread_mutex_t deadlock_lock;
extern pthread_cond_t deadlock_cond;
extern int owner_tx_idx[MAX_ACCOUNTS];
extern int waiting_for[MAX_WORKLOAD_SIZE];

bool acquire_account_lock(int acc_id, int tx_idx);
void release_account_lock(int acc_id, int tx_idx);

#endif // LOCK_MGR_H
