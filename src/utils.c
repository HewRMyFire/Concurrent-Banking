#include "utils.h"
#include "transaction.h"

const char* deadlock_strategy_global = "prevention";
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
FILE* log_file = NULL;
int tick_interval_ms = 100;
bool verbose = false;

uint64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
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
