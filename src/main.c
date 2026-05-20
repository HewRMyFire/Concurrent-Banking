#include "utils.h"
#include "bank.h"
#include "transaction.h"
#include "timer.h"
#include "lock_mgr.h"
#include "buffer_pool.h"
#include "metrics.h"

int main(int argc, char* argv[]) {
    const char* accounts_path = NULL;
    const char* trace_path = NULL;
    const char* deadlock_strategy = NULL;
    int num_workers = NUM_WORKERS;
    int initial_total = 0;
    int final_total = 0;

    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--accounts=", 11) == 0) {
            accounts_path = argv[i] + 11;
        } else if (strncmp(argv[i], "--trace=", 8) == 0) {
            trace_path = argv[i] + 8;
        } else if (strncmp(argv[i], "--deadlock=", 11) == 0) {
            deadlock_strategy = argv[i] + 11;
        } else if (strncmp(argv[i], "--tick-ms=", 10) == 0) {
            tick_interval_ms = atoi(argv[i] + 10);
        } else if (strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (strncmp(argv[i], "--workers=", 10) == 0) {
            num_workers = atoi(argv[i] + 10);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return EXIT_FAILURE;
        }
    }

    if (!accounts_path || !trace_path || !deadlock_strategy) {
        fprintf(stderr, "Usage: %s --accounts=FILE --trace=FILE --deadlock=prevention|detection [--tick-ms=N] [--verbose]\n", argv[0]);
        return EXIT_FAILURE;
    }
    if (strcmp(deadlock_strategy, "prevention") != 0 && strcmp(deadlock_strategy, "detection") != 0) {
        fprintf(stderr, "Error: Invalid deadlock strategy '%s'. Must be 'prevention' or 'detection'.\n", deadlock_strategy);
        return EXIT_FAILURE;
    }
    deadlock_strategy_global = deadlock_strategy;

    log_file = fopen("bankdb.log", "w");
    if (!log_file) {
        perror("Failed to open bankdb.log");
    }

    pthread_mutex_init(&log_mutex, NULL);
    pthread_mutex_init(&tick_lock, NULL);
    pthread_cond_init(&tick_changed, NULL);
    sem_init(&worker_sem, 0, num_workers);
    pthread_mutex_init(&load_phase_lock, NULL);

    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        owner_tx_idx[i] = -1;
    }
    for (int i = 0; i < MAX_WORKLOAD_SIZE; i++) {
        waiting_for[i] = -1;
    }
    if (verbose) {
        printf("=========================================\n");
        printf("Initializing Concurrent BankDB...\n");
        printf("  Workers     : %d\n", num_workers);
        printf("  Max Accounts: %d\n", MAX_ACCOUNTS);
        printf("  Buffer Size : %d\n", BUFFER_SIZE);
        printf("  Pool Size   : %d\n", BUFFER_POOL_SIZE);
        printf("=========================================\n");
    }

    init_bank(accounts_path);
    init_buffer(&tx_queue);
    init_buffer_pool(&buffer_pool);

    if (verbose) {
        printf("Loading workload from %s...\n", trace_path);
    }
    load_trace(trace_path);

    uint64_t real_start_ns = get_time_ns();

    pthread_t timer;
    pthread_t dispatcher;

    pthread_mutex_lock(&log_mutex);
    printf("=== Banking System Execution Log ===\n");
    printf("Timer thread started ( tick interval : %d ms )\n\n", tick_interval_ms);
    printf("Tick 0:\n");
    fflush(stdout);
    pthread_mutex_unlock(&log_mutex);

    for (int i = 0; i < bank.num_accounts; i++) {
        initial_total += bank.accounts[i].balance_centavos;
    }

    pthread_create(&timer, NULL, timer_thread, NULL);
    pthread_create(&dispatcher, NULL, dispatcher_thread, NULL);

    // Submit workload transactions to queue immediately
    if (verbose) {
        printf("Submitting workload transactions to bounded queue...\n");
    }
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

    for (int i = 0; i < bank.num_accounts; i++) {
        final_total += bank.accounts[i].balance_centavos;
    }

    uint64_t real_end_ns = get_time_ns();
    double real_elapsed_sec = (real_end_ns - real_start_ns) / 1000000000.0;

    pthread_mutex_lock(&tick_lock);
    int sim_clock = global_tick;
    pthread_mutex_unlock(&tick_lock);

    print_summary(workload_size, real_elapsed_sec, sim_clock, initial_total, final_total);

    // Clean up resources
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        rwlock_destroy_func(&bank.accounts[i].lock);
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
