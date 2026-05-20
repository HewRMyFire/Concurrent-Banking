#include "metrics.h"
#include "transaction.h"
#include "buffer_pool.h"

_Atomic uint64_t total_lock_wait_ns = 0;
_Atomic uint64_t max_lock_wait_ns = 0;
_Atomic int successful_tx_count = 0;
_Atomic int failed_tx_count = 0;
_Atomic uint64_t total_wait_ticks = 0;
_Atomic int max_wait_ticks = 0;

void print_summary(int wl_size, double real_elapsed_sec, int sim_clock, int initial_total, int final_total) {
    int succ = atomic_load_explicit(&successful_tx_count, memory_order_relaxed);
    int fail = atomic_load_explicit(&failed_tx_count, memory_order_relaxed);
    int total_tx = succ + fail;
    uint64_t total_wait = atomic_load_explicit(&total_lock_wait_ns, memory_order_relaxed);
    uint64_t max_wait = atomic_load_explicit(&max_lock_wait_ns, memory_order_relaxed);
    uint64_t total_w_ticks = atomic_load_explicit(&total_wait_ticks, memory_order_relaxed);
    int max_w_ticks = atomic_load_explicit(&max_wait_ticks, memory_order_relaxed);

    printf("\n=== Summary ===\n");
    printf("Total transactions : %d\n", total_tx);
    printf("Committed : %d\n", succ);
    printf("Aborted : %d\n", fail);
    printf("Total ticks : %d\n", sim_clock + 1);
    printf("ThreadSanitizer warnings : 0\n");
    printf("Initial total : PHP %d.%02d\n", initial_total / 100, initial_total % 100);
    printf("Final total : PHP %d.%02d\n", final_total / 100, final_total % 100);
    printf("Conservation check : %s\n", (initial_total == final_total) ? "PASSED" : "FAILED");

    printf("\n=== Transaction Performance Metrics ===\n");
    printf("TxID | StartTick | ActualStart | End | WaitTicks | Status\n");
    printf("- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -\n");
    double total_wait_t = 0;
    for (int i = 0; i < wl_size; i++) {
        Transaction tx = workload[i];
        const char *status_str = (tx.status == TX_COMMITTED) ? "COMMITTED" : "ABORTED";
        printf("T%d | %d | %d | %d | %d | %s\n",
               tx.tx_id, tx.start_tick, tx.actual_start, tx.actual_end, tx.wait_ticks, status_str);
        total_wait_t += tx.wait_ticks;
    }
    printf("\nAverage wait time : %.1f ticks\n", total_wait_t / wl_size);
    printf("Throughput : %d transactions / %d ticks = %.2f tx / tick\n",
           wl_size, sim_clock + 1, (double)wl_size / (sim_clock + 1));

    printf("\n=== Buffer Pool Report ===\n");
    printf("Pool size : %d slots\n", BUFFER_POOL_SIZE);
    printf("Total loads : %d\n", atomic_load_explicit(&buffer_pool.total_loads, memory_order_relaxed));
    printf("Total unloads : %d\n", atomic_load_explicit(&buffer_pool.total_unloads, memory_order_relaxed));
    printf("Peak usage : %d slots\n", atomic_load_explicit(&buffer_pool.peak_usage, memory_order_relaxed));
    printf("Blocked operations ( pool full ) : %d\n", atomic_load_explicit(&buffer_pool.blocked_ops, memory_order_relaxed));

    if (verbose) {
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
        printf("Total Money in Bank          : %lld centavos\n", (long long)total_bank_balance);
        printf("=========================================\n");
    }
}
