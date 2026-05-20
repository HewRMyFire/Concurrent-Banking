# Concurrent Banking Database (`bankdb`)

A modular, high-performance in-memory concurrent banking database engine featuring logical clock tick synchronization, dynamic thread dispatching, lock abstraction (Reader-Writer vs Mutex), deadlock prevention & detection (with rollback), and a bounded buffer pool manager.

## Group Members

- **Matthew F. Simpas**
- **Rainier RJ Espinal**

---

## Implemented Features

1. **Simulated Logical Tick Clock**: Runs on a background thread advancing global ticks deterministically, signaling worker threads to simulate exact concurrent schedules.
2. **Dynamic Transaction Dispatcher**: Feeds transactions into a thread-safe bounded queue (size 50) using semaphores, dispatching them dynamically to workers up to a specified worker pool limit.
3. **Lock Abstraction**: Supports compile-time locking toggle between `pthread_rwlock_t` and `pthread_mutex_t` using `-DUSE_PLAIN_MUTEX`.
4. **Deadlock Management**:
   - **Prevention**: Enforces strict ascending lock ordering on account IDs to eliminate the Coffman Circular Wait condition.
   - **Detection**: Builds a transaction wait-for graph, detects dependency cycles using depth-first search (DFS), aborts the current transaction, and rolls back all finished steps in reverse order.
5. **Bounded Buffer Pool**: Limits concurrent account loads to exactly `5` slots using semaphores. Tracks load/unload counts, peak usage, and pool-full blocks.
6. **Funds Conservation**: Validates all updates and verifies that total balance across all accounts remains constant at database shutdown.
7. **Performance Diagnostics**: Generates tables of execution timing, global tick latency, real execution seconds, throughput (transactions/tick and transactions/second), lock waiting averages, and buffer pool diagnostics.

---

## Compilation Instructions

The modular code builds on any standard Linux/POSIX or WSL environment.

### Compile Release Binary
```bash
make clean
make
```
*Compiles the release version of `bankdb` under `-O2` with full compiler warnings enabled.*

### Compile with Reader-Writer Locks Disabled (Plain Mutex Mode)
```bash
make clean
CFLAGS="-DUSE_PLAIN_MUTEX" make
```
*Forces all account locks to use exclusive mutextes instead of reader-writer locks.*

### Compile Debug / ThreadSanitizer Binary
```bash
make clean
make debug
```
*Compiles the `bankdb` binary with `-fsanitize=thread` and `-g` flags to audit data races.*

### Clean Generated Artifacts
```bash
make clean
```

---

## Usage Instructions

Run the compiled executable with the necessary parameters:

```bash
./bankdb --accounts=FILE --trace=FILE --deadlock=prevention|detection [OPTIONS]
```

### Required Options:
- `--accounts=FILE`: Path to the initial account balances text file.
- `--trace=FILE`: Path to the transaction operations workload trace file.
- `--deadlock=prevention|detection`: Select deadlock prevention (lock ordering) or deadlock detection (wait-for graph cycle rollback).

### Optional Arguments:
- `--tick-ms=N`: Set the duration of a simulated global clock tick in milliseconds (default: `100`).
- `--workers=N`: Set the size of the worker thread pool (default: `4`).
- `--verbose`: Enable detailed execution traces, final account balance dump, and real-world execution metrics.

---

## Running Verification Tests

Run all 5 provided test cases sequentially:

```bash
make test
```

*Note: For testing with ThreadSanitizer under WSL 2, disable ASLR to avoid mapping compatibility errors:*
```bash
make debug
setarch x86_64 -R ./bankdb --accounts=tests/accounts.txt --trace=tests/trace_deadlock.txt --deadlock=detection
```

---

## Known Limitations

- **Account Index Limit**: The system supports up to 100 accounts (`MAX_ACCOUNTS = 100`) indexed from `0` to `99`. Account IDs outside this range are ignored by the loader.
- **Transaction Size**: A single transaction holds up to 256 operations (`MAX_OPS = 256`).
- **Workload Capacity**: The queue limits total transaction records to 10,000 (`MAX_WORKLOAD_SIZE = 10000`).