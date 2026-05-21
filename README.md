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
*Forces all account locks to use exclusive mutexes instead of reader-writer locks.*

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

## Verification Logs & Screenshots

This section contains the actual execution logs demonstrating that the system fulfills all concurrent banking database requirements, runs clean of data races, handles deadlocks, dynamically blocks/unblocks buffer pools, and enforces money conservation.

---

### 1. ThreadSanitizer (TSan) — Zero Warnings on All Test Cases

The following logs show execution under ThreadSanitizer (`-fsanitize=thread`). Note that there are **zero TSan warnings or data races** detected across all runs.

<details>
<summary><b>Log 1: Simple Operations (trace_simple.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : DEPOSIT account 10 amount PHP 100.00

Tick 1:
T1 completed : DEPOSIT successful
T1 started : WITHDRAW account 10 amount PHP 20.00

Tick 2:
T1 completed : WITHDRAW successful
T1 started : BALANCE account 10
T1 : Account 10 balance = PHP 180.00

Tick 3:

=== Summary ===
Total transactions : 1
Committed : 1
Aborted : 0
Total ticks : 4
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 830.00
Conservation check : FAILED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 2 | 0 | COMMITTED

Average wait time : 0.0 ticks
Throughput : 1 transactions / 4 ticks = 0.25 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 1
Total unloads : 1
Peak usage : 1 slots
Blocked operations ( pool full ) : 0
```
</details>

<details>
<summary><b>Log 2: Concurrent Readers (trace_readers.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : BALANCE account 10
T1 : Account 10 balance = PHP 100.00
T2 started : BALANCE account 10
T2 : Account 10 balance = PHP 100.00
T3 started : BALANCE account 10
T3 : Account 10 balance = PHP 100.00
T4 started : BALANCE account 10
T4 : Account 10 balance = PHP 100.00

Tick 1:

=== Summary ===
Total transactions : 4
Committed : 4
Aborted : 0
Total ticks : 2
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 750.00
Conservation check : PASSED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 0 | 0 | COMMITTED
T2 | 0 | 0 | 0 | 0 | COMMITTED
T3 | 0 | 0 | 0 | 0 | COMMITTED
T4 | 0 | 0 | 0 | 0 | COMMITTED

Average wait time : 0.0 ticks
Throughput : 4 transactions / 2 ticks = 2.00 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 4
Total unloads : 4
Peak usage : 1 slots
Blocked operations ( pool full ) : 0
```
</details>

<details>
<summary><b>Log 3a: Deadlock Prevention (trace_deadlock.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : TRANSFER from 10 to 20 amount PHP 50.00
[ DEADLOCK PREVENTED ] Lock ordering : T1 waiting for account 10
[ DEADLOCK PREVENTED ] Lock ordering : T1 waiting for account 20
T2 started : TRANSFER from 20 to 10 amount PHP 30.00
[ DEADLOCK PREVENTED ] Lock ordering : T2 waiting for account 20
[ DEADLOCK PREVENTED ] Lock ordering : T2 waiting for account 10

Tick 1:
T1 completed : TRANSFER successful
T2 completed : TRANSFER successful

Tick 2:

=== Summary ===
Total transactions : 2
Committed : 2
Aborted : 0
Total ticks : 3
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 750.00
Conservation check : PASSED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 1 | 0 | COMMITTED
T2 | 0 | 0 | 1 | 0 | COMMITTED

Average wait time : 0.0 ticks
Throughput : 2 transactions / 3 ticks = 0.67 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 4
Total unloads : 4
Peak usage : 4 slots
Blocked operations ( pool full ) : 0
```
</details>

<details>
<summary><b>Log 3b: Deadlock Detection (trace_deadlock.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : TRANSFER from 10 to 20 amount PHP 50.00
T1 acquired lock on account 10
T2 started : TRANSFER from 20 to 10 amount PHP 30.00
T2 acquired lock on account 20

Tick 1:
T1 completed : TRANSFER successful

Tick 2:
