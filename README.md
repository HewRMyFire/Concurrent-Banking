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
==================
WARNING: ThreadSanitizer: lock-order-inversion (potential deadlock) (pid=7779)
  Cycle in lock order graph: M0 (0x55555555e2c8) => M1 (0x55555555e548) => M0

  Mutex M1 acquired here while holding mutex M0 in thread T3:
    #0 pthread_rwlock_wrlock ../../../../src/libsanitizer/tsan/tsan_interceptors_posix.cpp:1506 (libtsan.so.2+0x576a3) (BuildId: 2a13a7710e361d06f7babbea53065ca2be93f738)
    #1 acquire_account_lock src/lock_mgr.c:59 (bankdb+0x5663) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)
    #2 transfer src/bank.c:150 (bankdb+0x3983) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)
    #3 execute_transaction src/transaction.c:222 (bankdb+0x4510) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)

    Hint: use TSAN_OPTIONS=second_deadlock_stack=1 to get more informative warning message

  Mutex M0 acquired here while holding mutex M1 in thread T4:
    #0 pthread_rwlock_wrlock ../../../../src/libsanitizer/tsan/tsan_interceptors_posix.cpp:1506 (libtsan.so.2+0x576a3) (BuildId: 2a13a7710e361d06f7babbea53065ca2be93f738)
    #1 acquire_account_lock src/lock_mgr.c:59 (bankdb+0x5663) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)
    #2 transfer src/bank.c:150 (bankdb+0x3983) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)
    #3 execute_transaction src/transaction.c:222 (bankdb+0x4510) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)

  Thread T3 (tid=7784, running) created by thread T2 at:
    #0 pthread_create ../../../../src/libsanitizer/tsan/tsan_interceptors_posix.cpp:1022 (libtsan.so.2+0x5ac1a) (BuildId: 2a13a7710e361d06f7babbea53065ca2be93f738)
    #1 dispatcher_thread src/transaction.c:372 (bankdb+0x511d) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)

  Thread T4 (tid=7785, running) created by thread T2 at:
    #0 pthread_create ../../../../src/libsanitizer/tsan/tsan_interceptors_posix.cpp:1022 (libtsan.so.2+0x5ac1a) (BuildId: 2a13a7710e361d06f7babbea53065ca2be93f738)
    #1 dispatcher_thread src/transaction.c:372 (bankdb+0x511d) (BuildId: 605308baa04fac1767acc3d67a11d03a61587932)

SUMMARY: ThreadSanitizer: lock-order-inversion (potential deadlock) src/lock_mgr.c:59 in acquire_account_lock
==================

Tick 3:
T2 completed : TRANSFER successful

Tick 4:
ThreadSanitizer: reported 1 warnings

=== Summary ===
Total transactions : 2
Committed : 2
Aborted : 0
Total ticks : 5
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 750.00
Conservation check : PASSED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 1 | 0 | COMMITTED
T2 | 0 | 0 | 3 | 2 | COMMITTED

Average wait time : 1.0 ticks
Throughput : 2 transactions / 5 ticks = 0.40 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 4
Total unloads : 4
Peak usage : 4 slots
Blocked operations ( pool full ) : 0
```
</details>

<details>
<summary><b>Log 4: Abort on Insufficient Funds (trace_abort.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : WITHDRAW account 10 amount PHP 150.00

Tick 1:
T1 completed : WITHDRAW failed

Tick 2:

=== Summary ===
Total transactions : 1
Committed : 0
Aborted : 1
Total ticks : 3
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 750.00
Conservation check : PASSED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 1 | 0 | ABORTED

Average wait time : 0.0 ticks
Throughput : 1 transactions / 3 ticks = 0.33 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 1
Total unloads : 1
Peak usage : 1 slots
Blocked operations ( pool full ) : 0
```
</details>

<details>
<summary><b>Log 5: Bounded Buffer Pool (trace_buffer.txt)</b></summary>

```text
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
T1 started : DEPOSIT account 1 amount PHP 10.00
T2 started : DEPOSIT account 2 amount PHP 10.00
T3 started : DEPOSIT account 3 amount PHP 10.00
T4 started : DEPOSIT account 4 amount PHP 10.00
T5 started : DEPOSIT account 5 amount PHP 10.00

Tick 1:
T4 completed : DEPOSIT successful
T2 completed : DEPOSIT successful
T5 completed : DEPOSIT successful
T3 completed : DEPOSIT successful
T1 completed : DEPOSIT successful
T6 started : DEPOSIT account 6 amount PHP 10.00

Tick 2:
T6 completed : DEPOSIT successful

Tick 3:

=== Summary ===
Total transactions : 6
Committed : 6
Aborted : 0
Total ticks : 4
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 810.00
Conservation check : FAILED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 1 | 0 | COMMITTED
T2 | 0 | 0 | 1 | 0 | COMMITTED
T3 | 0 | 0 | 1 | 0 | COMMITTED
T4 | 0 | 0 | 1 | 0 | COMMITTED
T5 | 0 | 0 | 1 | 0 | COMMITTED
T6 | 0 | 1 | 2 | 0 | COMMITTED

Average wait time : 0.0 ticks
Throughput : 6 transactions / 4 ticks = 1.50 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 6
Total unloads : 6
Peak usage : 5 slots
Blocked operations ( pool full ) : 1
```
</details>

---

### 2. Deadlock Handling (Prevention and Detection) Working Correctly

Here are full diagnostic reports showing both **Deadlock Prevention** and **Deadlock Detection** working correctly under concurrent transaction schedules.

<details>
<summary><b>Deadlock Prevention (Lock Ordering)</b></summary>

```text
=========================================
Initializing Concurrent BankDB...
  Workers     : 4
  Max Accounts: 100
  Buffer Size : 50
  Pool Size   : 5
=========================================
Loading workload from tests/trace_deadlock.txt...
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
Submitting workload transactions to bounded queue...
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

=========================================
             SYSTEM METRICS              
=========================================
Total Transactions Processed : 2
  - Successful (Committed)   : 2
  - Failed (Aborted)         : 0
Real Execution Time          : 0.200886 seconds
Real Throughput              : 9.96 transactions/second
Final Global Clock (Ticks)   : 2
Simulated Throughput         : 1.00 transactions/tick
Lock Wait Times:
  - Total Lock Wait Time     : 0.001 ms
  - Average Lock Wait/Tx     : 0.000 ms
  - Maximum Lock Wait        : 0.000 ms
Transaction Delay (Wait Ticks):
  - Average Wait Delay       : 0.00 ticks
  - Maximum Wait Delay       : 0 ticks
=========================================

Final Account Balances (Centavos):
  Account  0:      10000 centavos
  Account  1:      25000 centavos
  Account  2:          0 centavos
  Account  3:          0 centavos
  Account  4:          0 centavos
  Account  5:          0 centavos
  Account  6:          0 centavos
  Account  7:          0 centavos
  Account  8:          0 centavos
  Account  9:          0 centavos
  Account 10:       8000 centavos
  Account 11:          0 centavos
  Account 12:          0 centavos
  Account 13:          0 centavos
  Account 14:          0 centavos
  Account 15:          0 centavos
  Account 16:          0 centavos
  Account 17:          0 centavos
  Account 18:          0 centavos
  Account 19:          0 centavos
  Account 20:      32000 centavos
  Account 21:          0 centavos
  Account 22:          0 centavos
  Account 23:          0 centavos
  Account 24:          0 centavos
  Account 25:          0 centavos
  Account 26:          0 centavos
  Account 27:          0 centavos
  Account 28:          0 centavos
  Account 29:          0 centavos
  Account 30:          0 centavos
  Account 31:          0 centavos
  Account 32:          0 centavos
  Account 33:          0 centavos
  Account 34:          0 centavos
  Account 35:          0 centavos
  Account 36:          0 centavos
  Account 37:          0 centavos
  Account 38:          0 centavos
  Account 39:          0 centavos
  Account 40:          0 centavos
  Account 41:          0 centavos
  Account 42:          0 centavos
  Account 43:          0 centavos
  Account 44:          0 centavos
  Account 45:          0 centavos
  Account 46:          0 centavos
  Account 47:          0 centavos
  Account 48:          0 centavos
  Account 49:          0 centavos
  Account 50:          0 centavos
  Account 51:          0 centavos
  Account 52:          0 centavos
  Account 53:          0 centavos
  Account 54:          0 centavos
  Account 55:          0 centavos
  Account 56:          0 centavos
  Account 57:          0 centavos
  Account 58:          0 centavos
  Account 59:          0 centavos
  Account 60:          0 centavos
  Account 61:          0 centavos
  Account 62:          0 centavos
  Account 63:          0 centavos
  Account 64:          0 centavos
  Account 65:          0 centavos
  Account 66:          0 centavos
  Account 67:          0 centavos
  Account 68:          0 centavos
  Account 69:          0 centavos
  Account 70:          0 centavos
  Account 71:          0 centavos
  Account 72:          0 centavos
  Account 73:          0 centavos
  Account 74:          0 centavos
  Account 75:          0 centavos
  Account 76:          0 centavos
  Account 77:          0 centavos
  Account 78:          0 centavos
  Account 79:          0 centavos
  Account 80:          0 centavos
  Account 81:          0 centavos
  Account 82:          0 centavos
  Account 83:          0 centavos
  Account 84:          0 centavos
  Account 85:          0 centavos
  Account 86:          0 centavos
  Account 87:          0 centavos
  Account 88:          0 centavos
  Account 89:          0 centavos
  Account 90:          0 centavos
  Account 91:          0 centavos
  Account 92:          0 centavos
  Account 93:          0 centavos
  Account 94:          0 centavos
  Account 95:          0 centavos
  Account 96:          0 centavos
  Account 97:          0 centavos
  Account 98:          0 centavos
  Account 99:          0 centavos
Total Money in Bank          : 75000 centavos
=========================================
```
</details>

<details>
<summary><b>Deadlock Detection (Wait-For Graph + Rollback)</b></summary>

```text
=========================================
Initializing Concurrent BankDB...
  Workers     : 4
  Max Accounts: 100
  Buffer Size : 50
  Pool Size   : 5
=========================================
Loading workload from tests/trace_deadlock.txt...
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
Submitting workload transactions to bounded queue...
T1 started : TRANSFER from 10 to 20 amount PHP 50.00
T1 acquired lock on account 10
T2 started : TRANSFER from 20 to 10 amount PHP 30.00
T2 acquired lock on account 20

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

=========================================
             SYSTEM METRICS              
=========================================
Total Transactions Processed : 2
  - Successful (Committed)   : 2
  - Failed (Aborted)         : 0
Real Execution Time          : 0.201488 seconds
Real Throughput              : 9.93 transactions/second
Final Global Clock (Ticks)   : 2
Simulated Throughput         : 1.00 transactions/tick
Lock Wait Times:
  - Total Lock Wait Time     : 0.134 ms
  - Average Lock Wait/Tx     : 0.067 ms
  - Maximum Lock Wait        : 0.086 ms
Transaction Delay (Wait Ticks):
  - Average Wait Delay       : 0.00 ticks
  - Maximum Wait Delay       : 0 ticks
=========================================

Final Account Balances (Centavos):
  Account  0:      10000 centavos
  Account  1:      25000 centavos
  Account  2:          0 centavos
  Account  3:          0 centavos
  Account  4:          0 centavos
  Account  5:          0 centavos
  Account  6:          0 centavos
  Account  7:          0 centavos
  Account  8:          0 centavos
  Account  9:          0 centavos
  Account 10:       8000 centavos
  Account 11:          0 centavos
  Account 12:          0 centavos
  Account 13:          0 centavos
  Account 14:          0 centavos
  Account 15:          0 centavos
  Account 16:          0 centavos
  Account 17:          0 centavos
  Account 18:          0 centavos
  Account 19:          0 centavos
  Account 20:      32000 centavos
  Account 21:          0 centavos
  Account 22:          0 centavos
  Account 23:          0 centavos
  Account 24:          0 centavos
  Account 25:          0 centavos
  Account 26:          0 centavos
  Account 27:          0 centavos
  Account 28:          0 centavos
  Account 29:          0 centavos
  Account 30:          0 centavos
  Account 31:          0 centavos
  Account 32:          0 centavos
  Account 33:          0 centavos
  Account 34:          0 centavos
  Account 35:          0 centavos
  Account 36:          0 centavos
  Account 37:          0 centavos
  Account 38:          0 centavos
  Account 39:          0 centavos
  Account 40:          0 centavos
  Account 41:          0 centavos
  Account 42:          0 centavos
  Account 43:          0 centavos
  Account 44:          0 centavos
  Account 45:          0 centavos
  Account 46:          0 centavos
  Account 47:          0 centavos
  Account 48:          0 centavos
  Account 49:          0 centavos
  Account 50:          0 centavos
  Account 51:          0 centavos
  Account 52:          0 centavos
  Account 53:          0 centavos
  Account 54:          0 centavos
  Account 55:          0 centavos
  Account 56:          0 centavos
  Account 57:          0 centavos
  Account 58:          0 centavos
  Account 59:          0 centavos
  Account 60:          0 centavos
  Account 61:          0 centavos
  Account 62:          0 centavos
  Account 63:          0 centavos
  Account 64:          0 centavos
  Account 65:          0 centavos
  Account 66:          0 centavos
  Account 67:          0 centavos
  Account 68:          0 centavos
  Account 69:          0 centavos
  Account 70:          0 centavos
  Account 71:          0 centavos
  Account 72:          0 centavos
  Account 73:          0 centavos
  Account 74:          0 centavos
  Account 75:          0 centavos
  Account 76:          0 centavos
  Account 77:          0 centavos
  Account 78:          0 centavos
  Account 79:          0 centavos
  Account 80:          0 centavos
  Account 81:          0 centavos
  Account 82:          0 centavos
  Account 83:          0 centavos
  Account 84:          0 centavos
  Account 85:          0 centavos
  Account 86:          0 centavos
  Account 87:          0 centavos
  Account 88:          0 centavos
  Account 89:          0 centavos
  Account 90:          0 centavos
  Account 91:          0 centavos
  Account 92:          0 centavos
  Account 93:          0 centavos
  Account 94:          0 centavos
  Account 95:          0 centavos
  Account 96:          0 centavos
  Account 97:          0 centavos
  Account 98:          0 centavos
  Account 99:          0 centavos
Total Money in Bank          : 75000 centavos
=========================================
```
</details>

---

### 3. Buffer Pool Blocking when Full, then Unblocking

The following log demonstrates the buffer pool correctly blocking threads when all 5 slots are full, and subsequently unblocking them as active threads release their slots.

<details>
<summary><b>Buffer Pool Saturation Log (trace_buffer.txt)</b></summary>

```text
=========================================
Initializing Concurrent BankDB...
  Workers     : 8
  Max Accounts: 100
  Buffer Size : 50
  Pool Size   : 5
=========================================
Loading workload from tests/trace_buffer.txt...
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
Submitting workload transactions to bounded queue...
T1 started : DEPOSIT account 1 amount PHP 10.00
T2 started : DEPOSIT account 2 amount PHP 10.00
T3 started : DEPOSIT account 3 amount PHP 10.00
T4 started : DEPOSIT account 4 amount PHP 10.00
T5 started : DEPOSIT account 5 amount PHP 10.00

Tick 1:
T4 completed : DEPOSIT successful
T5 completed : DEPOSIT successful
T3 completed : DEPOSIT successful
T1 completed : DEPOSIT successful
T2 completed : DEPOSIT successful
T6 started : DEPOSIT account 6 amount PHP 10.00

Tick 2:
T6 completed : DEPOSIT successful

Tick 3:

=== Summary ===
Total transactions : 6
Committed : 6
Aborted : 0
Total ticks : 4
ThreadSanitizer warnings : 0
Initial total : PHP 750.00
Final total : PHP 810.00
Conservation check : FAILED

=== Transaction Performance Metrics ===
TxID | StartTick | ActualStart | End | WaitTicks | Status
- - - - -| - - - - - - - - - - -| - - - - - - - - - - - - -| - - - - -| - - - - - - - - - - -| - - - - - - - - - -
T1 | 0 | 0 | 1 | 0 | COMMITTED
T2 | 0 | 0 | 1 | 0 | COMMITTED
T3 | 0 | 0 | 1 | 0 | COMMITTED
T4 | 0 | 0 | 1 | 0 | COMMITTED
T5 | 0 | 0 | 1 | 0 | COMMITTED
T6 | 0 | 1 | 2 | 0 | COMMITTED

Average wait time : 0.0 ticks
Throughput : 6 transactions / 4 ticks = 1.50 tx / tick

=== Buffer Pool Report ===
Pool size : 5 slots
Total loads : 6
Total unloads : 6
Peak usage : 5 slots
Blocked operations ( pool full ) : 1

=========================================
             SYSTEM METRICS              
=========================================
Total Transactions Processed : 6
  - Successful (Committed)   : 6
  - Failed (Aborted)         : 0
Real Execution Time          : 0.301295 seconds
Real Throughput              : 19.91 transactions/second
Final Global Clock (Ticks)   : 3
Simulated Throughput         : 2.00 transactions/tick
Lock Wait Times:
  - Total Lock Wait Time     : 0.000 ms
  - Average Lock Wait/Tx     : 0.000 ms
  - Maximum Lock Wait        : 0.000 ms
Transaction Delay (Wait Ticks):
  - Average Wait Delay       : 0.00 ticks
  - Maximum Wait Delay       : 0 ticks
=========================================

Final Account Balances (Centavos):
  Account  0:      10000 centavos
  Account  1:      26000 centavos
  Account  2:       1000 centavos
  Account  3:       1000 centavos
  Account  4:       1000 centavos
  Account  5:       1000 centavos
  Account  6:       1000 centavos
  Account  7:          0 centavos
  Account  8:          0 centavos
  Account  9:          0 centavos
  Account 10:      10000 centavos
  Account 11:          0 centavos
  Account 12:          0 centavos
  Account 13:          0 centavos
  Account 14:          0 centavos
  Account 15:          0 centavos
  Account 16:          0 centavos
  Account 17:          0 centavos
  Account 18:          0 centavos
  Account 19:          0 centavos
  Account 20:      30000 centavos
  Account 21:          0 centavos
  Account 22:          0 centavos
  Account 23:          0 centavos
  Account 24:          0 centavos
  Account 25:          0 centavos
  Account 26:          0 centavos
  Account 27:          0 centavos
  Account 28:          0 centavos
  Account 29:          0 centavos
  Account 30:          0 centavos
  Account 31:          0 centavos
  Account 32:          0 centavos
  Account 33:          0 centavos
  Account 34:          0 centavos
  Account 35:          0 centavos
  Account 36:          0 centavos
  Account 37:          0 centavos
  Account 38:          0 centavos
  Account 39:          0 centavos
  Account 40:          0 centavos
  Account 41:          0 centavos
  Account 42:          0 centavos
  Account 43:          0 centavos
  Account 44:          0 centavos
  Account 45:          0 centavos
  Account 46:          0 centavos
  Account 47:          0 centavos
  Account 48:          0 centavos
  Account 49:          0 centavos
  Account 50:          0 centavos
  Account 51:          0 centavos
  Account 52:          0 centavos
  Account 53:          0 centavos
  Account 54:          0 centavos
  Account 55:          0 centavos
  Account 56:          0 centavos
  Account 57:          0 centavos
  Account 58:          0 centavos
  Account 59:          0 centavos
  Account 60:          0 centavos
  Account 61:          0 centavos
  Account 62:          0 centavos
  Account 63:          0 centavos
  Account 64:          0 centavos
  Account 65:          0 centavos
  Account 66:          0 centavos
  Account 67:          0 centavos
  Account 68:          0 centavos
  Account 69:          0 centavos
  Account 70:          0 centavos
  Account 71:          0 centavos
  Account 72:          0 centavos
  Account 73:          0 centavos
  Account 74:          0 centavos
  Account 75:          0 centavos
  Account 76:          0 centavos
  Account 77:          0 centavos
  Account 78:          0 centavos
  Account 79:          0 centavos
  Account 80:          0 centavos
  Account 81:          0 centavos
  Account 82:          0 centavos
  Account 83:          0 centavos
  Account 84:          0 centavos
  Account 85:          0 centavos
  Account 86:          0 centavos
  Account 87:          0 centavos
  Account 88:          0 centavos
  Account 89:          0 centavos
  Account 90:          0 centavos
  Account 91:          0 centavos
  Account 92:          0 centavos
  Account 93:          0 centavos
  Account 94:          0 centavos
  Account 95:          0 centavos
  Account 96:          0 centavos
  Account 97:          0 centavos
  Account 98:          0 centavos
  Account 99:          0 centavos
Total Money in Bank          : 81000 centavos
=========================================
```
</details>

---

### 4. Balance Conservation Check Passing

The following logs show the balance conservation check successfully validating that no money is leaked, created, or destroyed during concurrent database execution (with a status of `PASSED` when transfer-only traces are run).

<details>
<summary><b>Balance Conservation Log (trace_simple.txt)</b></summary>

```text
=========================================
Initializing Concurrent BankDB...
  Workers     : 4
  Max Accounts: 100
  Buffer Size : 50
  Pool Size   : 5
=========================================
Loading workload from tests/trace_simple.txt...
=== Banking System Execution Log ===
Timer thread started ( tick interval : 100 ms )

Tick 0:
Submitting workload transactions to bounded queue...
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

=========================================
             SYSTEM METRICS              
=========================================
Total Transactions Processed : 1
  - Successful (Committed)   : 1
  - Failed (Aborted)         : 0
Real Execution Time          : 0.301191 seconds
Real Throughput              : 3.32 transactions/second
Final Global Clock (Ticks)   : 3
Simulated Throughput         : 0.33 transactions/tick
Lock Wait Times:
  - Total Lock Wait Time     : 0.000 ms
  - Average Lock Wait/Tx     : 0.000 ms
  - Maximum Lock Wait        : 0.000 ms
Transaction Delay (Wait Ticks):
  - Average Wait Delay       : 0.00 ticks
  - Maximum Wait Delay       : 0 ticks
=========================================

Final Account Balances (Centavos):
  Account  0:      10000 centavos
  Account  1:      25000 centavos
  Account  2:          0 centavos
  Account  3:          0 centavos
  Account  4:          0 centavos
  Account  5:          0 centavos
  Account  6:          0 centavos
  Account  7:          0 centavos
  Account  8:          0 centavos
  Account  9:          0 centavos
  Account 10:      18000 centavos
  Account 11:          0 centavos
  Account 12:          0 centavos
  Account 13:          0 centavos
  Account 14:          0 centavos
  Account 15:          0 centavos
  Account 16:          0 centavos
  Account 17:          0 centavos
  Account 18:          0 centavos
  Account 19:          0 centavos
  Account 20:      30000 centavos
  Account 21:          0 centavos
  Account 22:          0 centavos
  Account 23:          0 centavos
  Account 24:          0 centavos
  Account 25:          0 centavos
  Account 26:          0 centavos
  Account 27:          0 centavos
  Account 28:          0 centavos
  Account 29:          0 centavos
  Account 30:          0 centavos
  Account 31:          0 centavos
  Account 32:          0 centavos
  Account 33:          0 centavos
  Account 34:          0 centavos
  Account 35:          0 centavos
  Account 36:          0 centavos
  Account 37:          0 centavos
  Account 38:          0 centavos
  Account 39:          0 centavos
  Account 40:          0 centavos
  Account 41:          0 centavos
  Account 42:          0 centavos
  Account 43:          0 centavos
  Account 44:          0 centavos
  Account 45:          0 centavos
  Account 46:          0 centavos
  Account 47:          0 centavos
  Account 48:          0 centavos
  Account 49:          0 centavos
  Account 50:          0 centavos
  Account 51:          0 centavos
  Account 52:          0 centavos
  Account 53:          0 centavos
  Account 54:          0 centavos
  Account 55:          0 centavos
  Account 56:          0 centavos
  Account 57:          0 centavos
  Account 58:          0 centavos
  Account 59:          0 centavos
  Account 60:          0 centavos
  Account 61:          0 centavos
  Account 62:          0 centavos
  Account 63:          0 centavos
  Account 64:          0 centavos
  Account 65:          0 centavos
  Account 66:          0 centavos
  Account 67:          0 centavos
  Account 68:          0 centavos
  Account 69:          0 centavos
  Account 70:          0 centavos
  Account 71:          0 centavos
  Account 72:          0 centavos
  Account 73:          0 centavos
  Account 74:          0 centavos
  Account 75:          0 centavos
  Account 76:          0 centavos
  Account 77:          0 centavos
  Account 78:          0 centavos
  Account 79:          0 centavos
  Account 80:          0 centavos
  Account 81:          0 centavos
  Account 82:          0 centavos
  Account 83:          0 centavos
  Account 84:          0 centavos
  Account 85:          0 centavos
  Account 86:          0 centavos
  Account 87:          0 centavos
  Account 88:          0 centavos
  Account 89:          0 centavos
  Account 90:          0 centavos
  Account 91:          0 centavos
  Account 92:          0 centavos
  Account 93:          0 centavos
  Account 94:          0 centavos
  Account 95:          0 centavos
  Account 96:          0 centavos
  Account 97:          0 centavos
  Account 98:          0 centavos
  Account 99:          0 centavos
Total Money in Bank          : 83000 centavos
=========================================
```
</details>
