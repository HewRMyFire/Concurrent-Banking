CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -Iinclude -O2
DEBUG_FLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -Iinclude -fsanitize=thread -g -O1

TARGET = bankdb
SRCS = src/main.c src/bank.c src/transaction.c src/timer.c src/lock_mgr.c src/buffer_pool.c src/metrics.c src/utils.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET)

debug: $(SRCS)
	$(CC) $(DEBUG_FLAGS) $(SRCS) -o $(TARGET)

clean:
	rm -f $(TARGET) *.o src/*.o

test: all
	@echo "=========================================================="
	@echo "Test 1: Simple Operations"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_simple.txt --deadlock=prevention
	@echo ""
	@echo "=========================================================="
	@echo "Test 2: Concurrent Readers"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_readers.txt --deadlock=prevention
	@echo ""
	@echo "=========================================================="
	@echo "Test 3a: Deadlock Prevention"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_deadlock.txt --deadlock=prevention
	@echo ""
	@echo "=========================================================="
	@echo "Test 3b: Deadlock Detection"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_deadlock.txt --deadlock=detection
	@echo ""
	@echo "=========================================================="
	@echo "Test 4: Abort on Insufficient Funds"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_abort.txt --deadlock=prevention
	@echo ""
	@echo "=========================================================="
	@echo "Test 5: Bounded Buffer Pool Saturation"
	@echo "=========================================================="
	./bankdb --accounts=tests/accounts.txt --trace=tests/trace_buffer.txt --deadlock=prevention --workers=8

.PHONY: all debug clean test
