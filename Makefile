CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -Iinclude -O2
TSAN_FLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -Iinclude -fsanitize=thread -g -O1

TARGET = bankdb
SRCS = src/main.c src/bank.c src/transaction.c src/timer.c src/lock_mgr.c src/buffer_pool.c src/metrics.c src/utils.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET)

tsan: $(SRCS)
	$(CC) $(TSAN_FLAGS) $(SRCS) -o $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all tsan clean
