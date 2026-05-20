CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -O2
TSAN_FLAGS = -Wall -Wextra -pthread -std=c11 -D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -fsanitize=thread -g -O1

TARGET = bankdb

all: $(TARGET)

$(TARGET): bankdb.c
	$(CC) $(CFLAGS) bankdb.c -o $(TARGET)

tsan: bankdb.c
	$(CC) $(TSAN_FLAGS) bankdb.c -o $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all tsan clean
