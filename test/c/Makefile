# https://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/

CC     = gcc
CFLAGS = -Wall
DEPS   = ../../csrc/minio/minio.h
OBJ    = test_minio.o ../../csrc/minio/minio.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	rm -f test test-minio.o ../../csrc/minio/minio.o