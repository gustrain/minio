# https://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/

CC     = gcc
CFLAGS = -I -Wall
DEPS   = ../minio/minio.h
OBJ    = test-minio.o ../minio/minio.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	rm -f test test-minio.o ../minio/minio.o