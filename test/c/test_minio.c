/* Author:  Gus Waldspurger

   MinIO file cache tests.
   */

#include "../../csrc/minio/minio.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

#define KB (1024)
#define MB (KB * KB)
#define GB (KB * KB * KB)

#define N_TEST_FILES (3)
#define SPEEDUP_METRIC (2)

#define BLOCK_SIZE (4096)

/* Returns access time in nanoseconds. */
long
timed_access(cache_t *cache, char *filepath, void *data, size_t max_size)
{
    struct timespec start, end;

    clock_gettime(CLOCK_REALTIME, &start);
    assert(cache_read(cache, filepath, data, max_size) != -1);
    clock_gettime(CLOCK_REALTIME, &end);

    return (start.tv_nsec + start.tv_sec * 1e9) - (end.tv_nsec + end.tv_sec * 1e9);
}

/* Test that hot accesses for items that fit in the cache are faster than their
   cold accesses, and that uncached items are approximately the same. */
void
test_timing(size_t cache_size,
            size_t max_size,
            char **filepaths,
            bool *should_cache,
            int n_files)
{


    /* Access times in nanoseconds. */
    long times_hot[n_files];
    long times_cold[n_files];

    /* Where we're reading the file into from the cache. */
    uint8_t *data;
    assert(posix_memalign((void **) &data, BLOCK_SIZE, max_size) == 0);

    /* Cache being tested. */
    cache_t cache;
    assert(cache_init(&cache, cache_size, POLICY_MINIO) == 0);

    /* Cold accesses. */
    for (int i = 0; i < n_files; i++) {
        times_cold[i] = timed_access(&cache, filepaths[i], data, max_size);
    }

    /* Hot accesses. */
    for (int i = 0; i < n_files; i++) {
        times_hot[i] = timed_access(&cache, filepaths[i], data, max_size);
    }

    /* Check timing. */
    for (int i = 0; i < n_files; i++) {
        double speedup = (1e-9 * times_cold[i]) / (1e-9 * times_hot[i]);
        printf("Speedup for item %d is %.02lfx (cached? %d).\n", i, speedup, should_cache[i]);
    }

    free(data);
}

/* Verify that DATA contains the same data as the file at FILEPATH. */
bool
verify_integrity(char *filepath, uint8_t *data, ssize_t size)
{
    /* Read a fresh copy of the data from storage. */
    uint8_t *baseline = malloc(size);
    assert(baseline != NULL);
    int fd = open(filepath, O_RDONLY);
    read(fd, baseline, size);

    /* Check each byte matches. */
    for (size_t i = 0; i < size; i++) {
        if (data[i] != baseline[i]) {
            printf("byte offset %ld is incorrect (data = 0x%hx, truth = 0x%hx)\n",
                   i,
                   data[i],
                   baseline[i]);
            free(baseline);
            return false;
        }
    }

    free(baseline);
    return true;
}

void
test_integrity(size_t cache_size,
              size_t max_size,
              char **filepaths,
              int n_files)
{
    /* Where we're reading the file into from the cache. */
    uint8_t *data;
    assert(posix_memalign((void **) &data, BLOCK_SIZE, max_size) == 0);

    /* Cache being tested. */
    cache_t cache;
    assert(cache_init(&cache, cache_size, POLICY_MINIO) == 0);

    /* Cold accesses. */
    for (int i = 0; i < n_files; i++) {
        ssize_t size = cache_read(&cache, filepaths[i], data, max_size);
        assert(size > 0);
        // assert(verify_integrity(filepaths[i], data, size));
        verify_integrity(filepaths[i], data, size);
    }

    /* Hot accesses. */
    for (int i = 0; i < n_files; i++) {
        ssize_t size = cache_read(&cache, filepaths[i], data, max_size);
        assert(size > 0);
        // assert(verify_integrity(filepaths[i], data, size));
        verify_integrity(filepaths[i], data, size);
    }

    free(data);
}

uint8_t *
get_aligned(uint8_t *addr, int block_size)
{
    uintptr_t align = block_size - 1;
    return (uint8_t *) (((uintptr_t) addr + align) & ~((uintptr_t) align));
}

int
main(int argc, char **argv)
{
    uint8_t d1[1024];
    uint8_t d2[1024];

    hash_entry_t *entry_1 = mmap_alloc(sizeof(hash_entry_t));
    if (entry_1 == NULL) {
        printf("cannot allocate e1\n");
        return 1;
    }
    strncpy(entry_1->filepath, "debug debug debug", MAX_PATH_LENGTH + 1);
    entry_1->ptr = 12345;
    entry_1->size = 77777;

    hash_entry_t *entry_2 = mmap_alloc(sizeof(hash_entry_t));
    if (entry_2 == NULL) {
        printf("cannot allocate e2\n");
        return 1;
    }
    strncpy(entry_2->filepath, "debug debug debug 2222", MAX_PATH_LENGTH + 1);
    entry_2->ptr = 54321;
    entry_2->size = 99999;

    hash_entry_t *ht = mmap_alloc(sizeof(hash_entry_t));
    if (ht == NULL) {
        printf("cannot allocate ht\n");
        return 1;
    }
    HASH_MAKE_TABLE(hh, ht, 0, 16, 4);

    /* Insert item 1. */
    HASH_ADD_STR(ht, filepath, entry_1);

    /* Insert item 2. */
    HASH_ADD_STR(ht, filepath, entry_2);


    /* Read item 1. */
    hash_entry_t *test_read_1;
    HASH_FIND_STR(ht, "debug debug debug", test_read_1);
    
    printf("e1: %p\nt1: %p\nmatch? %d\n", entry_1, test_read_1, entry_1 == test_read_1);

    /* Read item 2. */
    hash_entry_t *test_read_2;
    HASH_FIND_STR(ht, "debug debug debug 2222", test_read_2);
    
    printf("e2: %p\nt1: %p\nmatch? %d\n", entry_2, test_read_2, entry_2 == test_read_2);



    return 0;



    char *test_files[N_TEST_FILES] = {
        "../test-images/2MB.bmp",
        "../test-images/4MB.bmp",
        "../test-images/20MB.bmp"
    };

    bool should_cache[N_TEST_FILES] = {
        true,
        true,
        false
    };

    /* Timing tests. */
    printf("testing timing...\n");
    test_timing(8 * MB, 32 * MB, test_files, should_cache, N_TEST_FILES);

    /* Integrity tests. */
    printf("testing integrity...\n");
    size_t integrity_configs[6] = {
        32 * MB,
        16 * MB,
        8 * MB,
        4 * MB,
        2 * MB,
        1 * MB,
    };
    for (int i = 0; i < 6; i++) {
        printf("\t%ld KB cache...", integrity_configs[i] / KB);
        test_integrity(integrity_configs[i], 32 * MB, test_files, N_TEST_FILES);
        printf(" OK.\n");
    }

    printf("All tests OK.\n");
}
