/* Author:  Gus Waldspurger

   MinIO file cache tests.
   */

#include "../../csrc/minio/minio.h"

#include <stdio.h>
#include <time.h>
#include <stdbool.h>
#include <assert.h>

#define KB (1024)
#define MB (KB * KB)
#define GB (KB * KB * KB)

#define N_TEST_FILES (3)
#define SPEEDUP_METRIC (2)

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
    uint8_t *data = malloc(max_size);
    assert(data != NULL);

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
        printf("Speedup for item %d is %.02lfx.\n", i, speedup);
        if (should_cache[i]) {
            assert(speedup >= SPEEDUP_METRIC);
        } else {
            assert(speedup < SPEEDUP_METRIC);
        }
    }

    free(data);
}

/* Verify that DATA contains the same data as the file at FILEPATH. */
bool
verify_integrity(char *filepath, uint8_t *data, size_t size)
{
    /* Read a fresh copy of the data from storage. */
    uint8_t *baseline = malloc(size);
    assert(baseline != NULL);
    FILE *file = fopen(filepath, "r");
    fread(baseline, size, 1, file);

    /* Check each byte matches. */
    for (size_t i = 0; i < size; i++) {
        if (data[i] != baseline[i]) {
            printf("byte offset %ld is incorrect (data = %hu, truth = %hu)\n",
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
    uint8_t *data = malloc(max_size);
    assert(data != NULL);

    /* Cache being tested. */
    cache_t cache;
    assert(cache_init(&cache, cache_size, POLICY_MINIO) == 0);

    /* Cold accesses. */
    for (int i = 0; i < n_files; i++) {
        long size = cache_read(&cache, filepaths[i], data, max_size);
        assert(verify_integrity(filepaths[i], data, size));
    }

    /* Hot accesses. */
    for (int i = 0; i < n_files; i++) {
        long size = cache_read(&cache, filepaths[i], data, max_size);
        assert(verify_integrity(filepaths[i], data, size));
    }

    free(data);
}

int
main(int argc, char **argv)
{
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
    size_t integrity_configs[7] = {
        32 * MB,
        16 * MB,
        8 * MB,
        4 * MB,
        2 * MB,
        1 * MB,
        0
    };
    for (int i = 0; i < 7; i++) {
        printf("\t%ld KB cache...", integrity_configs[i] / KB);
        test_integrity(integrity_configs[i], 32 * MB, test_files, N_TEST_FILES);
        printf(" OK.\n");
    }

    printf("All tests OK.\n");
}
