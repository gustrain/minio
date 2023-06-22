/* Author:  Gus Waldspurger

   Implementation of a MinIO file cache.
   */

#ifndef __MINIO_MODULE_H_
#define __MINIO_MODULE_H_

#include <stdlib.h>

#include "../include/uthash.h"

#define MAX_PATH_LENGTH 128

/* Cache replacement policy. */
typedef enum {
    POLICY_FIFO,
    POLICY_MINIO,
    N_POLICIES
} policy_t;

/* Hash table entry. Maps filepath to cached data. An entry must be in the hash
   table IFF the corresponding file is cached. */
typedef struct {
    char    filepath[MAX_PATH_LENGTH + 1];  /* Key. Filepath of file. */
    void   *ptr;                            /* Pointer to this file's data. */
    size_t  size;                           /* Size of file data in bytes. */

    UT_hash_handle hh;
} hash_entry_t;

/* Cache. */
typedef struct {
    /* Configuration. */
    policy_t policy;        /* Replacement policy. Only MinIO supported. */
    size_t   size;          /* Size of cache in bytes. */
    size_t   used;          /* Number of bytes cached. */

    /* State. */
    uint8_t      *data;     /* First byte of SIZE bytes of memory. */
    hash_entry_t *ht;       /* Hash table, maps filename to data. */
} cache_t;


ssize_t cache_read(cache_t *cache, char *filepath, void *data, uint64_t max_size);
void cache_flush(cache_t *cache);
int cache_init(cache_t *cache, size_t size, policy_t policy);

#endif