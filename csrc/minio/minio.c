/* MIT License

   Copyright (c) 2023 Gus Waldspurger

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
   */

#define _GNU_SOURCE

#include "minio.h"

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "../include/uthash.h"


/* ------------------------ */
/*   REPLACEMENT POLICIES   */
/* ------------------------ */

typedef uint64_t policy_func_t(cache_t *, void *, size_t);

/* FIFO cache replacement policy. */
static uint64_t
policy_FIFO(cache_t *cache, void *item, size_t size)
{
   return -1;
}

/* MinIO cache replacement policy. */
static uint64_t
policy_MINIO(cache_t *cache, void *item, size_t size)
{
   return -1;
}

/* Policy table converts from policy_t enum to policy function. */
__attribute__ ((unused))
static policy_func_t *policy_table[N_POLICIES] = {
   policy_FIFO,
   policy_MINIO
};


/* ------------- */
/*   INTERFACE   */
/* ------------- */

/* Read an item from CACHE into DATA, indexed by FILEPATH, and located on the
   filesystem at FILEPATH. On failure returns ERRNO code with negative value,
   otherwise returns bytes read on success.
   
   DATA must be block-aligned, in order for direct IO to work properly. */
ssize_t
cache_read(cache_t *cache, char *filepath, void *data, uint64_t max_size)
{
   static unsigned long n_accs = 0;
   static unsigned long n_hits = 0;
   static unsigned long n_miss = 0;
   static unsigned long n_fail = 0;

   if (n_accs % 1000 == 0) {
      printf("[MinIO debug] accesses = %lu, hits = %lu, misses = %lu, fails = %lu (usage = %lu/%lu MB\n", n_accs, n_hits, n_miss, n_fail, cache->used, cache->size);
   }

   n_accs++;

   /* Check if the file is cached. */
   hash_entry_t *entry = NULL;
   HASH_FIND_STR(cache->ht, filepath, entry);
   if (entry != NULL) {
      /* Don't overflow the buffer. */
      if (entry->size > max_size) {
         return -EINVAL;
      }
      memcpy(data, entry->ptr, entry->size);

      n_hits++;
      return entry->size;
   }

   /* Open the file in DIRECT mode. */
   int fd = open(filepath, O_RDONLY | __O_DIRECT);
   if (fd < 0) {
      n_fail++;
      return -ENOENT;
   }

   /* Ensure the size of the file is OK. */
   size_t size = lseek(fd, 0L, SEEK_END);
   if (size > max_size || size == 0) {
      close(fd);
      n_fail++;
      return -EINVAL;
   }
   lseek(fd, 0L, SEEK_SET);

   /* Read into data and cache the data if it'll fit. */
   read(fd, data, (size | 0xFFF) + 1);
   close(fd);
   if (size <= cache->size - cache->used) {
      /* Make an entry. */
      entry = malloc(sizeof(hash_entry_t));
      if (entry == NULL) {
         n_fail++;
         return -ENOMEM;
      }
      strncpy(entry->filepath, filepath, MAX_PATH_LENGTH);
      entry->size = size;

      /* Copy data to the cache. */
      entry->ptr = cache->data + cache->used;
      memcpy(entry->ptr, data, size);
      cache->used += size;

      /* Place the entry into the hash table. */
      HASH_ADD_STR(cache->ht, filepath, entry);
   }

   n_miss++;
   return size;
}

/* Clear the cache's hash table and reset used bytes to zero. */
void
cache_flush(cache_t *cache)
{
   HASH_CLEAR(hh, cache->ht);
   cache->used = 0;
}

/* Initialize a cache CACHE with SIZE bytes and POLICY replacement policy. On
   success, 0 is returned. On failure, negative ERRNO value is returned. */
int
cache_init(cache_t *cache, size_t size, policy_t policy)
{
   /* Cache configuration. */
   cache->size = size;
   cache->used = 0;
   cache->policy = policy;

   /* Initialize the hash table. */
   cache->ht = NULL;

   /* Allocate the cache's memory, and ensure it's 8-byte aligned so that direct
      IO will work properly. */
   if ((cache->data = malloc(size)) == NULL) {
      return -ENOMEM;
   }

   /* Pin the cache's memory. */
   if (mlock(cache->data, size) != 0) {
      return -EPERM;
   }

   return 0;
}