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
#include "../utils/utils.h"

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdatomic.h>
#include <pthread.h>

#include "../include/uthash.h"

#define AVERAGE_FILE_SIZE (100 * 1024)

#define STAT_INC(cache, field) atomic_fetch_add(&cache->field, 1)


/* Read an item from CACHE into DATA, indexed by FILEPATH, and located on the
   filesystem at FILEPATH. On failure returns ERRNO code with negative value,
   otherwise returns bytes read on success.
   
   DATA must be block-aligned, in order for direct IO to work properly.
   
   Note we use atomics to implement thread safe options because pthreads and
   traditional synchronization primitives are not safe to use with the Python
   interpreter, and may cause deadlock to occur, regardless of the correctness
   of primitives' usage.
   
   It should be noted that the cache is only thread/process safe so long as the
   cache entries are only written once (as is the case with MinIO). */
ssize_t
cache_read(cache_t *cache, char *filepath, void *data, uint64_t max_size)
{
   size_t n_accs = STAT_INC(cache, n_accs);
   if (n_accs % 2500 == 0) {
      DEBUG_LOG("[MinIO debug] accesses = %lu, hits = %lu, cold misses = %lu, capacity misses = %lu, fails = %lu (usage = %lu/%lu MB) (cache->data = %p) (&cache->used = %p) (pid = %d, ppid = %d)\n", cache->n_accs, cache->n_hits, cache->n_miss_cold, cache->n_miss_capacity, cache->n_fail, cache->used / (1024 * 1024), cache->size / (1024 * 1024), cache->data, &cache->used, getpid(), getppid());
   }

   /* Check if the file is cached. */
   hash_entry_t *entry = NULL;
   HASH_FIND_STR(cache->ht, filepath, entry);
   if (entry != NULL) {
      /* Don't overflow the buffer. */
      if (entry->size > max_size) {
         return -EINVAL;
      }
      memcpy(data, entry->ptr, entry->size);

      STAT_INC(cache, n_hits);
      return entry->size;
   }

   /* Open the file in DIRECT mode. */
   int fd = open(filepath, O_RDONLY | __O_DIRECT);
   if (fd < 0) {
      STAT_INC(cache, n_fail);
      return -ENOENT;
   }

   /* Ensure the size of the file is OK. */
   size_t size = lseek(fd, 0L, SEEK_END);
   if (size > max_size || size == 0) {
      close(fd);
      STAT_INC(cache, n_fail);
      return -EINVAL;
   }
   lseek(fd, 0L, SEEK_SET);

   /* Read into data and cache the data if it'll fit. */
   read(fd, data, (size | 0xFFF) + 1);
   close(fd);
   if (size <= cache->size - cache->used) {
      /* Acquire an entry. */
      size_t n = atomic_fetch_add(&cache->n_ht_entries, 1);
      if (n >= cache->max_ht_entries) {
         STAT_INC(cache, n_fail);
         return -ENOMEM;
      }
      hash_entry_t *entry = &cache->ht_entries[n];

      /* Copy the filepath into the entry. */
      strncpy(entry->filepath, filepath, MAX_PATH_LENGTH);

      /* Figure out where the data goes. Note that the atomic_X functions return
         the pre-operation value. */
      entry->size = size;
      entry->ptr = cache->data + atomic_fetch_add(&cache->used, size);

      /* Copy data to the cache. */
      memcpy(entry->ptr, data, size);

      /* Note there is an implicit assumption that two threads/processes will
         not simultaneously attempt to access the same filepath for the first
         time. For ML data-loader applications this is satisfied, since each
         element is accessed only once per epoch, however this will present a
         race condition in applications where this scenario can occur. */

      /* Insert into hash table. */
      pthread_spin_lock(&cache->ht_lock);
      HASH_ADD_STR(cache->ht, filepath, entry);
      pthread_spin_unlock(&cache->ht_lock);

      STAT_INC(cache, n_miss_cold);
   } else {
      STAT_INC(cache, n_miss_capacity);
   }

   return size;
}

/* Clear the cache's hash table and reset used bytes to zero.

   Note this function is NOT thread safe, as safety cannot be ensured by the
   use of atomics alone, and enforcing safety over the update of all touched
   fields in read would significantly increase the time the spinlock is held for
   and thus potentially significantly decrease parallel read performance. */
void
cache_flush(cache_t *cache)
{
   /* Clear the HT and the cache metadata. */
   HASH_CLEAR(hh, cache->ht);
   cache->n_ht_entries = 0UL;
   cache->used = 0UL;
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

   /* Zero initial stats. */
   cache->n_accs = 0;
   cache->n_fail = 0;
   cache->n_hits = 0;
   cache->n_miss_capacity = 0;
   cache->n_miss_cold = 0;

   /* Initialize the hash table. Allocate more entries than we'll likely need,
      since file size may vary, and entries are relatively small. */
   cache->n_ht_entries = 0;
   cache->max_ht_entries = (2 * size) / AVERAGE_FILE_SIZE;
   assert(cache->max_ht_entries > 0);
   cache->ht_size = cache->max_ht_entries * sizeof(hash_entry_t);
   if ((cache->ht_entries = mmap_alloc(cache->ht_size)) == NULL) {
      return -ENOMEM;
   }
   if ((cache->ht = mmap_alloc(sizeof(hash_entry_t))) == NULL) {
      mmap_free(cache->ht_entries, cache->ht_size);
      return -ENOMEM;
   }

   /* Synchronization initialization. */
   pthread_spin_init(&cache->ht_lock, PTHREAD_PROCESS_SHARED);

   /* Get log2 of the number of entries. */
   int max_ht_entries_copy = cache->max_ht_entries;
   int max_ht_entries_log2 = 0;
   while (max_ht_entries_copy >>= 1) ++max_ht_entries_log2;
   HASH_MAKE_TABLE(hh, cache->ht, 0, cache->max_ht_entries, max_ht_entries_log2);

   /* Allocate the cache's memory, and ensure it's 8-byte aligned so that direct
      IO will work properly. */
   if ((cache->data = mmap_alloc(cache->size)) == NULL) {
      mmap_free(cache->ht_entries, cache->ht_size);
      mmap_free(cache->ht, sizeof(hash_entry_t));
      return -ENOMEM;
   }

   return 0;
}