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


/* Check if CACHE contains PATH. Returns true if cached, else false. */
bool
cache_contains(cache_t *cache, char *path)
{
   printf("cache: %p, ht: %p, entries: %p, n entries: %lu, max entries: %lu, path: %s\n", cache, cache->ht, cache->ht_entries, cache->n_ht_entries, cache->max_ht_entries, path);

   hash_entry_t *entry = NULL;
   printf("calling HASH_FIND_STR\n");
   HASH_FIND_STR(cache->ht, path, entry);
   printf("other side\n");

   hash_entry_t *el, *tmp;
   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   return (entry != NULL);
}

/* Store DATA into CACHE indexed by PATH. On success, returns 0. On failure,
   returns negative errno value. */
int
cache_store(cache_t *cache, char *path, uint8_t *data, size_t size)
{
   hash_entry_t *el, *tmp;
   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   /* Acquire an entry. */
   size_t n = atomic_fetch_add(&cache->n_ht_entries, 1);
   if (n >= cache->max_ht_entries) {
      return -ENOMEM;
   }
   hash_entry_t *entry = &cache->ht_entries[n];

   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   /* Copy the path into the entry. */
   strncpy(entry->path, path, MAX_PATH_LENGTH);

   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   /* Figure out where the data goes. */
   entry->size = size;
   entry->ptr = cache->data + atomic_fetch_add(&cache->used, size);

   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   /* Copy data to the cache. */
   memcpy(entry->ptr, data, size);

   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   /* Insert into hash table. */
   pthread_spin_lock(&cache->ht_lock);
   HASH_ADD_STR(cache->ht, path, entry);
   pthread_spin_unlock(&cache->ht_lock);

   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   return 0;
}

/* Load the data at PATH in CACHE into DATA (a maximum of MAX bytes), storing
   the size of the file into SIZE. A cache miss is considered a failure
   (-ENODATA is returned without any IO being issued). On success returns 0.
   On failure returns negative errno. */
int
cache_load(cache_t *cache, char *path, uint8_t *data, size_t *size, size_t max)
{
   hash_entry_t *entry = NULL;
   HASH_FIND_STR(cache->ht, path, entry);
   if (entry == NULL) {
      return -ENODATA;
   }

   /* Don't overflow the buffer. */
   *size = entry->size;
   if (entry->size > max) {
      return -EINVAL;
   }
   memcpy(data, entry->ptr, entry->size);

   hash_entry_t *el, *tmp;
   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   return 0;
}

/* Read an item from CACHE into DATA, indexed by PATH, and located on the
   filesystem at PATH. On failure returns errno code with negative value,
   otherwise returns bytes read on success.
   
   DATA must be block-aligned, in order for direct IO to work properly.
   
   Note we use atomics to implement thread safe options because pthreads and
   traditional synchronization primitives are not safe to use with the Python
   interpreter, and may cause deadlock to occur, regardless of the correctness
   of primitives' usage.
   
   It should be noted that the cache is only thread/process safe so long as the
   cache entries are only written once (as is the case with MinIO). */
ssize_t
cache_read(cache_t *cache, char *path, void *data, uint64_t max_size)
{
   size_t n_accs = STAT_INC(cache, n_accs);
   if (n_accs % 2500 == 0) {
      DEBUG_LOG("[MinIO debug] accesses = %lu, hits = %lu, cold misses = %lu, capacity misses = %lu, fails = %lu (usage = %lu/%lu MB) (cache->data = %p) (&cache->used = %p) (pid = %d, ppid = %d)\n", cache->n_accs, cache->n_hits, cache->n_miss_cold, cache->n_miss_capacity, cache->n_fail, cache->used / (1024 * 1024), cache->size / (1024 * 1024), cache->data, &cache->used, getpid(), getppid());
   }

   /* Check if the file is cached. */
   size_t bytes = 0;
   int status = cache_load(cache, path, data, &bytes, max_size);
   if (status < 0) {
      if (status != -ENODATA) {
         return (ssize_t) status;
      }
   } else {
      STAT_INC(cache, n_hits);
      return (ssize_t) bytes;
   }

   /* Open the file in DIRECT mode. */
   int fd = open(path, O_RDONLY | __O_DIRECT);
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

   /* Note there is an implicit assumption that two threads/processes will not
      simultaneously attempt to access the same path for the *first* time.
      For ML data-loader applications this is satisfied, since each element is
      accessed only once per epoch, however this will present a race condition
      in applications where this scenario can occur. */

   /* Read into data and cache the data if it'll fit. */
   read(fd, data, (size | 0xFFF) + 1);
   close(fd);
   if ((size <= cache->size - cache->used) &&
       (size <= cache->max_item_size || cache->max_item_size == 0)) {
      int status = cache_store(cache, path, data, size);
      if (status < 0) {
         STAT_INC(cache, n_fail);
         return (ssize_t) status;
      }
      STAT_INC(cache, n_miss_cold);
   } else {
      STAT_INC(cache, n_miss_capacity);
   }

   hash_entry_t *el, *tmp;
   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
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
   cache->n_ht_entries = 0;
   cache->used = 0;
}

/* Initialize a cache CACHE with SIZE bytes and POLICY replacement policy. On
   success, 0 is returned. On failure, negative errno value is returned. */
int
cache_init(cache_t *cache,
           size_t size,
           size_t max_item_size,
           size_t avg_item_size,
           policy_t policy)
{
   /* Cache configuration. */
   cache->size = size;
   cache->used = 0;
   cache->policy = policy;
   cache->max_item_size = max_item_size;

   /* Zero initial stats. */
   cache->n_accs = 0;
   cache->n_fail = 0;
   cache->n_hits = 0;
   cache->n_miss_capacity = 0;
   cache->n_miss_cold = 0;

   /* Initialize the hash table. Allocate more entries than we'll likely need,
      since file size may vary, and entries are relatively small. */
   cache->n_ht_entries = 0;
   if (avg_item_size != 0) {
      cache->max_ht_entries = (2 * size) / avg_item_size;
   } else {
      cache->max_ht_entries = (2 * size) / AVERAGE_FILE_SIZE;
   }
   assert(cache->max_ht_entries > 0);
   cache->ht_size = (cache->max_ht_entries + 1) * sizeof(hash_entry_t);
   if ((cache->ht_entries = mmap_alloc(cache->ht_size)) == NULL) {
      return -ENOMEM;
   }
   cache->ht = &cache->ht_entries[cache->max_ht_entries];
   memset(cache->ht_entries, 0, cache->ht_size);

   /* Synchronization initialization. */
   pthread_spin_init(&cache->ht_lock, PTHREAD_PROCESS_SHARED);

   /* Get log2 of the number of entries. */
   int max_ht_entries_copy = cache->max_ht_entries;
   int max_ht_entries_log2 = 0;
   while (max_ht_entries_copy >>= 1) max_ht_entries_log2++;
   HASH_MAKE_TABLE(hh, cache->ht, 0, cache->max_ht_entries, max_ht_entries_log2);

   /* Allocate the cache's memory, and ensure it's 8-byte aligned so that direct
      IO will work properly. */
   if ((cache->data = mmap_alloc(cache->size)) == NULL) {
      mmap_free(cache->ht_entries, cache->ht_size);
      mmap_free(cache->ht, sizeof(hash_entry_t));
      return -ENOMEM;
   }

   hash_entry_t *el, *tmp;
   HASH_ITER(hh, cache->ht, el, tmp) {
      assert(el->hh.keylen < 128);
   }

   return 0;
}