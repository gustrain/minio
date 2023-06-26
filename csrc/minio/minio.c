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


/* ----------------- */
/*   SHARED MEMORY   */
/* ----------------- */

#define AVERAGE_FILE_SIZE (100 * 1024)

/* Allocate shared, page-locked memory, using an anonymous mmap. If this process
   forks, and all "shared" state was allocated using this function, everything
   will behave properly, as if we're synchronizing threads.
   
   Returns a pointer to a SIZE-byte region of memory on success, and returns
   NULL on failure. */
void *
mmap_alloc(size_t size)
{
   /* Allocate SIZE bytes of page-aligned memory in an anonymous shared mmap. */
   void *ptr = mmap(NULL, size,
                    PROT_READ | PROT_WRITE,
                    MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE,
                    -1, 0);

   /* Lock this region. */
   if (mlock(ptr, size) != 0) {
      /* Don't allow a double failure. */
      assert(munmap(ptr, size) == 0);
      return NULL;
   }

   return ptr;
}

/* ------------- */
/*   INTERFACE   */
/* ------------- */

#define STAT_INC(cache, field)                     \
         pthread_mutex_lock(&cache->stats_lock);   \
         cache->field++;                           \
         pthread_mutex_unlock(&cache->stats_lock)

/* Read an item from CACHE into DATA, indexed by FILEPATH, and located on the
   filesystem at FILEPATH. On failure returns ERRNO code with negative value,
   otherwise returns bytes read on success.
   
   DATA must be block-aligned, in order for direct IO to work properly. */
ssize_t
cache_read(cache_t *cache, char *filepath, void *data, uint64_t max_size)
{
   if (cache->n_accs % 1000 == 0) {
      printf("[MinIO debug] accesses = %lu, hits = %lu, cold misses = %lu, capacity misses = %lu, fails = %lu (usage = %lu/%lu MB) (cache->data = %p) (&cache->used = %p) (pid = %d, ppid = %d)\n", cache->n_accs, cache->n_hits, cache->n_miss_cold, cache->n_miss_capacity, cache->n_fail, cache->used / (1024 * 1024), cache->size / (1024 * 1024), cache->data, &cache->used, getpid(), getppid());
   }

   STAT_INC(cache, n_accs);

   /* Check if the file is cached. */
   hash_entry_t *entry = NULL;
   HASH_FIND_STR(cache->ht, filepath, entry);
   if (entry != NULL) {
      /* Don't overflow the buffer. */
      pthread_rwlock_rdlock(&entry->rwlock);
      if (entry->size > max_size) {
         pthread_rwlock_unlock(&entry->rwlock);
         return -EINVAL;
      }
      memcpy(data, entry->ptr, entry->size);
      pthread_rwlock_unlock(&entry->rwlock);

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
      pthread_mutex_lock(&cache->meta_lock);
      hash_entry_t *entry = &cache->ht_entries[cache->n_ht_entries++];
      pthread_mutex_unlock(&cache->meta_lock);
      if (cache->n_ht_entries > cache->max_ht_entries) {
         cache->n_fail++;
         return -ENOMEM;
      }
      HASH_ADD_STR(cache->ht, filepath, entry);
      pthread_mutex_unlock(&cache->meta_lock);

      /* Acquire the writer lock before writing. */
      pthread_rwlock_wrlock(&entry->rwlock);

      /* Copy the filepath into the entry. */
      strncpy(entry->filepath, filepath, MAX_PATH_LENGTH);
      entry->size = size;

      /* Copy data to the cache. */
      pthread_mutex_lock(&cache->meta_lock);
      entry->ptr = cache->data + cache->used;
      memcpy(entry->ptr, data, size);
      cache->used += size;
      pthread_mutex_unlock(&cache->meta_lock);
      pthread_rwlock_unlock(&entry->rwlock);

      STAT_INC(cache, n_miss_cold);
   } else {
      STAT_INC(cache, n_miss_capacity);
   }

   return size;
}

/* Clear the cache's hash table and reset used bytes to zero. */
void
cache_flush(cache_t *cache)
{
   /* Acquiring the meta lock will prevent N_HT_ENTRIES changing, so using this
      value as the HT iterator is safe. */
   pthread_mutex_lock(&cache->meta_lock);
   size_t old_n_entries = cache->n_ht_entries;

   /* Acquire every entry lock. */
   for (size_t i = 0; i < old_n_entries; i++) {
      pthread_rwlock_wrlock(&cache->ht_entries[i].rwlock);
   }
   
   /* Clear the HT and the cache metadata. */
   HASH_CLEAR(hh, cache->ht);
   cache->n_ht_entries = 0;
   cache->used = 0;

   /* Release every entry lock. */
   for (size_t i = 0; i < old_n_entries; i++) {
      pthread_rwlock_unlock(&cache->ht_entries[i].rwlock);
   }

   pthread_mutex_unlock(&cache->meta_lock);
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

   /* Initialize locks. */
   pthread_mutex_init(&cache->meta_lock, PTHREAD_MUTEX_NORMAL);
   pthread_mutex_init(&cache->stats_lock, PTHREAD_MUTEX_NORMAL);

   /* Initialize the hash table. Allocate more entries than we'll likely need,
      since file size may vary, and entries are relatively small. */
   cache->ht = NULL;
   cache->max_ht_entries = 2 * (size / AVERAGE_FILE_SIZE);
   cache->ht_size = cache->max_ht_entries * sizeof(hash_entry_t);
   if ((cache->ht_entries = mmap_alloc(cache->ht_size)) == NULL) {
      return -ENOMEM;
   }

   /* Set up each of the HT entries. */
   for (size_t i = 0; i < cache->max_ht_entries; i++) {
      hash_entry_t *entry = &cache->ht_entries[i];
      pthread_rwlock_init(&entry->rwlock, PTHREAD_RWLOCK_DEFAULT_NP);
   }

   /* Allocate the cache's memory, and ensure it's 8-byte aligned so that direct
      IO will work properly. */
   if ((cache->data = mmap_alloc(cache->size)) == NULL) {
      munmap(cache->ht_entries, cache->ht_size);
      return -ENOMEM;
   }

   printf("[MinIO debug] Initialized %lu byte cache, starting at %p (pid = %d)\n", cache->size, cache->data, getpid());

   return 0;
}