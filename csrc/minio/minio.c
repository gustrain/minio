/* Author:  Gus Waldspurger

   Implementation of a MinIO file cache.
   */

#include "minio.h"

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
   otherwise returns bytes read on success. */
ssize_t
cache_read(cache_t *cache, char *filepath, void *data, uint64_t max_size)
{
   /* Check if the file is cached. */
   hash_entry_t *entry = NULL;
   HASH_FIND_STR(cache->ht, filepath, entry);
   if (entry != NULL) {
      /* Don't overflow the buffer. */
      if (entry->size > max_size) {
         return -EINVAL;
      }
      memcpy(data, entry->ptr, entry->size);

      return entry->size;
   }

   /* Open the file in DIRECT mode. */
   int fd = open(filepath, O_RDONLY | __O_DIRECT);
   if (fd < 0) {
      return -ENOENT;
   }
   FILE *file = fdopen(fd, "r");
   if (file == NULL) {
      close(fd);
      return -EBADFD;
   }

   /* Ensure the size of the file is OK. */
   fseek(file, 0L, SEEK_END);
   size_t size = ftell(file);
   if (size > max_size) {
      fclose(file);
      close(fd);
      return -EINVAL;
   }
   rewind(file);

   /* Read into data and cache the data if it'll fit. */
   read(fd, data, size);
   fclose(file);
   close(fd);
   if (size <= cache->size - cache->used) {
      /* Make an entry. */
      entry = malloc(sizeof(hash_entry_t));
      if (entry == NULL) {
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

   /* Allocate the cache's memory. */
   if ((cache->data = malloc(size)) == NULL) {
      return -ENOMEM;
   }

   /* Pin the cache's memory. */
   if (mlock(cache->data, size) != 0) {
      return -EPERM;
   }

   return 0;
}