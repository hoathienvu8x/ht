// Simple hash table implemented in C.

#include "ht.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Hash table entry (slot may be filled or empty).
typedef struct {
  const char* key;  // key is NULL if this slot is empty
  void* value;
} ht_entry;

// Hash table structure: create with ht_create, free with ht_destroy.
struct ht {
  ht_entry* entries;  // hash slots
  size_t capacity;  // size of _entries array
  size_t length;    // number of items in hash table
  pthread_mutex_t mtx;
};

#define INITIAL_CAPACITY 16  // must not be zero

ht* ht_create(void) {
  // Allocate space for hash table struct.
  ht* table = malloc(sizeof(ht));
  if (table == NULL) {
    return NULL;
  }
  table->length = 0;
  table->capacity = INITIAL_CAPACITY;

  // Allocate (zero'd) space for entry buckets.
  table->entries = calloc(table->capacity, sizeof(ht_entry));
  if (table->entries == NULL) {
    free(table); // error, free table before we return!
    return NULL;
  }

  if(pthread_mutex_init(&table->mtx, NULL) == 0) {
    return table;
  }
  free(table->entries);
  free(table);
  return NULL;
}

void ht_destroy(ht* table) {
  // First free allocated keys.
  for (size_t i = 0; i < table->capacity; i++) {
    free((void*)table->entries[i].key);
  }
  pthread_mutex_destroy(&table->mtx);
  // Then free entries array and table itself.
  free(table->entries);
  free(table);
}

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

// Return 64-bit FNV-1a hash for key (NUL-terminated). See description:
// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
static uint64_t hash_key(const char* key) {
  uint64_t hash = FNV_OFFSET;
  for (const char* p = key; *p; p++) {
    hash ^= (uint64_t)(unsigned char)(*p);
    hash *= FNV_PRIME;
  }
  return hash;
}

void* ht_get(ht* table, const char* key) {
  // AND hash with capacity-1 to ensure it's within entries array.
  uint64_t hash = hash_key(key);
  size_t index = (size_t)(hash & (uint64_t)(table->capacity - 1));
  size_t i = index;

  pthread_mutex_lock(&table->mtx);
  // Loop till we find an empty entry.
  while (table->entries[i].key != NULL) {
    if (strcmp(key, table->entries[i].key) == 0) {
      // Found key, return value.
      void * val = table->entries[i].value;
      pthread_mutex_unlock(&table->mtx);
      return val;
    }
    // Key wasn't in this slot, move to next (linear probing).
    i++;
    if (i >= table->capacity) {
      // At end of entries array, wrap around.
      i = 0;
    }
    if (i == index) break;
  }
  pthread_mutex_unlock(&table->mtx);
  return NULL;
}

// Internal function to set an entry (without expanding table).
static const char* ht_set_entry(ht_entry* entries, size_t capacity,
    const char* key, void* value, size_t* plength) {
  // AND hash with capacity-1 to ensure it's within entries array.
  uint64_t hash = hash_key(key);
  size_t index = (size_t)(hash & (uint64_t)(capacity - 1));
  size_t i = index;

  // Loop till we find an empty entry.
  while (entries[i].key != NULL) {
    if (strcmp(key, entries[i].key) == 0) {
      // Found key (it already exists), update value.
      entries[i].value = value;
      return entries[i].key;
    }
    // Key wasn't in this slot, move to next (linear probing).
    i++;
    if (i >= capacity) {
      // At end of entries array, wrap around.
      i = 0;
    }
    if (i == index) return NULL;
  }

  // Didn't find key, allocate+copy if needed, then insert it.
  if (plength != NULL) {
    key = strdup(key);
    if (key == NULL) {
      return NULL;
    }
    (*plength)++;
  }
  entries[index].key = (char*)key;
  entries[index].value = value;
  return key;
}

// Expand hash table to twice its current size. Return true on success,
// false if out of memory.
static bool ht_expand(ht* table) {
  // Allocate new entries array.
  size_t new_capacity = table->capacity * 2;
  if (new_capacity < table->capacity) {
    return false;  // overflow (capacity would be too big)
  }
  ht_entry* new_entries = calloc(new_capacity, sizeof(ht_entry));
  if (new_entries == NULL) {
    return false;
  }

  // Iterate entries, move all non-empty ones to new table's entries.
  for (size_t i = 0; i < table->capacity; i++) {
    ht_entry entry = table->entries[i];
    if (entry.key != NULL) {
      ht_set_entry(new_entries, new_capacity, entry.key, entry.value, NULL);
    }
  }

  // Free old entries array and update this table's details.
  free(table->entries);
  table->entries = new_entries;
  table->capacity = new_capacity;
  return true;
}

int ht_set(ht* table, const char* key, void* value) {
  assert(value != NULL);
  if (value == NULL) {
    return -1;
  }

  // If length will exceed half of current capacity, expand it.
pthread_mutex_lock(&table->mtx);
  if (table->length >= table->capacity / 2) {
    if (!ht_expand(table)) {
      pthread_mutex_unlock(&table->mtx);
      return -1;
    }
  }

  // Set entry and update length.
  const char * ret = ht_set_entry(
    table->entries, table->capacity, key, value, &table->length
  );
  pthread_mutex_unlock(&table->mtx);
  return ret ? 0 : -1;
}

size_t ht_length(ht* table) {
  return table->length;
}

hti ht_iterator(ht* table) {
  hti it;
  it._table = table;
  it._index = 0;
  return it;
}

bool ht_next(hti* it) {
  // Loop till we've hit end of entries array.
  ht* table = it->_table;
  pthread_mutex_lock(&table->mtx);
  while (it->_index < table->capacity) {
    size_t i = it->_index;
    it->_index++;
    if (table->entries[i].key != NULL) {
      // Found next non-empty item, update iterator key and value.
      ht_entry entry = table->entries[i];
      it->key = entry.key;
      it->value = entry.value;
      pthread_mutex_unlock(&table->mtx);
      return true;
    }
  }
  pthread_mutex_unlock(&table->mtx);
  return false;
}

int ht_remove(ht* table, const char* key) {
  uint64_t hash = hash_key(key);
  size_t index;
  if (!table) return -1;
  index = (size_t)(hash & (uint64_t)(table->capacity - 1));
  pthread_mutex_lock(&table->mtx);
  if (table->entries[index].key == NULL) {
    pthread_mutex_unlock(&table->mtx);
    return -1;
  }
  while (table->entries[index].key != NULL) {
    if (strcmp(key, table->entries[index].key) == 0) {
      free((void*)table->entries[index].key);
      table->length--;
      pthread_mutex_unlock(&table->mtx);
      return 0;
    }
  }
  pthread_mutex_unlock(&table->mtx);
  return -1;
}
