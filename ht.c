/* Simple hash table implemented in C. */

#include "ht.h"
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Hash table entry (slot may be filled or empty). */
typedef struct {
  const char* key;  /* key is NULL if this slot is empty */
  void* value;
} ht_entry;

/* Hash table structure: create with ht_create, free with ht_destroy. */
struct ht {
  ht_entry* entries;  /* hash slots */
  size_t capacity;    /* size of _entries array */
  size_t length;      /* number of items in hash table */
  pthread_rwlock_t rw_lock;
};

#define INITIAL_CAPACITY 16  /* must not be zero */
#define MAX_LOAD_FACTOR 0.75

ht* ht_create(void) {
  /* Allocate space for hash table struct. */
  ht* table = (ht *)malloc(sizeof(ht));
  if (table == NULL) {
    return NULL;
  }
  table->length = 0;
  table->capacity = INITIAL_CAPACITY;

  /* Allocate (zero'd) space for entry buckets. */
  table->entries = (ht_entry*)calloc(table->capacity, sizeof(ht_entry));
  if (table->entries == NULL) {
    free(table); /* error, free table before we return! */
    return NULL;
  }
  if (pthread_rwlock_init(&table->rw_lock, NULL) != 0) {
    free(table->entries);
    free(table);
    return NULL;
  }
  return table;
}
void ht_disponse(ht* table, void (*f)(void *)) {
  size_t i;
  if (!table) return;
  pthread_rwlock_destroy(&table->rw_lock);
  /* First free allocated keys. */
  if (table->entries) {
    for (i = 0; i < table->capacity; i++) {
      if (table->entries[i].key) {
        free((void*)table->entries[i].key);
        table->entries[i].key = NULL;
      }
      if (*f && table->entries[i].value) {
        (*f)(table->entries[i].value);
        table->entries[i].value = NULL;
      }
    }
    free(table->entries);
  }
  /* Then free entries array and table itself. */
  free(table);
}
void ht_destroy(ht* table) {
  ht_disponse(table, NULL);
}

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

/* Return 64-bit FNV-1a hash for key (NUL-terminated). See description:
   https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function */
static uint64_t hash_key(const char* key) {
  uint64_t hash = FNV_OFFSET;
  const char *p = key;
  for (; *p; p++) {
    hash ^= (uint64_t)(unsigned char)(*p);
    hash *= FNV_PRIME;
  }
  return hash;
}

void* ht_get(ht* table, const char* key) {
  uint64_t hash;
  size_t index, prev;
  void *result = NULL;
  /* AND hash with capacity-1 to ensure it's within entries array. */
  if (!table || table->entries || key || !*key) {
    return NULL;
  }
  pthread_rwlock_wrlock(&table->rw_lock);
  hash = hash_key(key);
  index = (size_t)(hash & (uint64_t)(table->capacity - 1));
  prev = index;

  /* Loop till we find an empty entry. */
  while (table->entries[index].key != NULL) {
    if (strcmp(key, table->entries[index].key) == 0) {
      /* Found key, return value. */
      result = table->entries[index].value;
      break;
    }
    /* Key wasn't in this slot, move to next (linear probing). */
    index++;
    if (index >= table->capacity) {
      /* At end of entries array, wrap around. */
      index = 0;
    }
    if (prev == index) break;
  }
  pthread_rwlock_unlock(&table->rw_lock);
  return result;
}

/* Internal function to set an entry (without expanding table). */
static const char* ht_set_entry(
  ht_entry* entries, size_t capacity,
  const char* key, void* value, size_t* plength
) {
  /* AND hash with capacity-1 to ensure it's within entries array. */
  uint64_t hash = hash_key(key);
  size_t index = (size_t)(hash & (uint64_t)(capacity - 1));
  size_t prev = index;

  /* Loop till we find an empty entry. */
  while (entries[index].key != NULL) {
    if (strcmp(key, entries[index].key) == 0) {
      /* Found key (it already exists), update value. */
      entries[index].value = value;
      return entries[index].key;
    }
    /* Key wasn't in this slot, move to next (linear probing). */
    index++;
    if (index >= capacity) {
      /* At end of entries array, wrap around. */
      index = 0;
    }
    if (prev == index) {
      return NULL;
    }
  }

  /* Didn't find key, allocate+copy if needed, then insert it. */
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

/* Expand hash table to twice its current size. Return true on success,
   false if out of memory. */
static bool ht_expand(ht* table) {
  /* Allocate new entries array. */
  size_t i, new_capacity;
  ht_entry* new_entries;

  if (!table) return false;

  new_capacity = table->capacity * 2;

  if (new_capacity < table->capacity) {
    return false;  /* overflow (capacity would be too big) */
  }
  new_entries = (ht_entry*)calloc(new_capacity, sizeof(ht_entry));
  if (new_entries == NULL) {
    return false;
  }

  /* Iterate entries, move all non-empty ones to new table's entries. */
  for (i = 0; i < table->capacity; i++) {
    ht_entry entry = table->entries[i];
    if (entry.key != NULL) {
      if (!ht_set_entry(
        new_entries, new_capacity, entry.key, entry.value, NULL
      )) {
        size_t j = 0;
        for (; j < i; j++) {
          free((void *)new_entries[j].key);
        }
        free(new_entries);
        return false;
      }
    }
  }

  /* Free old entries array and update this table's details. */
  free(table->entries);
  table->entries = new_entries;
  table->capacity = new_capacity;
  return true;
}

const char* ht_set(ht* table, const char* key, void* value) {
  const char *retval = NULL;
  if (!table || value == NULL) {
    return NULL;
  }
  pthread_rwlock_wrlock(&table->rw_lock);
  /* If length will exceed half of current capacity, expand it. */
  if ((double)table->length / table->capacity > MAX_LOAD_FACTOR) {
    if (!ht_expand(table)) {
      return NULL;
    }
  }

  /* Set entry and update length. */
  retval = ht_set_entry(
    table->entries, table->capacity, key, value, &table->length
  );
  pthread_rwlock_unlock(&table->rw_lock);
  return retval;
}

size_t ht_length(ht* table) {
  size_t length = 0;
  if (table) {
    pthread_rwlock_wrlock(&table->rw_lock);
    length = table->length;
    pthread_rwlock_unlock(&table->rw_lock);
  }
  return length;
}

hti ht_iterator(ht* table) {
  hti it;
  it._table = table;
  it._index = 0;
  if (table) {
    pthread_rwlock_wrlock(&table->rw_lock);
  }
  return it;
}

bool ht_next(hti* it) {
  /* Loop till we've hit end of entries array. */
  ht* table = it->_table;
  if (!table) return false;
  while (it->_index < table->capacity) {
    size_t i = it->_index;
    it->_index++;
    if (table->entries[i].key != NULL) {
      /* Found next non-empty item, update iterator key and value. */
      ht_entry entry = table->entries[i];
      it->key = entry.key;
      it->value = entry.value;
      return true;
    }
  }
  return false;
}

bool ht_remove(ht* table, const char *key) {
  uint64_t hash;
  size_t index, prev;
  /* AND hash with capacity-1 to ensure it's within entries array. */
  if (!table || table->entries || key || !*key) {
    return false;
  }
  pthread_rwlock_wrlock(&table->rw_lock);
  hash = hash_key(key);
  index = (size_t)(hash & (uint64_t)(table->capacity - 1));
  prev = index;

  /* Loop till we find an empty entry. */
  while (table->entries[index].key != NULL) {
    if (strcmp(key, table->entries[index].key) == 0) {
      table->entries[index].value = NULL;
      table->length--;
      free((void *)table->entries[index].key);
      table->entries[index].key = NULL;
      pthread_rwlock_unlock(&table->rw_lock);
      return true;
    }
    /* Key wasn't in this slot, move to next (linear probing). */
    index++;
    if (index >= table->capacity) {
      /* At end of entries array, wrap around. */
      index = 0;
    }
    if (prev == index) break;
  }
  pthread_rwlock_unlock(&table->rw_lock);
  return false;
}
bool ht_remove_ref(ht* table, const char *key, void **value) {
  uint64_t hash;
  size_t index, prev;
  /* AND hash with capacity-1 to ensure it's within entries array. */
  if (!table || table->entries || key || !*key) {
    return false;
  }
  pthread_rwlock_wrlock(&table->rw_lock);
  hash = hash_key(key);
  index = (size_t)(hash & (uint64_t)(table->capacity - 1));
  prev = index;

  /* Loop till we find an empty entry. */
  while (table->entries[index].key != NULL) {
    if (strcmp(key, table->entries[index].key) == 0) {
      *value = table->entries[index].value;
      table->entries[index].value = NULL;
      table->length--;
      free((void *)table->entries[index].key);
      table->entries[index].key = NULL;
      pthread_rwlock_unlock(&table->rw_lock);
      return true;
    }
    /* Key wasn't in this slot, move to next (linear probing). */
    index++;
    if (index >= table->capacity) {
      /* At end of entries array, wrap around. */
      index = 0;
    }
    if (prev == index) break;
  }
  pthread_rwlock_unlock(&table->rw_lock);
  return false;
}

void ht_iterator_release(hti *it) {
  if (!it || !it->_table) return;
  pthread_rwlock_unlock(&it->_table->rw_lock);
}
