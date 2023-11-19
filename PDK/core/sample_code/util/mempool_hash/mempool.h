#include <stdlib.h>
#include <stdint.h>
#include <kvs_api.h>
#include "../logger.h"

#define BUFFER_SIZE (64ull * 1024 * 1024) // 64MB
#define PAGE_SIZE (4096ull) // 4KB
#define SMALL_LENGTH 1024
#define LARGE_LENGTH 4096

static inline unsigned int kv_mempool_hash_function(unsigned char *key, const int length)
{
	unsigned char *p = key;
	unsigned int h = 2166136261;
	int i;

	for (i = 0; i < length; i++)
		h = (h * 16777619) ^ p[i];

	return h;
}

struct kv_mempool_mapping_entry {
	char key[18]; // Currently supporting keys smaller than 18 bytes
	size_t mem_offset;
	size_t length;
    size_t in_page_offset;
	unsigned int next_slot;
};

#define KV_MEMPOOL_MAPPING_ENTRY_SIZE sizeof(struct kv_mempool_mapping_entry)
#define KV_MAPPING_TABLE_SIZE (1ULL * 1024 * 1024 * 1024)
struct lru_node {
	unsigned int mempool_frame_id;
	unsigned int log_offset;
	unsigned int next, prev;
};

struct lru {
	int total_size;
	int left_size;
    int head_index;
    int tail_index;
	struct lru_node *lru_queue;
};

struct kv_mempool_page_frame {
	struct mempool_page
	{
		uint8_t data[PAGE_SIZE];
	} page; // The persist part
	bool is_dirty;
	int df_id;
  uint64_t mem_offset;
  int cur_local_offset;
};

struct memory_pool {
	struct kv_mempool_page_frame data[BUFFER_SIZE / PAGE_SIZE];
	bool is_empty[BUFFER_SIZE / PAGE_SIZE];
	int write_buffer_id;
	struct lru *mempool_lru;
	void *LruCache;
};

struct kv_mempool_ftl {
	struct kv_mempool_mapping_entry *kv_mapping_table;
	unsigned long hash_slots;
	struct memory_pool *mempool;
};

static inline unsigned int kv_mempool_hash_function(char *key, const int length)
{
	unsigned char *p = (unsigned char*)key;
	unsigned int h = 2166136261;
	int i;

	for (i = 0; i < length; i++)
		h = (h * 16777619) ^ p[i];

	return h;
}
static unsigned int get_hash_slot(struct kv_mempool_ftl *kv_mempool_ftl, char *key, uint32_t key_len)
{
	return kv_mempool_hash_function(key, key_len) % kv_mempool_ftl->hash_slots;
}

static void chain_mapping(struct kv_mempool_ftl *kv_mempool_ftl, unsigned int prev, unsigned int slot)
{
	kv_mempool_ftl->kv_mapping_table[prev].next_slot = slot;
}

static unsigned int find_next_slot(struct kv_mempool_ftl *kv_mempool_ftl, int original_slot, int *prev_slot)
{
	unsigned int ret_slot = original_slot;

	// 1. Find the tail of the link.
	unsigned int tail = original_slot;
	unsigned int prevs = -1;
	while (kv_mempool_ftl->kv_mapping_table[tail].mem_offset != -1) {	
		prevs = tail;
		tail = kv_mempool_ftl->kv_mapping_table[tail].next_slot;
		if (tail == -1) break;
	}

	ret_slot = prevs;
	*prev_slot = prevs;

	// 2. Search the next available slots starting from the tail.
	while (kv_mempool_ftl->kv_mapping_table[ret_slot].mem_offset != -1) {
		ret_slot++;
		if (ret_slot >= kv_mempool_ftl->hash_slots)
			ret_slot = 0;
	}

	// *prev_slot = original_slot;

	if (*prev_slot < 0) {
		perror("Prev slot less than 0\n");
	}

	DEBUG("Collision at slot %d, found new slot %u\n", original_slot, ret_slot);
	if (ret_slot - original_slot > 3)
		DEBUG("Slot difference: %d\n", ret_slot - original_slot);

	return ret_slot;
}

static struct kv_mempool_mapping_entry get_kv_mempool_mapping_entry(struct kv_mempool_ftl *kv_mempool_ftl, kvs_key* key)
{
	struct kv_mempool_mapping_entry mapping;
	// char *key = NULL;
	unsigned int slot = 0;
	bool found = false;
	// u64 t0, t1;

	uint32_t count = 0;

	memset(&mapping, -1, sizeof(struct kv_mempool_mapping_entry)); // init mapping

	// t0 = ktime_get_ns();
	slot = get_hash_slot(kv_mempool_ftl, (char*)key->key, key->length);
	// t1 = ktime_get_ns();
	// printk("Hashing took %llu\n", t1-t0);

	while (kv_mempool_ftl->kv_mapping_table[slot].mem_offset != -1) {
		DEBUG("Comparing %s | %.*s\n", cmd.kv_store.key, cmd_key_length(cmd),
			    kv_mempool_ftl->kv_mapping_table[slot].key);
		count++;

		if (count > 10) {
			DEBUG("Searched %u times", count);
			// break;
		}

		if (memcmp((char*)key->key, kv_mempool_ftl->kv_mapping_table[slot].key,
			   key->length) == 0) {
			DEBUG("1 Found\n");
			found = true;
			break;
		}

		slot = kv_mempool_ftl->kv_mapping_table[slot].next_slot;
		if (slot == -1)
			break;
		DEBUG("Next slot %d", slot);
		// t1 = ktime_get_ns();
		// printk("Comparison took %llu", t1-t0);
	}

	if (found) {
		DEBUG("2 Found\n");
		memcpy(mapping.key, kv_mempool_ftl->kv_mapping_table[slot].key, key->length);
		mapping.mem_offset = kv_mempool_ftl->kv_mapping_table[slot].mem_offset;
		mapping.next_slot = kv_mempool_ftl->kv_mapping_table[slot].next_slot;
		mapping.length = kv_mempool_ftl->kv_mapping_table[slot].length;
		mapping.in_page_offset = kv_mempool_ftl->kv_mapping_table[slot].in_page_offset;
		DEBUG("mem_offset %zu, next_slot %u, length %zu, in_page_offset %zu", mapping.mem_offset, mapping.next_slot, mapping.length, mapping.in_page_offset);
	}

	if (!found) {
		DEBUG("No mapping found for key %s\n", cmd.kv_store.key);
    }
	else {
		DEBUG("Returning mapping %lu length %lu for key %s\n", mapping.mem_offset,
			    mapping.length, cmd.kv_store.key);
    }

	return mapping;
}


class Memhash {
public:
    int32_t ssd;
    struct kv_mempool_ftl *mempool_ftl;
    Memhash(const char* ssd_path) {
        int flags = O_RDWR | O_DIRECT | O_SYNC;
        ssd = open(ssd_path, flags, 0666);
        mempool_ftl = (struct kv_mempool_ftl*)malloc(sizeof(struct kv_mempool_ftl));
        mempool_ftl->kv_mapping_table = (struct kv_mempool_mapping_entry*)malloc(KV_MAPPING_TABLE_SIZE);
        if (mempool_ftl->kv_mapping_table == NULL)
		    perror("Failed to map kv mapping table.\n");
        else
            memset(mempool_ftl->kv_mapping_table, 0x0, KV_MAPPING_TABLE_SIZE);

        // Allocate space for memory pool
        mempool_ftl->mempool = (struct memory_pool*)malloc(sizeof(struct memory_pool));
        mempool_ftl->mempool->write_buffer_id = -1;
        if (!mempool_ftl->mempool) {
            perror ("Failed to allocate mempool.\n");
        }

        int nr_buffer_slots = (int) BUFFER_SIZE / PAGE_SIZE;
        for (int i = 0; i < nr_buffer_slots; i++) {
            mempool_ftl->mempool->data[i].is_dirty = false;
            mempool_ftl->mempool->data[i].df_id = i;
            mempool_ftl->mempool->data[i].mem_offset = -1;
            mempool_ftl->mempool->data[i].cur_local_offset = 0;
            memset(mempool_ftl->mempool->data[i].page.data, 0x0, PAGE_SIZE);
            mempool_ftl->mempool->is_empty[i] = true;
        }

        // Allocate space for LRU in mempool
        mempool_ftl->mempool->mempool_lru = (struct lru*)malloc(sizeof(struct lru));
        mempool_ftl->mempool->mempool_lru->lru_queue = (struct lru_node*)malloc(sizeof(struct lru_node) * nr_buffer_slots);
        for (int i = 0; i < nr_buffer_slots; i++) {
            mempool_ftl->mempool->mempool_lru->lru_queue[i].next = i + 1;
            mempool_ftl->mempool->mempool_lru->lru_queue[i].prev = i - 1;
        }
        mempool_ftl->mempool->mempool_lru->lru_queue[nr_buffer_slots - 1].next = -1;
        mempool_ftl->mempool->mempool_lru->total_size = nr_buffer_slots;
        mempool_ftl->mempool->mempool_lru->left_size = nr_buffer_slots;
        mempool_ftl->mempool->mempool_lru->head_index = 0;
        mempool_ftl->mempool->mempool_lru->tail_index = 0;

        // LRU
        if (0 == LRUCacheCreate(nr_buffer_slots, &mempool_ftl->mempool->LruCache))
            printf("缓存器创建成功,容量为%d\n", nr_buffer_slots);
        
        mempool_ftl->hash_slots = KV_MAPPING_TABLE_SIZE / KV_MEMPOOL_MAPPING_ENTRY_SIZE;
        printf ("Hash slots: %ld\n", mempool_ftl->hash_slots);

        for (uint32_t i = 0; i < mempool_ftl->hash_slots; i++) {
            mempool_ftl->kv_mapping_table[i].mem_offset = -1;
            mempool_ftl->kv_mapping_table[i].next_slot = -1;
            mempool_ftl->kv_mapping_table[i].length = -1;
        }

    }

    ~Memhash() {
        close(ssd);
        LRUCacheDestory(mempool_ftl->mempool->LruCache);
    }

    unsigned int new_kv_mempool_mapping_entry(kvs_key* key, kvs_value* val,
				      size_t val_offset, size_t in_page_offset) {
        unsigned int slot = -1;
        unsigned int prev_slot;
        // assert(val_offset < 0 || val_offset >= storage_size);

        slot = get_hash_slot(mempool_ftl, key->key, key->length);

        prev_slot = -1;
        if (mempool_ftl->kv_mapping_table[slot].mem_offset != -1) {
            DEBUG("Collision\n");
            slot = find_next_slot(mempool_ftl, slot, &prev_slot);
        }

        if (slot < 0 || slot >= mempool_ftl->hash_slots) {
            perror("slot < 0 || slot >= kv_mempool_ftl->hash_slots\n");
        }

        memcpy(mempool_ftl->kv_mapping_table[slot].key, key->key, key->length + 1);
        mempool_ftl->kv_mapping_table[slot].mem_offset = val_offset;
        mempool_ftl->kv_mapping_table[slot].in_page_offset = in_page_offset;
        mempool_ftl->kv_mapping_table[slot].length = val->length;
        /* hash chaining */
        if (prev_slot != -1) {
            DEBUG("Linking slot %d to new slot %d", prev_slot, slot);
            chain_mapping(mempool_ftl, prev_slot, slot);
        }

        DEBUG("New mapping entry key %s offset %lu length %u slot %u\n", cmd.kv_store.key,
                val_offset, cmd_value_length(cmd), slot);

        return 0;
    }

    unsigned int update_kv_mempool_mapping_entry(kvs_key* key, kvs_value* val)
    {
        unsigned int slot = 0;
        bool found = false;
        // u64 t0, t1;

        uint32_t count = 0;

        // t0 = ktime_get_ns();
        slot = get_hash_slot(mempool_ftl, (char*)key->key, key->length);
        // t1 = ktime_get_ns();
        // printk("Hashing took %llu\n", t1-t0);

        while (mempool_ftl->kv_mapping_table[slot].mem_offset != -1) {
            DEBUG("Comparing %s | %.*s\n", key->key, key->length,
                    mempool_ftl->kv_mapping_table[slot].key);
            count++;

            if (count > 10) {
                DEBUG("Searched %u times", count);
                // break;
            }

            if (memcmp(key->key, mempool_ftl->kv_mapping_table[slot].key, key->length) == 0) {
                DEBUG("1 Found\n");
                found = true;
                break;
            }

            slot = mempool_ftl->kv_mapping_table[slot].next_slot;
            if (slot == -1)
                break;
            // t1 = ktime_get_ns();
            // printk("Comparison took %llu", t1-t0);
        }

        if (found) {
            DEBUG("Updating mapping length %lu to %u for key %s\n",
                    mempool_ftl->kv_mapping_table[slot].length, val->length,
                    key->key);
            mempool_ftl->kv_mapping_table[slot].length = val->length;
        }

        if (!found) {
            DEBUG("No mapping found for key %s\n", key->key);
            return 1;
        }

        return 0;
    }

    // Get write buffer means there is a KV ready to write
    struct kv_mempool_page_frame * get_kv_mempool_for_write (kvs_value *val) {
        size_t offset;
        int i;
        int nr_buffer_slots;
        int wb_id;
        uint64_t key; // memory offset mapping
        uint64_t value; // buffer frame id
        int available_length;
        int cmd_val_length;
        uint64_t start_buffer_flush_time;
        uint64_t target_buffer_flush_time;

        // 1. check if there is a valid write buffer
        nr_buffer_slots = BUFFER_SIZE / PAGE_SIZE;
        wb_id = mempool_ftl->mempool->write_buffer_id;
        if (wb_id == -1) {
            // 1.1. No write buffer now, allocate a new one and get the offset
            offset = lseek(ssd, 0, SEEK_CUR);
            
            DEBUG(">> Allocate new buffer with file offset %zu <<\n", offset);
            // 1.2. find an empty page frame
            for (i = 0; i < nr_buffer_slots; i++) {
                if (mempool_ftl->mempool->is_empty[i]) {
                    mempool_ftl->mempool->is_empty[i] = false;
                    mempool_ftl->mempool->write_buffer_id = i;
                    mempool_ftl->mempool->data[i].mem_offset = offset;

                    // 1.3 update the LRU
                    if (0 != LRUCacheSet(mempool_ftl->mempool->LruCache, offset, i)) {
                        perror("Bad LRU update\n");
                    }
                    // LRUCachePrint(kv_mempool_ftl->mempool->LruCache);
                    return &mempool_ftl->mempool->data[i];
                }
            }
            perror ("Bad write buffer allocation");
        }
        // 2. use current write buffer
        // 2.1. check current write has available space
        available_length =
            PAGE_SIZE - mempool_ftl->mempool->data[wb_id].cur_local_offset;
        cmd_val_length = val->length;
        if (available_length >= cmd_val_length) {
            DEBUG("available_length %d cmd_val_length %d\n", available_length, cmd_val_length);
            return &mempool_ftl->mempool->data[wb_id];
        }
        // 3. Allocate a new write buffer
        offset = lseek(ssd, PAGE_SIZE, SEEK_CUR);
        
        for (i = 0; i < nr_buffer_slots; i++) {
            if (mempool_ftl->mempool->is_empty[i]) {
                mempool_ftl->mempool->is_empty[i] = false;
                mempool_ftl->mempool->write_buffer_id = i;
                mempool_ftl->mempool->data[i].mem_offset = offset;
                if (0 != LRUCacheSet(mempool_ftl->mempool->LruCache, offset, i)) {
                    perror("Bad LRU update\n");
                }
                return &mempool_ftl->mempool->data[i];
            }
        }
        // 4. Allocate a new write buffer failed! Unload a page from dataframe to reserved memory space (flash)
        // 4.1. find the tail of the LRU and get its buffer frame id and memory offset mapping
        getTailFromList((LRUCacheS*)mempool_ftl->mempool->LruCache, &key, &value);
        // 4.2. unload the memory
        DEBUG("Buffer Frame %llu (offset %llu) is full, flush to the disk\n", value, key);
        int ret = write(ssd, &mempool_ftl->mempool->data[value].page, PAGE_SIZE); // write to ssd

        // 4.3. reset the buffer frame and allocate a new buffer, update the LRU entry as well
        // memset(&kv_mempool_ftl->mempool->data[value], 0, sizeof(struct kv_mempool_page_frame));
        mempool_ftl->mempool->data[value].mem_offset = -1;
        mempool_ftl->mempool->data[value].is_dirty = false;
        mempool_ftl->mempool->data[value].cur_local_offset = 0;
        memset(mempool_ftl->mempool->data[value].page.data, 0x0, PAGE_SIZE);
        mempool_ftl->mempool->is_empty[value] = true;

        offset = lseek(ssd, PAGE_SIZE, SEEK_CUR);

        mempool_ftl->mempool->is_empty[value] = false;
        mempool_ftl->mempool->write_buffer_id = value;
        mempool_ftl->mempool->data[value].mem_offset = offset;
        if (0 != LRUCacheSet(mempool_ftl->mempool->LruCache, offset, value)) {
            perror("Bad LRU update\n");
        }
        return &mempool_ftl->mempool->data[value];
    }

    // Read from
    struct kv_mempool_page_frame * get_kv_mempool_for_read (kvs_value * val, uint64_t mem_offset) {
        uint64_t offset;
        int ret;
        int i;
        int nr_buffer_slots;
        int wb_id;
        uint64_t key; // memory offset mapping
        uint64_t value; // buffer frame id
        int available_length;
        int cmd_val_length;
        uint64_t start_buffer_flush_time;
        uint64_t target_buffer_load_time;
        uint64_t target_buffer_flush_time;

        // 1. check if the page is in memory pool
        uint64_t pf_id = LRUCacheGet(mempool_ftl->mempool->LruCache, mem_offset);
        if (pf_id != -1) {
            DEBUG("LRU cache hit at pf_id %llu and mem_offset %llu\n", pf_id, mem_offset);
            return &mempool_ftl->mempool->data[pf_id];
        }

        // 2. LRU cache miss
        // 2.1. memory pool has available frame
        nr_buffer_slots = BUFFER_SIZE / PAGE_SIZE;
        for (i = 0; i < nr_buffer_slots; i++) {
            if (mempool_ftl->mempool->is_empty[i]) {
                mempool_ftl->mempool->is_empty[i] = false;
                mempool_ftl->mempool->data[i].mem_offset = mem_offset;
                // copy page from flash to the frame

                offset = lseek(ssd, mem_offset, SEEK_SET);
                ret = read(ssd, &mempool_ftl->mempool->data[i].page, PAGE_SIZE);
                DEBUG("Buffer Frame %d is empty, load page from disk (offset %llu)\n", i, mem_offset);

                if (0 != LRUCacheSet(mempool_ftl->mempool->LruCache, mem_offset, i)) {
                    perror("Bad LRU update\n");
                }
                return &mempool_ftl->mempool->data[i];
            }
        }
        // 2.2. memory pool has no available frame

        // 3.1. Allocate a new write buffer failed! Unload a page from dataframe to reserved memory space (flash)
        // 3.2. find the tail of the LRU and get its buffer frame id and memory offset mapping
        getTailFromList((LRUCacheS *)mempool_ftl->mempool->LruCache, &key, &value);
        // 3.3. unload the memory
        offset = lseek(ssd, key, SEEK_SET);
        ret = write(ssd, &mempool_ftl->mempool->data[value].page, PAGE_SIZE);
        
        DEBUG("Buffer Frame %llu (offset %llu) is full, flush to the flash with delay\n", value, key);

        // 3.4. reset the buffer frame and load the page from flash, update the LRU entry as well
        // memset(&kv_mempool_ftl->mempool->data[value], 0, sizeof(struct kv_mempool_page_frame));
        mempool_ftl->mempool->data[value].mem_offset = -1;
        mempool_ftl->mempool->data[value].is_dirty = false;
        mempool_ftl->mempool->data[value].cur_local_offset = 0;
        memset(mempool_ftl->mempool->data[value].page.data, 0x0, PAGE_SIZE);
        mempool_ftl->mempool->is_empty[value] = true;

        // 3.5. load the page from flash
        offset = lseek(ssd, mem_offset, SEEK_SET);
        ret = read(ssd, &mempool_ftl->mempool->data[value].page, PAGE_SIZE);

        DEBUG("Buffer Frame %d is empty, load page from flash (offset %llu) with delay\n", value, mem_offset);

        if (0 != LRUCacheSet(mempool_ftl->mempool->LruCache, mem_offset, value)) {
            perror("Bad LRU update\n");
        }

        return &mempool_ftl->mempool->data[value];
    }

    int Put(kvs_key *key, kvs_value *value) {
        // 1. Get the buffer from the buffer pool
        int is_insert = 0;

	    struct kv_mempool_page_frame *pf;
        struct kv_mempool_mapping_entry entry = get_kv_mempool_mapping_entry(mempool_ftl, key);
        uint64_t offset = entry.mem_offset;
        uint64_t mem_local_offset = entry.in_page_offset;
        uint64_t length = value->length;
        uint64_t remaining;

        if (entry.mem_offset == -1) { // entry doesn't exist -> is insert
			pf = get_kv_mempool_for_write(value);

			is_insert = 1; // is insert

			DEBUG("[kv_store insert] %s to bf %d memoffset %llu in_page_offset %d\n", cmd.kv_store.key, pf->df_id, pf->mem_offset, pf->cur_local_offset);
		} else {
			DEBUG("[kv_store update] %s %lu\n", cmd.kv_store.key, offset);
			pf = get_kv_mempool_for_read(value, entry.mem_offset);

			if (length != entry.length) {
				if (length <= SMALL_LENGTH && entry.length <= SMALL_LENGTH) {
					is_insert = 2; // is update with different length;
				} else {
					perror("Length size invalid!!");
				}
			}
		}

        memcpy(pf->page.data + pf->cur_local_offset, value->value, value->length);
        entry.in_page_offset = pf->cur_local_offset;
        pf->cur_local_offset += value->length;

        if (is_insert == 1) { // need to make new mapping
            new_kv_mempool_mapping_entry(key, value, pf->mem_offset, entry.in_page_offset);
        } else if (is_insert == 2) {
            update_kv_mempool_mapping_entry(key, value);
        }

        return 1;

    }

    int Get(kvs_key *key, kvs_value *value){
        struct kv_mempool_page_frame *pf;
        struct kv_mempool_mapping_entry entry = get_kv_mempool_mapping_entry(mempool_ftl, key);
        uint64_t offset = entry.mem_offset;
        uint64_t mem_local_offset = entry.in_page_offset;
        uint64_t length = value->length;
        uint64_t remaining;

        if (entry.mem_offset == -1) { // kv pair doesn't exist
			DEBUG("[kv_retrieve] %s no exist\n", key->key);
			return 0; // dev_status_code for KVS_ERR_KEY_NOT_EXIST
		} else {
			length = std::min(entry.length, length);
			pf = get_kv_mempool_for_read(value, entry.mem_offset);

			DEBUG("[kv_retrieve] %s exist - length %lu, offset %lu\n",
				    key->key, length, offset);
		}

        memcpy(value->value, pf->page.data + pf->cur_local_offset + entry.in_page_offset, length);
        
		return length;
    }
    int Delete(){}
};

