#include <immintrin.h>

#include <condition_variable>  // std::condition_variable
#include <cstdlib>
#include <mutex>   // std::mutex
#include <thread>  // std::thread

#include "gflags/gflags.h"
#include "util/histogram.h"
#include "util/logger.h"
#include "util/slice.h"
#include "util/time.h"
#include "util/test_util.h"

#include <unistd.h>
#include <queue>
#include <kvs_api.h>

#include <random>

#define kvs_key_t uint16_t

#define SUCCESS 0
#define FAILED 1
#define DEBUG_ON 1
#define ITER_BUFFER_SIZE (32*1024)

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

using namespace util;

#define likely(x) (__builtin_expect (false || (x), true))
#define unlikely(x) (__builtin_expect (x, 0))

// For hash table
DEFINE_uint32 (batch, 100, "report batch");
DEFINE_uint32 (readtime, 0, "if 0, then we read all keys");
DEFINE_uint32 (thread, 1, "");
DEFINE_uint64 (report_interval, 1, "Report interval in seconds");
DEFINE_uint64 (stats_interval, 200000000, "Report interval in ops");
DEFINE_uint32 (key_size, 8, "The key size");
DEFINE_uint32 (value_size, 64, "The value size");
DEFINE_uint64 (num, 1000000LU, "Number of total record");
DEFINE_uint64 (read, 0, "Number of read operations");
DEFINE_uint64 (write, 0, "Number of read operations");
DEFINE_string (device_path, "0001:10:00.0", "KV SSD device path, eg.0001:10:00.0, ");
DEFINE_string (keyspace_name, "keyspace_test", "keyspace name");
DEFINE_uint64 (qdepth, 64, "queue depth of aio");

DEFINE_bool (hist, false, "");

DEFINE_string (benchmarks, "load", "");

namespace {

pthread_mutex_t lock;

struct kv_pool {
  std::queue<char*> keypool;
  std::queue<char*> valuepool;
};

struct common_data {
  std::atomic<int> completed;
  std::atomic<int> cur_qdepth;
  int thread_id;
};

kvs_key* _allocate_kvskey(void *key_buff,  kvs_key_t key_len) {
  kvs_key *kvskey = (kvs_key *)malloc(sizeof(kvs_key));
  if(!kvskey) {
    fprintf(stderr, "failed to allocate\n");
    return NULL;
  }
  kvskey->key = key_buff;
  kvskey->length = key_len;
  return kvskey;
}

kvs_value* _allocate_kvsvalue(void *val_buff, uint32_t val_len) {
  kvs_value *kvsvalue = (kvs_value *)malloc(sizeof(kvs_value));
  if(!kvsvalue) {
    fprintf(stderr, "failed to allocate\n");
    return NULL;
  }
  kvsvalue->value = val_buff;
  kvsvalue->length = val_len;
  kvsvalue->actual_value_size = kvsvalue->offset = 0;
  return kvsvalue;
}

bool _allocate_kvs_pair(void *key_buff, kvs_key_t key_len, kvs_key **kvskey,
  void *val_buff, uint32_t val_len, kvs_value **kvsvalue) {
  *kvskey = _allocate_kvskey(key_buff, key_len);
  if(!(*kvskey))
    return false;
  *kvsvalue = _allocate_kvsvalue(val_buff, val_len);
  if(!(*kvsvalue)){
    free(*kvskey);
    return false;
  }
  return true;
}

void _free_str_pool(std::queue<char *> *pool, pthread_mutex_t *pool_lock){
  pthread_mutex_lock(pool_lock);
  while(!pool->empty()) {
      auto p = pool->front();
      pool->pop();
      kvs_free(p);
  }
  pthread_mutex_unlock(pool_lock);
}

// malloc string pool
bool _malloc_str_pool(uint32_t pool_size, std::queue<char *> *pool,
  pthread_mutex_t *pool_lock, uint32_t str_len){
  for (uint32_t i = 0; i < pool_size; i++) {
    char *keymem = (char*)kvs_malloc(str_len, 4096);
    if(!keymem) {
      _free_str_pool(pool, pool_lock);
      return false;
    }
    memset(keymem, 0, str_len);  
    pool->push(keymem);
  }
  return true;
}

// If value pool NULL inputted, will not malloc value pool.
bool _malloc_kv_pool(uint32_t pool_size, std::queue<char *> *key_pool,
  uint32_t key_len, std::queue<char*> *val_pool, uint32_t val_len,
  pthread_mutex_t *pool_lock){
  if( !_malloc_str_pool(pool_size, key_pool, pool_lock, key_len)) {
    return false;
  }

  if(val_pool) {
    if( !_malloc_str_pool(pool_size, val_pool, pool_lock, val_len)) {
      _free_str_pool(key_pool, pool_lock);
      return false;
    }
  }
  return true;
}

void _free_kv_pool(std::queue<char *> *key_pool,
  std::queue<char *> *val_pool, pthread_mutex_t *pool_lock) {
  _free_str_pool(key_pool, pool_lock);
  if(val_pool)
    _free_str_pool(val_pool, pool_lock);
}

void _free_kvs_pair(kvs_key *key, kvs_value *value,
  std::queue<char *> *keypool, std::queue<char *> *valpool,
  pthread_mutex_t *pool_lock) {
  pthread_mutex_lock(pool_lock);
  if(key) {
    if(key->key) {
        memset((char*)key->key, 0 ,key->length);
        keypool->push((char*)key->key);
    }
    free(key);
  }
  if(value) {
    if(value->value) {
        memset((char*)value->value, 0 ,value->length);
        valpool->push((char*)value->value);
    }
    free(value);
  }
  pthread_mutex_unlock(pool_lock);
}

void _free_kv_buff(void *key_buff, void *val_buff,
  std::queue<char *> *keypool, std::queue<char *> *valpool,
  pthread_mutex_t *pool_lock) {
  pthread_mutex_lock(pool_lock);
  if(key_buff)
    keypool->push((char*)key_buff);
  if(val_buff)
    keypool->push((char*)val_buff);
  pthread_mutex_unlock(pool_lock);
}

double _calc_time_span(struct timespec start_time) {
  struct timespec curr_time;
  clock_gettime(CLOCK_REALTIME, &curr_time);
  unsigned long long start, end;
  start = start_time.tv_sec * 1000000000L + start_time.tv_nsec;
  end = curr_time.tv_sec * 1000000000L + curr_time.tv_nsec;
  return (double)(end - start) / 1000000000L;
}

void print_iterator_keyvals(kvs_iterator_list *iter_list, kvs_option_iterator g_iter_mode){
  uint8_t *it_buffer = (uint8_t *) iter_list->it_list;
  uint32_t i;

  if(g_iter_mode.iter_type) {
    // key and value iterator (KVS_ITERATOR_KEY_VALUE)
    uint32_t vlen = sizeof(uint32_t);
    uint32_t vlen_value = 0;
      
    for(i = 0;i < iter_list->num_entries; i++) {
      fprintf(stdout, "Iterator get %dth key: %s\n", i, it_buffer);
      it_buffer += 16;

      uint8_t *addr = (uint8_t *)&vlen_value;
      for (unsigned int i = 0; i < vlen; i++) {
         *(addr + i) = *(it_buffer + i);
      }

      it_buffer += vlen;
      it_buffer += vlen_value;
    }

  } else {
    // for ETA50K24 firmware with various key length       
    uint32_t key_size = 0;
    char key[256];

    for(i = 0;i < iter_list->num_entries; i++) {
      // get key size
      key_size = *((unsigned int*)it_buffer);
      it_buffer += sizeof(unsigned int);

      // print key
      memcpy(key, it_buffer, key_size);
      key[key_size] = 0;
      fprintf(stdout, "%dth key --> %s, size = %d\n", i, key, key_size);

      it_buffer += key_size;
    }
  }
}

void _iterator_complete_handle(kvs_postprocess_context* ioctx) {
  if (ioctx->result != KVS_SUCCESS) {
    fprintf(stderr, "ERROR io_complete: iterator result=0x%x\n",
       ioctx->result);
    return;
  }
  kvs_iterator_list* iter_list = ioctx->result_buffer.iter_list;
#if DEBUG_ON
  fprintf(stdout, "io_complete: op=%d, end=%d, num entries=%d, size=%d, result=0x%x\n",
    ioctx->context, iter_list->end,iter_list->num_entries,
    iter_list->size, ioctx->result);
  kvs_option_iterator option = {KVS_ITERATOR_KEY};
  print_iterator_keyvals(iter_list, option);
#endif
}

void _key_exist_complete_handle(kvs_postprocess_context* ioctx) {
  if (ioctx->result != 0) {
    fprintf(stderr, "ERROR io_complete: key exist, key = %s, result = 0x%x\n",
      ioctx->key ? (char*)ioctx->key->key:0, ioctx->result);
    return;
  } 

  uint8_t *exist;
  exist = (uint8_t*)ioctx->result_buffer.list->result_buffer;
#if DEBUG_ON
  fprintf(stdout, "key %s exist? %s\n", (char*)ioctx->key->key,
          *exist == 0? "FALSE":"TRUE");
#endif
  if(ioctx->result_buffer.list->result_buffer)
    free(ioctx->result_buffer.list->result_buffer);
  if(ioctx->result_buffer.list)
    free(ioctx->result_buffer.list);
  kv_pool *pool;
  pool = (kv_pool*)(ioctx->private1);
  _free_kvs_pair(ioctx->key, NULL, &(pool->keypool), NULL, &lock);
}

void _delete_complete_handle(kvs_postprocess_context* ioctx) {
  if(ioctx->result != KVS_SUCCESS) {
    fprintf(stdout, "ERROR io_complete: delete . result=%d key=%s\n",
    ioctx->result, (char*)ioctx->key->key);
  }
#if DEBUG_ON
  fprintf(stderr, "delete key %s done \n", (char*)ioctx->key->key);
#endif
  kv_pool *pool;
  pool = (kv_pool*)(ioctx->private1);
  _free_kvs_pair(ioctx->key, NULL, &(pool->keypool), NULL, &lock);
}

void _default_complete_handle(kvs_postprocess_context* ioctx) {
  if(ioctx->result != KVS_SUCCESS) {
    fprintf(stdout, "ERROR io_complete: op=%d. result=%d key=%s\n",
    ioctx->context, ioctx->result, (char*)ioctx->key->key);
  }

  kv_pool *pool;
  pool = (kv_pool*)(ioctx->private1);
  _free_kvs_pair(ioctx->key, ioctx->value, &(pool->keypool), &(pool->valuepool),
                 &lock);
}

void complete(kvs_postprocess_context* ioctx) {
  switch(ioctx->context) {
    case KVS_CMD_ITER_NEXT:
      _iterator_complete_handle(ioctx);
      break;
    case KVS_CMD_DELETE:
      _delete_complete_handle(ioctx);
      break;
    case KVS_CMD_EXIST:
      _key_exist_complete_handle(ioctx);
      break;
    case KVS_CMD_STORE:
    case KVS_CMD_RETRIEVE:
      _default_complete_handle(ioctx);
      break;
    default:
      fprintf(stdout, "ERROR io_complete unknow op = %d, result=0x%x.\n",
        ioctx->context, ioctx->result);
      break;
  }
  common_data *data;
  data = (common_data*)(ioctx->private2);

  data->completed++;
  data->cur_qdepth--;
  if (data->completed.load() % 1000000 == 0)
    fprintf(stdout, "thread [%d] completed io count: %d\n", data->thread_id,
            data->completed.load());
}


int _env_exit(kvs_device_handle dev, char* keyspace_name,
    kvs_key_space_handle ks_hd) {
    uint32_t dev_util = 0;
    kvs_get_device_utilization(dev, &dev_util);
    fprintf(stdout, "After: Total used is %d\n", dev_util);  
    kvs_close_key_space(ks_hd);
    kvs_key_space_name ks_name;
    ks_name.name_len = strlen(keyspace_name);
    ks_name.name = keyspace_name;
    kvs_delete_key_space(dev, &ks_name);
    kvs_result ret = kvs_close_device(dev);
    return ret;
}

int _env_init(char* dev_path, kvs_device_handle* dev, char *keyspace_name,
    kvs_key_space_handle* ks_hd) {
    kvs_result ret = kvs_open_device(dev_path, dev);
    if(ret != KVS_SUCCESS) {
        fprintf(stderr, "Device open failed 0x%x\n", ret);
        return 0;
    }

    //keyspace list after create "test"
    const uint32_t retrieve_cnt = 2;
    kvs_key_space_name names[retrieve_cnt];
    for(uint8_t idx = 0; idx < retrieve_cnt; idx++) {
        names[idx].name_len = MAX_KEYSPACE_NAME_LEN;
        names[idx].name = (char*)malloc(MAX_KEYSPACE_NAME_LEN);
    }

    uint32_t valid_cnt = 0;
    ret = kvs_list_key_spaces(*dev, 1, retrieve_cnt*sizeof(kvs_key_space_name),
    names, &valid_cnt);
    if(ret != KVS_SUCCESS) {
        fprintf(stderr, "List current keyspace failed. error:0x%x.\n", ret);
        kvs_close_device(*dev);
        return 0;
    }
    for (uint8_t idx = 0; idx < valid_cnt; idx++) {
        kvs_delete_key_space(*dev, &names[idx]);
    }

    //create key spaces
    kvs_key_space_name ks_name;
    kvs_option_key_space option = { KVS_KEY_ORDER_NONE };
    ks_name.name = keyspace_name;
    ks_name.name_len = strlen(keyspace_name);
    //currently size of keyspace is not support specify
    ret = kvs_create_key_space(*dev, &ks_name, 0, option);
    if (ret != KVS_SUCCESS) {
        kvs_close_device(*dev);
        fprintf(stderr, "Create keyspace failed. error:0x%x.\n", ret);
        return 0;
    }

    ret = kvs_open_key_space(*dev, keyspace_name, ks_hd);
    if(ret != KVS_SUCCESS) {
        fprintf(stderr, "Open keyspace %s failed. error:0x%x.\n", keyspace_name, ret);
        kvs_delete_key_space(*dev, &ks_name);
        kvs_close_device(*dev);
        return 0;
    }

    kvs_key_space ks_info;
    ks_info.name = (kvs_key_space_name *)malloc(sizeof(kvs_key_space_name));
    if(!ks_info.name) {
        fprintf(stderr, "Malloc resource failed.\n");
        _env_exit(*dev, keyspace_name, *ks_hd);
        return 0;
    }
    ks_info.name->name = (char*)malloc(MAX_CONT_PATH_LEN);
    if(!ks_info.name->name) {
        fprintf(stderr, "Malloc resource failed.\n");
        free(ks_info.name);
        _env_exit(*dev, keyspace_name, *ks_hd);
        return 0;
    }
    ks_info.name->name_len = MAX_CONT_PATH_LEN;
    ret = kvs_get_key_space_info(*ks_hd, &ks_info);
    if(ret != KVS_SUCCESS) {
        fprintf(stderr, "Get info of keyspace failed. error:0x%x.\n", ret);
        free(ks_info.name->name);
        free(ks_info.name);
        return 0;
    }
    fprintf(stdout, "Keyspace information get name: %s\n", ks_info.name->name);
    fprintf(stdout, "open:%d, count:%ld, capacity:%ld, free_size:%ld.\n", 
    ks_info.opened, ks_info.count, ks_info.capacity, ks_info.free_size);
    free(ks_info.name->name);
    free(ks_info.name);

    uint32_t dev_util = 0;
    uint64_t dev_capa = 0;
    kvs_get_device_utilization(*dev, &dev_util);
    kvs_get_device_capacity(*dev, &dev_capa);
    fprintf(stdout, "Before: Total size is %ld bytes, used is %d\n", dev_capa, dev_util);
    kvs_device *dev_info = (kvs_device*)malloc(sizeof(kvs_device));
    if(dev_info) {
        kvs_get_device_info(*dev, dev_info);
        fprintf(stdout, "Total size: %.2f GB\nMax value size: %d\nMax key size: %d\nOptimal value size: %d\n",
        (float)dev_info->capacity/1000/1000/1000, dev_info->max_value_len,
        dev_info->max_key_len, dev_info->optimal_value_len);
        free(dev_info);
    }

    return 1;
}

class Stats {
public:
    int tid_;
    double start_;
    double finish_;
    double seconds_;
    double next_report_time_;
    double last_op_finish_;
    unsigned last_level_compaction_num_;
    HistogramImpl hist_;

    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t last_report_finish_;
    uint64_t next_report_;
    std::string message_;

    Stats () { Start (); }
    explicit Stats (int id) : tid_ (id) { Start (); }

    void Start () {
        start_ = util::NowMicros ();
        next_report_time_ = start_ + FLAGS_report_interval * 1000000;
        next_report_ = 100;
        last_op_finish_ = start_;
        last_report_done_ = 0;
        last_report_finish_ = start_;
        last_level_compaction_num_ = 0;
        done_ = 0;
        seconds_ = 0;
        finish_ = start_;
        message_.clear ();
        hist_.Clear ();
    }

    void Merge (const Stats& other) {
        hist_.Merge (other.hist_);
        done_ += other.done_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty ()) message_ = other.message_;
    }

    void Stop () {
        finish_ = util::NowMicros ();
        seconds_ = (finish_ - start_) * 1e-6;
        ;
    }

    void StartSingleOp () { last_op_finish_ = util::NowMicros (); }

    void PrintSpeed () {
        uint64_t now = util::NowMicros ();
        int64_t usecs_since_last = now - last_report_finish_;

        std::string cur_time = TimeToString (now / 1000000);
        // printf (
        //     "%s ... thread %d: (%lu,%lu) ops and "
        //     "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
        //     cur_time.c_str (), tid_, done_ - last_report_done_, done_,
        //     (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
        //     done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
        //     (now - start_) / 1000000.0);
        // INFO (
        //     "%s ... thread %d: (%lu,%lu) ops and "
        //     "( %.1f,%.1f ) ops/second in (%.6f,%.6f) seconds\n",
        //     cur_time.c_str (), tid_, done_ - last_report_done_, done_,
        //     (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
        //     done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
        //     (now - start_) / 1000000.0);
        
        printf ("[Epoch] %d,%lu,%lu,%.4f,%.4f\n", tid_, done_ - last_report_done_, done_,
              (now - last_report_finish_) / 1000000.0, (now - start_) / 1000000.0);
        INFO ("[Epoch] %d,%lu,%lu,%.4f,%.4f\n", tid_, done_ - last_report_done_, done_,
              (now - last_report_finish_) / 1000000.0, (now - start_) / 1000000.0);
        
        
        last_report_finish_ = now;
        last_report_done_ = done_;
        fflush (stdout);
    }

    static void AppendWithSpace (std::string* str, const std::string& msg) {
        if (msg.empty ()) return;
        if (!str->empty ()) {
            str->push_back (' ');
        }
        str->append (msg.data (), msg.size ());
    }

    void AddMessage (const std::string& msg) { AppendWithSpace (&message_, msg); }

    inline void FinishedBatchOp (size_t batch) {
        double now = util::NowNanos ();
        last_op_finish_ = now;
        done_ += batch;
        if (unlikely (done_ >= next_report_)) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && util::NowMicros () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
        }
    }

    inline void FinishedSingleOp () {
        double now = util::NowNanos ();
        last_op_finish_ = now;

        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && util::NowMicros () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
        }
    }

    std::string TimeToString (uint64_t secondsSince1970) {
        const time_t seconds = (time_t)secondsSince1970;
        struct tm t;
        int maxsize = 64;
        std::string dummy;
        dummy.reserve (maxsize);
        dummy.resize (maxsize);
        char* p = &dummy[0];
        localtime_r (&seconds, &t);
        snprintf (p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900, t.tm_mon + 1,
                  t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
        return dummy;
    }

    void Report (const Slice& name, bool print_hist = false) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;

        AppendWithSpace (&extra, message_);

        double elapsed = (finish_ - start_) * 1e-6;

        double throughput = (double)done_ / elapsed;

        printf ("%-12s : %11.3f micros/op %lf Mops/s;%s%s\n", name.ToString ().c_str (),
                elapsed * 1e6 / done_, throughput / 1024 / 1024, (extra.empty () ? "" : " "),
                extra.c_str ());
        INFO ("%-12s : %11.3f micros/op %lf Mops/s;%s%s\n", name.ToString ().c_str (),
              elapsed * 1e6 / done_, throughput / 1024 / 1024, (extra.empty () ? "" : " "),
              extra.c_str ());
        if (print_hist) {
            fprintf (stdout, "Nanoseconds per op:\n%s\n", hist_.ToString ().c_str ());
        }

        fflush (stdout);
        fflush (stderr);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    std::mutex mu;
    std::condition_variable cv;
    int total;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized;
    int num_done;
    bool start;

    SharedState (int total) : total (total), num_initialized (0), num_done (0), start (false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;  // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    YCSBGenerator ycsb_gen;
    ThreadState (int index) : tid (index), stats (index) {}
};

class Duration {
public:
    Duration (uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = util::NowMicros ();
    }

    inline int64_t GetStage () { return std::min (ops_, max_ops_ - 1) / ops_per_stage_; }

    inline bool Done (int64_t increment) {
        if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = 1000;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = util::NowMicros ();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

    inline int64_t Ops () { return ops_; }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace (std::string s) {
    size_t start = 0;
    while (start < s.size () && isspace (s[start])) {
        start++;
    }
    size_t limit = s.size ();
    while (limit > start && isspace (s[limit - 1])) {
        limit--;
    }
    return std::string (s.data () + start, limit - start);
}
#endif

}  // namespace
class Benchmark {
public:
    uint64_t num_;
    size_t reads_;
    size_t writes_;
    RandomKeyTrace* key_trace_;
    size_t trace_size_;
    char ks_name[MAX_KEYSPACE_NAME_LEN];
    kvs_device_handle dev;
    kvs_key_space_handle ks_hd;
    uint16_t klen;
    uint32_t vlen;
    uint64_t maxdepth;
    Benchmark ()
        : num_ (FLAGS_num),
          trace_size_ (FLAGS_num),
          reads_ (FLAGS_read),
          writes_ (FLAGS_write),
          key_trace_ (nullptr),
          klen (FLAGS_key_size),
          vlen (FLAGS_value_size),
          maxdepth (FLAGS_qdepth) {
            snprintf(ks_name, MAX_KEYSPACE_NAME_LEN, "%s", FLAGS_keyspace_name.c_str());
            if(_env_init(FLAGS_device_path.c_str(), &dev, ks_name, &ks_hd) != 1) {
                ERROR ("Initialize env error .. ");
                exit(0);
            }

          }
    ~Benchmark () {
        if (key_trace_ != nullptr) {
            delete key_trace_;
            _env_exit(dev, ks_name, ks_hd);
        }
    }
    void Run () {
        printf ("key trace size: %lu\n", trace_size_);
        key_trace_ = new RandomKeyTrace (trace_size_);
        if (reads_ == 0) {
            reads_ = key_trace_->count_ / FLAGS_thread;
            FLAGS_read = key_trace_->count_ / FLAGS_thread;
        }
        PrintHeader ();
        bool fresh_db = true;
        // run benchmark
        const char* benchmarks = FLAGS_benchmarks.c_str ();
        while (benchmarks != nullptr) {
            int thread = FLAGS_thread;
            bool print_hist = false;
            void (Benchmark::*method) (ThreadState*) = nullptr;
            const char* sep = strchr (benchmarks, ',');
            std::string name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = std::string (benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }
            if (name == "load") {
                method = &Benchmark::DoWrite;
            } else if (name == "loadverify") {
                method = &Benchmark::DoWriteRead;
            } else if (name == "loadlat") {
                print_hist = true;
                method = &Benchmark::DoWriteLat;
            } else if (name == "overwrite") {
                key_trace_->Randomize ();
                method = &Benchmark::DoOverWrite;
            } else if (name == "delete") {
                key_trace_->Randomize ();
                method = &Benchmark::DoDelete;
            } else if (name == "readrandom") {
                key_trace_->Randomize ();
                method = &Benchmark::DoRead;
            } else if (name == "readall") {
                // key_trace_->Randomize ();
                method = &Benchmark::DoReadAll;
            } else if (name == "readlatest") {
                // key_trace_->Randomize ();
                method = &Benchmark::DoReadLatest;
            } else if (name == "readnon") {
                key_trace_->Randomize ();
                method = &Benchmark::DoReadNon;
            } else if (name == "readlat") {
                print_hist = true;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadLat;
            } else if (name == "readnonlat") {
                print_hist = true;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadNonLat;
            } else if (name == "stats") {
                thread = 1;
                method = &Benchmark::DoStats;
            } else if (name == "ycsba") {
                key_trace_->Randomize ();
                method = &Benchmark::YCSBA;
            } else if (name == "ycsbb") {
                key_trace_->Randomize ();
                method = &Benchmark::YCSBB;
            } else if (name == "ycsbc") {
                key_trace_->Randomize ();
                method = &Benchmark::YCSBC;
            } else if (name == "ycsbd") {
                method = &Benchmark::YCSBD;
            } else if (name == "ycsbf") {
                key_trace_->Randomize ();
                method = &Benchmark::YCSBF;
            }
            if (method != nullptr) RunBenchmark (thread, name, method, print_hist);
        }
    }

    void DoStats (ThreadState* thread) {
        thread->stats.Start ();
        char buf[100];
        snprintf (buf, sizeof (buf), "Info");
        thread->stats.AddMessage (buf);
    }

    void DoRead (ThreadState* thread) {
        INFO ("DoRead");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoRead lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();

        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                auto res = 0;
                if (unlikely (!res)) {
                    not_find++;
                    // INFO("Not find key: %lu\n", *key_iterator);
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        // printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, reads_, not_find);
        INFO ("DoRead thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, reads_,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadAll (ThreadState* thread) {
        INFO ("DoAIORead");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoWrite lack key_trace_ initialization.");
            return;
        }
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);

        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

        /* aio */
        kv_pool pool;
        common_data data;
        int num_submitted = 0;
        data.completed = 0;
        data.cur_qdepth = 0;
        data.thread_id = thread->tid;

        if ( !_malloc_kv_pool(maxdepth, &(pool.keypool), klen, &(pool.valuepool), vlen, &lock)) {
            printf("Allocate kv pool failed.\n");
            return FAILED;
        }

        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);


        thread->stats.Start ();
        size_t not_find = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            uint64_t valid_ops = 0;
            for (; j < batch && key_iterator.Valid () && data.cur_qdepth < maxdepth; j++) {
                pthread_mutex_lock(&lock);
                char *key_   = pool.keypool.front(); pool.keypool.pop();
                char *value_ = pool.valuepool.front(); pool.valuepool.pop();
                pthread_mutex_unlock(&lock);
                if (!key_ || !value_) {
                    fprintf(stderr, "ERROR, key value memory pools exhaust.\n");
                    return;
                }

                size_t key = key_iterator.Next ();
                sprintf(key_, "%0*d", klen - 1, key);
                memset(value_, 0, vlen);

                kvs_option_retrieve option = {false};
                kvs_key *kvskey = NULL;
                kvs_value *kvsvalue = NULL;
                if ( !_allocate_kvs_pair(key_, klen, &kvskey, value_, vlen, &kvsvalue)) {
                    _free_kv_buff(key_, value_, &(pool.keypool), &(pool.valuepool), &lock);
                    return;
                }

                kvs_result ret = kvs_retrieve_kvp_async(ks_hd, kvskey, &option, &pool, &data, kvsvalue, complete);
                if (ret != KVS_SUCCESS ) {
                    fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
                    not_find++;
                } else {
                    valid_ops++;
                }
                data.cur_qdepth++;
                num_submitted++;
            }
            thread->stats.FinishedBatchOp (valid_ops);
            if (data.cur_qdepth == maxdepth) {
               usleep(1);
            }
        }

        // wait until commands that has succussfully submitted finish
        while (data.completed < num_submitted) {
            usleep(1);
        }
        _free_kv_pool(&(pool.keypool), &(pool.valuepool), &lock);


        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", interval, not_find);
        // printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, interval, not_find);
        INFO ("DoReadAll thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, interval,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadLatest (ThreadState* thread) {
        INFO ("DoAIOReadLatest");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoWrite lack key_trace_ initialization.");
            return;
        }
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);

        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

        /* aio */
        kv_pool pool;
        common_data data;
        int num_submitted = 0;
        data.completed = 0;
        data.cur_qdepth = 0;
        data.thread_id = thread->tid;

        if ( !_malloc_kv_pool(maxdepth, &(pool.keypool), klen, &(pool.valuepool), vlen, &lock)) {
            printf("Allocate kv pool failed.\n");
            return FAILED;
        }

        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        // Read the latest 20%
        auto key_iterator =
            key_trace_->iterate_between (start_offset + 0.8 * interval, start_offset + interval);
        printf ("thread %2d, between %lu - %lu\n", thread->tid,
                (size_t) (start_offset + 0.8 * interval), start_offset + interval);


        thread->stats.Start ();
        size_t not_find = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            uint64_t valid_ops = 0;
            for (; j < batch && key_iterator.Valid () && data.cur_qdepth < maxdepth; j++) {
                pthread_mutex_lock(&lock);
                char *key_   = pool.keypool.front(); pool.keypool.pop();
                char *value_ = pool.valuepool.front(); pool.valuepool.pop();
                pthread_mutex_unlock(&lock);
                if (!key_ || !value_) {
                    fprintf(stderr, "ERROR, key value memory pools exhaust.\n");
                    return;
                }

                size_t key = key_iterator.Next ();
                sprintf(key_, "%0*d", klen - 1, key);
                memset(value_, 0, vlen);

                kvs_option_retrieve option = {false};
                kvs_key *kvskey = NULL;
                kvs_value *kvsvalue = NULL;
                if ( !_allocate_kvs_pair(key_, klen, &kvskey, value_, vlen, &kvsvalue)) {
                    _free_kv_buff(key_, value_, &(pool.keypool), &(pool.valuepool), &lock);
                    return;
                }

                kvs_result ret = kvs_retrieve_kvp_async(ks_hd, kvskey, &option, &pool, &data, kvsvalue, complete);
                if (ret != KVS_SUCCESS ) {
                    fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
                    not_find++;
                } else {
                    valid_ops++;
                }
                data.cur_qdepth++;
                num_submitted++;
            }
            thread->stats.FinishedBatchOp (valid_ops);
            if (data.cur_qdepth == maxdepth) {
               usleep(1);
            }
        }

        // wait until commands that has succussfully submitted finish
        while (data.completed < num_submitted) {
            usleep(1);
        }
        _free_kv_pool(&(pool.keypool), &(pool.valuepool), &lock);


        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", interval, not_find);
        // printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, interval, not_find);
        INFO ("DoReadAll thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, interval,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadNon (ThreadState* thread) {
        INFO ("DoReadNon");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoReadNon lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next () + num_;
                bool res = 0;
                if (likely (!res)) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        // printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, reads_, not_find);
        INFO ("DoReadNon thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, reads_,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadLat (ThreadState* thread) {
        INFO ("DoReadLat");
        if (key_trace_ == nullptr) {
            ERROR ("DoReadLat lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (1) && key_iterator.Valid ()) {
            size_t key = key_iterator.Next ();
            auto time_start = util::NowNanos ();
            auto res = 0;
            auto time_duration = util::NowNanos () - time_start;
            thread->stats.hist_.Add (time_duration);

            if (unlikely (!res)) {
                not_find++;
            }
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        INFO ("DoReadLat thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, reads_,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadNonLat (ThreadState* thread) {
        INFO ("DoReadNonLat");
        if (key_trace_ == nullptr) {
            ERROR ("DoReadNonLat lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (1) && key_iterator.Valid ()) {
            size_t key = key_iterator.Next () + num_;
            auto time_start = util::NowNanos ();
            auto res = 0;
            auto time_duration = util::NowNanos () - time_start;
            thread->stats.hist_.Add (time_duration);
            if (likely (!res)) {
                not_find++;
            }
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        INFO ("DoReadNonLat thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, reads_,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoWrite (ThreadState* thread) {
        INFO ("DoAIOWrite");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoWrite lack key_trace_ initialization.");
            return;
        }
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);

        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

        /* aio */
        kv_pool pool;
        common_data data;
        int num_submitted = 0;
        data.completed = 0;
        data.cur_qdepth = 0;
        data.thread_id = thread->tid;

        if ( !_malloc_kv_pool(maxdepth, &(pool.keypool), klen, &(pool.valuepool), vlen, &lock)) {
            printf("Allocate kv pool failed.\n");
            return FAILED;
        }

        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);


        thread->stats.Start ();
        size_t not_inserted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            uint64_t valid_ops = 0;
            for (; j < batch && key_iterator.Valid () && data.cur_qdepth < maxdepth; j++) {
                pthread_mutex_lock(&lock);
                char *key_   = pool.keypool.front(); pool.keypool.pop();
                char *value_ = pool.valuepool.front(); pool.valuepool.pop();
                pthread_mutex_unlock(&lock);
                if (!key_ || !value_) {
                    fprintf(stderr, "ERROR, key value memory pools exhaust.\n");
                    return;
                }

                size_t key = key_iterator.Next ();
                sprintf(key_, "%0*d", klen - 1, key);

                kvs_option_store option;
                option.st_type = KVS_STORE_POST;
                option.assoc = NULL;
                kvs_key *kvskey = NULL;
                kvs_value *kvsvalue = NULL;
                if ( !_allocate_kvs_pair(key_, klen, &kvskey, value_, vlen, &kvsvalue)) {
                    _free_kv_buff(key_, value_, &(pool.keypool), &(pool.valuepool), &lock);
                    return;
                }

                kvs_result ret = kvs_store_kvp_async(ks_hd, kvskey, kvsvalue, &option, &pool, &data, complete);
                if (ret != KVS_SUCCESS ) {
                    fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
                    not_inserted++;
                } else {
                    valid_ops++;
                }
                data.cur_qdepth++;
                num_submitted++;
            }
            thread->stats.FinishedBatchOp (valid_ops);
            if (data.cur_qdepth == maxdepth) {
               usleep(1);
            }
        }

        // wait until commands that has succussfully submitted finish
        while (data.completed < num_submitted) {
            usleep(1);
        }
        _free_kv_pool(&(pool.keypool), &(pool.valuepool), &lock);

        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not inserted: %lu)", interval, not_inserted);
        if (not_inserted)
            printf ("thread %2d num: %lu, not inserted: %lu\n", thread->tid, interval, not_inserted);
        INFO ("DoWrite thread: %2d. Total write num: %lu, not inserted: %lu)", thread->tid, interval,
              not_inserted);
        thread->stats.AddMessage (buf);
        return;
    }

    void DoWriteRead (ThreadState* thread) {
        INFO ("DoWriteRead");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoWriteRead lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();
        size_t not_find = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                bool res = 0;
                if (!res) {
                }
                res = 0;
                if (!res) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", interval, not_find);
        if (not_find)
            printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, interval, not_find);
        INFO ("DoWriteRead thread: %2d. Total read num: %lu, not find: %lu)", thread->tid, interval,
              not_find);
        thread->stats.AddMessage (buf);
    }

    void DoWriteLat (ThreadState* thread) {
        INFO ("DoWriteLat");
        if (key_trace_ == nullptr) {
            ERROR ("DoWriteLat lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            size_t key = key_iterator.Next ();
            auto time_start = util::NowNanos ();
            bool res = 0;
            auto time_duration = util::NowNanos () - time_start;
            thread->stats.hist_.Add (time_duration);
            if (!res) {
                INFO ("Hash Table Full!!!\n");
                printf ("Hash Table Full!!!\n");
            }
        }
        return;
    }

    void DoOverWrite (ThreadState* thread) {
        INFO ("DoOverWrite");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoOverWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                bool res = 0;
                if (!res) {
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        return;
    }

    void DoDelete (ThreadState* thread) {
        INFO ("DoDelete");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoDelete lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();
        size_t deleted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                auto res = 0;
                if (res) {
                    deleted++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, deleted: %lu)", interval, deleted);
        INFO ("(num: %lu, deleted: %lu)", interval, deleted);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBA (ThreadState* thread) {
        INFO ("YCSBA");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("YCSBA lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextA () == kYCSB_Write) {
                    // TODO: Insert
                    insert++;
                } else {
                    // TODO: Read
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        INFO ("(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBB (ThreadState* thread) {
        INFO ("YCSBB");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("YCSBB lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextB () == kYCSB_Write) {
                    // TODO: Insert
                    insert++;
                } else {
                    // TODO: Read
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        INFO ("(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBC (ThreadState* thread) {
        INFO ("YCSBC");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("YCSBC lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                // TODO: Read
                auto res = 0;
                if (res) {
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        INFO ("(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBD (ThreadState* thread) {
        INFO ("YCSBD");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("YCSBD lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        // Read the latest 20%
        auto key_iterator =
            key_trace_->iterate_between (start_offset + 0.8 * interval, start_offset + interval);
        printf ("thread %2d, between %lu - %lu\n", thread->tid,
                (size_t) (start_offset + 0.8 * interval), start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                // TODO: Read
                auto res = 0;
                if (res) {
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        INFO ("(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBF (ThreadState* thread) {
        INFO ("YCSBF");
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("YCSBF lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextF () == kYCSB_Read) {
                    // TODO: Read
                    auto res = 0;
                    if (res) {
                        find++;
                    }
                } else {
                    // TODO: Read
                    // TODO: Insert
                    insert++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(read_modify: %lu, read: %lu)", insert, find);
        INFO ("(read_modify: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method) (ThreadState*);
    };

    static void ThreadBody (void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*> (v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.notify_all ();
            }
            while (!shared->start) {
                shared->cv.wait (lck);
            }
        }

        thread->stats.Start ();
        (arg->bm->*(arg->method)) (thread);
        thread->stats.Stop ();

        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.notify_all ();
            }
        }
    }

    void RunBenchmark (int thread_num, const std::string& name,
                       void (Benchmark::*method) (ThreadState*), bool print_hist) {
        SharedState shared (thread_num);
        ThreadArg* arg = new ThreadArg[thread_num];
        std::thread server_threads[thread_num];
        for (int i = 0; i < thread_num; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState (i);
            arg[i].thread->shared = &shared;
            server_threads[i] = std::thread (ThreadBody, &arg[i]);
        }

        std::unique_lock<std::mutex> lck (shared.mu);
        while (shared.num_initialized < thread_num) {
            shared.cv.wait (lck);
        }

        shared.start = true;
        shared.cv.notify_all ();
        while (shared.num_done < thread_num) {
            shared.cv.wait (lck);
        }

        for (int i = 1; i < thread_num; i++) {
            arg[0].thread->stats.Merge (arg[i].thread->stats);
        }
        arg[0].thread->stats.Report (name, print_hist);

        for (auto& th : server_threads) th.join ();

        for (int i = 0; i < thread_num; i++) {
            delete arg[i].thread;
        }
        delete[] arg;
    }

    void PrintEnvironment () {
#if defined(__linux)
        time_t now = time (nullptr);
        fprintf (stderr, "Date:                  %s", ctime (&now));  // ctime() adds newline

        FILE* cpuinfo = fopen ("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets (line, sizeof (line), cpuinfo) != nullptr) {
                const char* sep = strchr (line, ':');
                if (sep == nullptr) {
                    continue;
                }
                std::string key = TrimSpace (std::string (line, sep - 1 - line));
                std::string val = TrimSpace (std::string (sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val;
                } else if (key == "cache size") {
                    cache_size = val;
                }
            }
            fclose (cpuinfo);
            fprintf (stderr, "CPU:                   %d * %s\n", num_cpus, cpu_type.c_str ());
            fprintf (stderr, "CPUCache:              %s\n", cache_size.c_str ());
        }
#endif
    }

    void PrintHeader () {
        INFO ("------------------------------------------------\n");
        fprintf (stdout, "------------------------------------------------\n");
        PrintEnvironment ();
        fprintf (stdout, "Key type:              %s\n", "String");
        INFO ("Key type:              %s\n", "String");
        fprintf (stdout, "Val type:              %s\n", "String");
        INFO ("Val type:              %s\n", "String");
        fprintf (stdout, "Keys:                  %lu bytes each\n", 8);
        INFO ("Keys:                  %lu bytes each\n", 8);
        fprintf (stdout, "Values:                %lu bytes each\n", FLAGS_value_size);
        INFO ("Values:                %lu bytes each\n", (int)FLAGS_value_size);
        fprintf (stdout, "Entries:               %lu\n", (uint64_t)num_);
        INFO ("Entries:               %lu\n", (uint64_t)num_);
        fprintf (stdout, "Trace size:            %lu\n", (uint64_t)trace_size_);
        INFO ("Trace size:            %lu\n", (uint64_t)trace_size_);
        fprintf (stdout, "Read:                  %lu \n", (uint64_t)FLAGS_read);
        INFO ("Read:                  %lu \n", (uint64_t)FLAGS_read);
        fprintf (stdout, "Write:                 %lu \n", (uint64_t)FLAGS_write);
        INFO ("Write:                 %lu \n", (uint64_t)FLAGS_write);
        fprintf (stdout, "Thread:                %lu \n", (uint64_t)FLAGS_thread);
        INFO ("Thread:                %lu \n", (uint64_t)FLAGS_thread);
        fprintf (stdout, "Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
        INFO ("Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
        fprintf (stdout, "Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
        INFO ("Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
        fprintf (stdout, "benchmarks:            %s\n", FLAGS_benchmarks.c_str ());
        INFO ("benchmarks:            %s\n", FLAGS_benchmarks.c_str ());
        fprintf (stdout, "Thread:                %lu \n", (uint64_t)FLAGS_thread);
        fprintf (stdout, "------------------------------------------------\n");
        INFO ("------------------------------------------------\n");
        INFO ("Write mode             %s \n", "ASYNC");
        fprintf (stdout, "Write mode             %s \n", "ASYNC");
        INFO ("Queue depth:           %lu \n", (uint64_t)FLAGS_qdepth);
        fprintf (stdout, "Queue depth:           %lu \n", (uint64_t)FLAGS_qdepth);
        INFO ("Keyspace name:         %s \n", FLAGS_keyspace_name.c_str());
        fprintf (stdout, "Keyspace name:         %s \n", FLAGS_keyspace_name.c_str());
        INFO ("Device path(bdf):      %s \n", FLAGS_device_path.c_str());
        fprintf (stdout, "Device path(bdf):      %s \n", FLAGS_device_path.c_str());
        fprintf (stdout, "------------------------------------------------\n");
        INFO ("------------------------------------------------\n");
    }
};

int main (int argc, char* argv[]) {
    // for (int i = 0; i < argc; i++) {
    //     printf ("%s ", argv[i]);
    // }
    // printf ("\n");
    ParseCommandLineFlags (&argc, &argv, true);
    Benchmark benchmark;
    benchmark.Run ();
    return 0;
}
