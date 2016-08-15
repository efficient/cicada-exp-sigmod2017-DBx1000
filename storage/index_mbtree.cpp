#define CONFIG_H "silo/config/config-perf.h"

#define NDB_MASSTREE 1
#include "silo/masstree/config.h"
#include "silo/masstree_btree.h"

#ifdef NDEBUG
#undef NDEBUG
#endif

#include "index_mbtree.h"
#include "helper.h"
#include "storage/row.h"

// #ifdef DEBUG
// #undef NDEBUG
// #endif

struct mbtree_params : public Masstree::nodeparams<> {
  typedef itemid_t* value_type;
  typedef Masstree::value_print<value_type> value_print_type;
  typedef simple_threadinfo threadinfo_type;
  enum { RcuRespCaller = true };
};

typedef mbtree<mbtree_params> concurrent_mbtree;

RC IndexMBTree::init(uint64_t part_cnt, table_t* table) {
  return init(part_cnt, table, 0);
}

RC IndexMBTree::init(uint64_t part_cnt, table_t* table, uint64_t bucket_cnt) {
  (void)bucket_cnt;

  this->table = table;

  for (uint64_t part_id = 0; part_id < part_cnt; part_id++)
    btree_idx.push_back(new concurrent_mbtree());

  return RCOK;
}

bool IndexMBTree::index_exist(idx_key_t key) {
  assert(false);
  return false;
}

RC IndexMBTree::index_insert(idx_key_t key, itemid_t* item, int part_id) {
#if CC_ALG == MICA
	item->location = reinterpret_cast<void*>(((row_t*)item->location)->get_row_id());
#endif

  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->insert_if_absent(mbtree_key, item)) return ERROR;

  return RCOK;
}

RC IndexMBTree::index_read(idx_key_t key, itemid_t*& item, int part_id,
                           int thd_id) {
  (void)thd_id;

  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->search(mbtree_key, item)) return ERROR;

  return RCOK;
}

// RC IndexMBTree::index_read_multiple(idx_key_t key, uint64_t* row_ids, uint64_t& count, int part_id, int thd_id) {
//   // Duplicate keys are currently not supported.
//   assert(false);
//   (void)key;
//   (void)row_ids;
//   (void)count;
//   (void)part_id;
//   (void)thd_id;
//   return ERROR;
// }

RC IndexMBTree::index_read_range(idx_key_t min_key, idx_key_t max_key,
                                 itemid_t** items, uint64_t& count, int part_id,
                                 int thd_id) {
  (void)thd_id;

  if (count == 0) return RCOK;

  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key_min(min_key);

  // mbtree's range is right-open.
  max_key++;
  assert(max_key != 0);
  u64_varkey mbtree_key_max(max_key);

  uint64_t i = 0;
  auto f = [&i, items, count](auto& k, auto& v) {
    items[i++] = v;
    return i < count;
  };

  idx->search_range(mbtree_key_min, &mbtree_key_max, f);

  count = i;

  return RCOK;
}

RC IndexMBTree::index_remove(idx_key_t key, itemid_t** out_item, int part_id) {
  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->remove(mbtree_key, out_item)) return ERROR;

  return RCOK;
}
