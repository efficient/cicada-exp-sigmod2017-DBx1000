#define CONFIG_H "silo/config/config-perf.h"

#define NDB_MASSTREE 1
#include "silo/masstree/config.h"
#include "silo/masstree_btree.h"

#ifdef NDEBUG
#undef NDEBUG
#endif

#include "global.h"
#include "index_mica_mbtree.h"
#include "mem_alloc.h"
#include "table.h"
#include "row.h"
#include "helper.h"

#if INDEX_STRUCT == IDX_MICA

struct mica_mbtree_value_type {
  row_t* row;
  int refcount;
};

struct mica_mbtree_params : public Masstree::nodeparams<> {
  typedef mica_mbtree_value_type value_type;
  typedef Masstree::value_print<value_type> value_print_type;
  typedef simple_threadinfo threadinfo_type;
  enum { RcuRespCaller = true };
};

typedef mbtree<mica_mbtree_params> concurrent_mica_mbtree;

RC IndexMICAMBTree::init(uint64_t part_cnt, table_t* table) {
  return init(part_cnt, table, 0);
}

RC IndexMICAMBTree::init(uint64_t part_cnt, table_t* table,
                         uint64_t bucket_cnt) {
  (void)bucket_cnt;

  this->table = table;

  for (uint64_t part_id = 0; part_id < part_cnt; part_id++) {
    mem_allocator.register_thread(part_id % g_thread_cnt);

    auto t = (concurrent_mica_mbtree*)mem_allocator.alloc(sizeof(concurrent_mica_mbtree),
                                                     part_id);
    new (t) concurrent_mica_mbtree;

    btree_idx.push_back(t);
  }

  return RCOK;
}

RC IndexMICAMBTree::index_insert(MICATransaction* tx, idx_key_t key, row_t* row,
                                 int part_id) {
  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  mica_mbtree_value_type v{row, 1};
  if (!idx->insert_refcount(mbtree_key, v)) return ERROR;

  return RCOK;
}

RC IndexMICAMBTree::index_read(MICATransaction* tx, idx_key_t key, row_t** row,
                               int part_id) {
  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  mica_mbtree_value_type v;
  if (!idx->search(mbtree_key, v)) return ERROR;
  *row = v.row;

  return RCOK;
}

RC IndexMICAMBTree::index_read_multiple(MICATransaction* tx, idx_key_t key,
                                        row_t** rows, uint64_t& count,
                                        int part_id) {
  // Duplicate keys are currently not supported in IndexMBTree.
  assert(false);
  (void)key;
  (void)rows;
  (void)count;
  (void)part_id;
  return ERROR;
}

RC IndexMICAMBTree::index_read_range(MICATransaction* tx, idx_key_t min_key,
                                     idx_key_t max_key, row_t** rows,
                                     uint64_t& count, int part_id) {
  if (count == 0) return RCOK;

  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key_min(min_key);

  // mbtree's range is right-open.
  max_key++;
  assert(max_key != 0);
  u64_varkey mbtree_key_max(max_key);

  MICARowAccessHandlePeekOnly rah(tx);
  // MICARowAccessHandle rah(tx);
  auto mica_tbl = table->mica_tbl[part_id];

  uint64_t i = 0;
  auto f = [&i, rows, count, &rah, mica_tbl](auto& k, auto& v) {
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, false, false, false)) return true;
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, true, false, false)) return true;
    rah.reset();

    rows[i++] = v.row;
    return i < count;
  };

  idx->search_range(mbtree_key_min, &mbtree_key_max, f);

  count = i;

  return RCOK;
}

RC IndexMICAMBTree::index_read_range_rev(MICATransaction* tx, idx_key_t min_key,
                                         idx_key_t max_key, row_t** rows,
                                         uint64_t& count, int part_id) {
  if (count == 0) return RCOK;

  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  // mbtree's range is left-open.
  assert(min_key != 0);
  min_key--;
  u64_varkey mbtree_key_min(min_key);

  u64_varkey mbtree_key_max(max_key);

  MICARowAccessHandlePeekOnly rah(tx);
  // MICARowAccessHandle rah(tx);
  auto mica_tbl = table->mica_tbl[part_id];

  uint64_t i = 0;
  auto f = [&i, rows, count, &rah, mica_tbl](auto& k, auto& v) {
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, false, false, false)) return true;
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, true, false, false)) return true;
    rah.reset();

    rows[i++] = v.row;
    return i < count;
  };

  idx->rsearch_range(mbtree_key_max, &mbtree_key_min, f);

  count = i;

  return RCOK;
}

RC IndexMICAMBTree::index_remove(MICATransaction* tx, idx_key_t key, row_t*,
                                 int part_id) {
  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->remove_refcount(mbtree_key, NULL)) return ERROR;

  return RCOK;
}

#endif
