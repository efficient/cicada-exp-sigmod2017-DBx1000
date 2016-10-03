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
#include "catalog.h"

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

    auto t = (concurrent_mica_mbtree*)mem_allocator.alloc(
        sizeof(concurrent_mica_mbtree), part_id);
    new (t) concurrent_mica_mbtree;

    btree_idx.push_back(t);
  }

#if TPCC_VALIDATE_GAP
  validate_gap = false;
#endif

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

#if TPCC_VALIDATE_GAP
// TODO: Implement.
#endif

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

  // MICARowAccessHandlePeekOnly rah(tx);
  // MICARowAccessHandle rah(tx);
  auto mica_tbl = table->mica_tbl[part_id];

  uint64_t i = 0;
  auto f = [&i, rows, count, /*&rah,*/ mica_tbl](auto& k, auto& v) {
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, false, false, false)) return true;
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, true, false, false)) return true;
    // rah.reset();

    rows[i++] = v.row;
    return i < count;
  };

  idx->search_range(mbtree_key_min, &mbtree_key_max, f);

  count = i;

#if TPCC_VALIDATE_GAP
  if (!tx->is_peek_only() && validate_gap) {
    uint64_t next;
    if (count == 0)
      next = pos_infty;
    else {
      MICARowAccessHandlePeekOnly rah(tx);
      if (!rah.peek_row(mica_tbl, 1, (uint64_t)rows[count - 1], true, false,
                        false))
        return Abort;
      next = *(uint64_t*)(rah.cdata() + gap_off + 8);
    }
    MICARowAccessHandle rah(tx);
    if (!rah.peek_row(mica_tbl, 1, next, true, true, false) || !rah.read_row())
      return Abort;
  }
#endif

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

  // MICARowAccessHandlePeekOnly rah(tx);
  // MICARowAccessHandle rah(tx);
  auto mica_tbl = table->mica_tbl[part_id];

  uint64_t i = 0;
  auto f = [&i, rows, count, /*&rah,*/ mica_tbl](auto& k, auto& v) {
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, false, false, false)) return true;
    // if (!rah.peek_row(mica_tbl, 0, (uint64_t)v.row, true, false, false)) return true;
    // rah.reset();

    rows[i++] = v.row;
    return i < count;
  };

  idx->rsearch_range(mbtree_key_max, &mbtree_key_min, f);

  count = i;

#if TPCC_VALIDATE_GAP
  if (!tx->is_peek_only() && validate_gap) {
    uint64_t next;
    if (count == 0)
      next = neg_infty;
    else {
      MICARowAccessHandlePeekOnly rah(tx);
      if (!rah.peek_row(mica_tbl, 1, (uint64_t)rows[count - 1], true, false,
                        false))
        return Abort;
      next = *(uint64_t*)(rah.cdata() + gap_off + 0);
    }
    MICARowAccessHandle rah(tx);
    if (!rah.peek_row(mica_tbl, 1, next, true, true, false) || !rah.read_row())
      return Abort;
  }
#endif

  return RCOK;
}

RC IndexMICAMBTree::index_remove(MICATransaction* tx, idx_key_t key, row_t*,
                                 int part_id) {
  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->remove_refcount(mbtree_key, NULL)) return ERROR;

  return RCOK;
}

#if TPCC_VALIDATE_GAP
RC IndexMICAMBTree::list_init(MICADB* mica_db, uint64_t gap_off) {
  // printf("gap_off=%" PRIu64 "\n", gap_off);

  validate_gap = true;
  this->gap_off = gap_off;

  auto thread_id = ::mica::util::lcore.lcore_id();

  MICATransaction tx(mica_db->context(thread_id));

  for (uint64_t part_id = 0; part_id < btree_idx.size(); part_id++) {
    auto mica_tbl = table->mica_tbl[part_id];
    auto tuple_size = table->get_schema()->get_tuple_size();

    // Create -infty, +infty.
    MICARowAccessHandle rah0(&tx);
    MICARowAccessHandle rah1(&tx);

    if (!tx.begin()) assert(false);

    if (!rah0.new_row(mica_tbl, 0, MICATransaction::kNewRowID, false,
                      tuple_size))
      assert(false);
    if (!rah1.new_row(mica_tbl, 0, MICATransaction::kNewRowID, false,
                      tuple_size))
      assert(false);

    if (part_id == 0) {
      neg_infty = rah0.row_id();
      pos_infty = rah1.row_id();
    } else {
      assert(neg_infty == rah0.row_id());
      assert(pos_infty == rah1.row_id());
    }

    rah0.reset();
    rah1.reset();
    if (!rah0.new_row(mica_tbl, 1, neg_infty, false, 16)) assert(false);
    if (!rah1.new_row(mica_tbl, 1, pos_infty, false, 16)) assert(false);

    *(uint64_t*)(rah0.data() + gap_off + 0) = (uint64_t)-1;

    *(uint64_t*)(rah1.data() + gap_off + 8) = (uint64_t)-1;

    if (!tx.commit()) assert(false);
  }

  return RCOK;
}

RC IndexMICAMBTree::list_make(MICADB* mica_db) {
  assert(validate_gap);

  auto thread_id = ::mica::util::lcore.lcore_id();

  MICATransaction tx(mica_db->context(thread_id));

  for (uint64_t part_id = 0; part_id < btree_idx.size(); part_id++) {
    auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);
    auto mica_tbl = table->mica_tbl[part_id];

    u64_varkey mbtree_key_min(0);

    uint64_t prev_row = neg_infty;

    // Create the main chain.
    auto f = [&tx, &mica_tbl, &prev_row, this](auto& k, auto& v) {
      MICARowAccessHandle rah_left(&tx);
      MICARowAccessHandle rah(&tx);
      uint64_t row_id = (uint64_t)v.row;

      if (!tx.begin()) assert(false);

      if (!rah_left.peek_row(mica_tbl, 1, prev_row, false, true, true) ||
          !rah_left.read_row() || !rah_left.write_row())
        assert(false);
      if (!rah.new_row(mica_tbl, 1, row_id, false, 16)) assert(false);

      *(uint64_t*)(rah_left.data() + gap_off + 8) = row_id;
      *(uint64_t*)(rah.data() + gap_off + 0) = prev_row;
      prev_row = row_id;

      if (!tx.commit()) assert(false);
      return true;
    };

    idx->search_range(mbtree_key_min, NULL, f);

    {
      MICARowAccessHandle rah_left(&tx);
      MICARowAccessHandle rah(&tx);

      if (!tx.begin()) assert(false);

      if (!rah_left.peek_row(mica_tbl, 1, prev_row, false, true, true) ||
          !rah_left.read_row() || !rah_left.write_row())
        assert(false);
      if (!rah.peek_row(mica_tbl, 1, pos_infty, false, true, true) ||
          !rah.read_row() || !rah.write_row())
        assert(false);

      *(uint64_t*)(rah_left.data() + gap_off + 8) = pos_infty;
      *(uint64_t*)(rah.data() + gap_off + 0) = prev_row;
      *(uint64_t*)(rah.data() + gap_off + 8) = (uint64_t)-1;

      if (!tx.commit()) assert(false);
    }
  }

  return RCOK;
}

RC IndexMICAMBTree::list_insert(MICATransaction* tx, idx_key_t key, row_t* row,
                                int part_id) {
  if (!validate_gap) return RCOK;

  auto idx = reinterpret_cast<concurrent_mica_mbtree*>(btree_idx[part_id]);
  auto mica_tbl = table->mica_tbl[part_id];
  uint64_t row_id = row->get_row_id();

  // Make a node.
  MICARowAccessHandle rah(tx);
  if (!rah.new_row(mica_tbl, 1, row_id, true, 16)) assert(false);

  // Find the right node.
  // TODO: Do not use the index because the same transaction may insert multiple
  // entries to the index (e.g., new_order_createOrderLine).
  uint64_t right = pos_infty;
  {
    u64_varkey mbtree_key_min(key);
    auto f = [&right](auto& k, auto& v) {
      right = (uint64_t)v.row;
      return false;
    };
    idx->search_range(mbtree_key_min, NULL, f);

    // if (right == pos_infty) printf("warning: the right node is pos_infty\n");
    // printf("right=%" PRIu64 "\n", right);
  }

  // Read the right node and find the left node.
  MICARowAccessHandle rah_right(tx);
  uint64_t left;
  {
    if (!rah_right.peek_row(mica_tbl, 1, right, true, true, true) ||
        !rah_right.read_row() || !rah_right.write_row()) {
      // printf("i right\n");
      return Abort;
    }
    left = *(uint64_t*)(rah_right.data() + gap_off + 0);
    // printf("left=%" PRIu64 "\n", left);
  }

  // Read the left node.
  MICARowAccessHandle rah_left(tx);
  if (!rah_left.peek_row(mica_tbl, 1, left, true, true, true) ||
      !rah_left.read_row() || !rah_left.write_row()) {
    // printf("i left\n");
    return Abort;
  }

  // Insert the new node.
  *(uint64_t*)(rah_left.data() + gap_off + 8) = row_id;
  *(uint64_t*)(rah.data() + gap_off + 0) = left;
  *(uint64_t*)(rah.data() + gap_off + 8) = right;
  *(uint64_t*)(rah_right.data() + gap_off + 0) = row_id;
  return RCOK;
}

RC IndexMICAMBTree::list_remove(MICATransaction* tx, idx_key_t key, row_t* row,
                                int part_id) {
  if (!validate_gap) return RCOK;

  auto mica_tbl = table->mica_tbl[part_id];
  uint64_t row_id = row->get_row_id();

  MICARowAccessHandle rah(tx);
  MICARowAccessHandle rah_left(tx);
  MICARowAccessHandle rah_right(tx);

  // Find left and right nodes.
  if (!rah.peek_row(mica_tbl, 1, row_id, true, true, true) || !rah.read_row()) {
    // printf("r node\n");
    return Abort;
  }
  auto left = *(uint64_t*)(rah.cdata() + gap_off + 0);
  auto right = *(uint64_t*)(rah.cdata() + gap_off + 8);

  // Read the left node.
  if (!rah_left.peek_row(mica_tbl, 1, left, true, true, true) ||
      !rah_left.read_row() || !rah_left.write_row()) {
    // printf("r left\n");
    return Abort;
  }
  // Read the right node.
  if (!rah_right.peek_row(mica_tbl, 1, right, true, true, true) ||
      !rah_right.read_row() || !rah_right.write_row()) {
    // printf("r right\n");
    return Abort;
  }

  // Remove the node from the chain.
  *(uint64_t*)(rah_left.data() + gap_off + 8) = right;
  *(uint64_t*)(rah_right.data() + gap_off + 0) = left;

  // Delete the node
  if (!rah.write_row(0) || !rah.delete_row()) {
    // printf("r node\n");
    return Abort;
  }

  return RCOK;
}
#endif

#endif
