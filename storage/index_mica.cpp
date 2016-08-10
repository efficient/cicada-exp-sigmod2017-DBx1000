#include "global.h"
#include "index_mica.h"
#include "mem_alloc.h"
#include "table.h"
#include "row.h"

#if INDEX_STRUCT == IDX_MICA

using ::mica::transaction::BTreeRangeType;

template <typename MICAIndexT>
RC IndexMICAGeneric<MICAIndexT>::init(uint64_t bucket_cnt, int part_cnt) {
  assert(false);
  (void)bucket_cnt;
  (void)part_cnt;
  return ERROR;
}

template <>
RC IndexMICAGeneric<MICAIndex>::init(int part_cnt, table_t* table,
                                     uint64_t bucket_cnt) {
  this->table = table;
  this->bucket_cnt = bucket_cnt;
  item_cnt = 0;
  overload_warning = false;

  auto mica_tbl = table->mica_tbl;
  auto db = table->mica_db;
  auto thread_id = ::mica::util::lcore.lcore_id();

  // printf("idx=%p part_cnt=%d bucket_cnt=%" PRIu64 "\n", this, part_cnt,
  // bucket_cnt);

  for (int part_id = 0; part_id < part_cnt; part_id++) {
    char buf[1024];
    int i = 0;
    while (true) {
      sprintf(buf, "%d", i);
      if (mica_tbl->db()->create_hash_index_nonunique_u64(buf, mica_tbl,
                                                          bucket_cnt))
        break;
      i++;
    }
    auto p = mica_tbl->db()->get_hash_index_nonunique_u64(buf);
    assert(p != nullptr);

    MICATransaction tx(db->context(thread_id));
    if (!p->init(&tx)) {
      assert(false);
      return ERROR;
    }

    printf("idx_name=%s part_cnt=%d bucket_cnt=%" PRIu64 "\n", buf, part_cnt,
           bucket_cnt);

    mica_idx.push_back(p);
  }

  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAOrderedIndex>::init(int part_cnt, table_t* table,
                                            uint64_t bucket_cnt) {
  this->table = table;
  this->bucket_cnt = bucket_cnt;
  item_cnt = 0;
  overload_warning = false;

  auto mica_tbl = table->mica_tbl;
  auto db = table->mica_db;
  auto thread_id = ::mica::util::lcore.lcore_id();

  // printf("idx=%p part_cnt=%d bucket_cnt=%" PRIu64 "\n", this, part_cnt,
  // bucket_cnt);

  for (int part_id = 0; part_id < part_cnt; part_id++) {
    char buf[1024];
    int i = 0;
    while (true) {
      sprintf(buf, "%d", i);
      if (mica_tbl->db()->create_btree_index_nonunique_u64(buf, mica_tbl))
        break;
      i++;
    }
    auto p = mica_tbl->db()->get_btree_index_nonunique_u64(buf);
    assert(p != nullptr);

    MICATransaction tx(db->context(thread_id));
    if (!p->init(&tx)) {
      assert(false);
      return ERROR;
    }

    printf("idx_name=%s part_cnt=%d bucket_cnt=%" PRIu64 "\n", buf, part_cnt,
           bucket_cnt);

    mica_idx.push_back(p);
  }

  return RCOK;
}

template <typename MICAIndexT>
bool IndexMICAGeneric<MICAIndexT>::index_exist(idx_key_t key) {
  assert(false);
}

template <>
RC IndexMICAGeneric<MICAIndex>::index_insert(idx_key_t key, itemid_t* item,
                                             int part_id) {
  __sync_fetch_and_add(&item_cnt, 1);
  if (item_cnt > bucket_cnt) {
    if (!overload_warning) {
      overload_warning = true;
      printf("bucket_cnt=%lu item_cnt=%lu\n", bucket_cnt, item_cnt);
    }
  }

  auto row_id = ((row_t*)item->location)->get_row_id();
  // assert(row_id < table->mica_tbl->row_count());

  auto db = table->mica_db;

  auto thread_id = ::mica::util::lcore.lcore_id();
  // printf("thread_id = %lu\n", thread_id);

  MICATransaction tx(db->context(thread_id));
  while (true) {
    // printf("idx=%p part_id=%d key=%lu row_id=%lu\n", this, part_id, key,
    // row_id);
    if (!tx.begin()) assert(false);
    if (!mica_idx[part_id]->insert(&tx, key, row_id)) {
      if (!tx.abort()) assert(false);
      continue;
    }
    if (!tx.commit()) continue;

    break;
  }
  // printf("index updated\n");

  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAIndex>::index_read(idx_key_t key, itemid_t*& item,
                                           int part_id, int thd_id) {
  (void)thd_id;

  bool skip_validation = !(MICA_FULLINDEX);

  auto tx = item->mica_tx;
  uint64_t row_id;
  auto ret = mica_idx[part_id]->lookup(tx, key, skip_validation,
                                       [&row_id](auto& key, auto value) {
                                         row_id = value;
                                         return false;
                                       });
  if (ret == 0) return ERROR;
  if (ret == MICAIndex::kHaveToAbort) return Abort;
  // printf("%lu %lu\n", key, row_id);
  item->location = reinterpret_cast<void*>(row_id);
  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAIndex>::index_read_multiple(MICATransaction* tx,
                                                    idx_key_t key,
                                                    uint64_t* row_ids,
                                                    uint64_t& count,
                                                    int part_id, int thd_id) {
  (void)thd_id;

  if (count == 0) return RCOK;

  bool skip_validation = !(MICA_FULLINDEX);

  uint64_t i = 0;
  uint64_t ret = mica_idx[part_id]->lookup(
      tx, key, skip_validation, [&i, row_ids, count](auto& k, auto v) {
        row_ids[i++] = v;
        // printf("%" PRIu64 "\n", i);
        return i < count;
      });
  if (ret == MICAIndex::kHaveToAbort) return Abort;
  count = i;
  // printf("%lu %lu\n", key, row_id);
  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAOrderedIndex>::index_insert(idx_key_t key,
                                                    itemid_t* item,
                                                    int part_id) {
  __sync_fetch_and_add(&item_cnt, 1);
  if (item_cnt > bucket_cnt) {
    if (!overload_warning) {
      overload_warning = true;
      printf("bucket_cnt=%lu item_cnt=%lu\n", bucket_cnt, item_cnt);
    }
  }

  auto row_id = ((row_t*)item->location)->get_row_id();
  // assert(row_id < table->mica_tbl->row_count());

  auto db = table->mica_db;

  auto thread_id = ::mica::util::lcore.lcore_id();
  // printf("thread_id = %lu\n", thread_id);

  MICATransaction tx(db->context(thread_id));
  while (true) {
    // printf("idx=%p part_id=%d key=%lu row_id=%lu\n", this, part_id, key,
    // row_id);
    if (!tx.begin()) assert(false);
    if (!mica_idx[part_id]->insert(&tx, make_pair(key, row_id), 0)) {
      if (!tx.abort()) assert(false);
      continue;
    }
    if (!tx.commit()) continue;

    break;
  }
  // printf("index updated\n");

  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAOrderedIndex>::index_read(idx_key_t key,
                                                  itemid_t*& item, int part_id,
                                                  int thd_id) {
  (void)thd_id;

  bool skip_validation = !(MICA_FULLINDEX);

  auto tx = item->mica_tx;
  uint64_t row_id = (uint64_t)-1;
  auto ret =
      mica_idx[part_id]
          ->lookup<BTreeRangeType::kInclusive, BTreeRangeType::kOpen, false>(
              tx, std::make_pair(key, 0), std::make_pair(key, 0),
              skip_validation, [&key, &row_id](auto& k, auto v) {
                if (k.first == key) row_id = k.second;
                return false;
              });
  if (row_id == (uint64_t)-1) return ERROR;
  if (ret == MICAOrderedIndex::kHaveToAbort) return Abort;
  // printf("%lu %lu\n", key, row_id);
  item->location = reinterpret_cast<void*>(row_id);
  return RCOK;
}

template <>
RC IndexMICAGeneric<MICAOrderedIndex>::index_read_multiple(
    MICATransaction* tx, idx_key_t key, uint64_t* row_ids, uint64_t& count,
    int part_id, int thd_id) {
  (void)thd_id;

  if (count == 0) return RCOK;

  bool skip_validation = !(MICA_FULLINDEX);

  uint64_t i = 0;
  uint64_t ret =
      mica_idx[part_id]
          ->lookup<BTreeRangeType::kInclusive, BTreeRangeType::kOpen, false>(
              tx, std::make_pair(key, 0), std::make_pair(key, 0),
              skip_validation, [&key, &i, row_ids, count](auto& k, auto v) {
                if (k.first == key) {
                  row_ids[i++] = k.second;
                  return i < count;
                } else
                  return false;
              });
  if (ret == MICAOrderedIndex::kHaveToAbort) return Abort;
  count = i;
  // printf("%lu %lu\n", key, row_id);
  return RCOK;
}

template class IndexMICAGeneric<MICAIndex>;

template class IndexMICAGeneric<MICAOrderedIndex>;

#endif
