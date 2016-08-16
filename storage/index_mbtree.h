#pragma once

// #include "global.h"
// #include "helper.h"
#include "index_base.h"

class table_t;
class itemid_t;

class IndexMBTree : public index_base {
 public:
  RC init(uint64_t part_cnt, table_t* table);
  RC init(uint64_t part_cnt, table_t* table, uint64_t bucket_cnt);

  bool index_exist(idx_key_t key);  // check if the key exist.
  RC index_insert(idx_key_t key, itemid_t* item, int part_id = -1);

  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1) {
    return index_read(key, item, part_id, 0);
  }
  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1,
                int thd_id = 0);
  // RC index_read_multiple(idx_key_t key, uint64_t* row_ids, size_t& count,
  //                        int part_id = -1, int thd_id = 0);
  RC index_read_range(idx_key_t min_key, idx_key_t max_key, itemid_t** items,
                      size_t& count, int part_id = -1, int thd_id = 0);
  RC index_read_range_rev(idx_key_t min_key, idx_key_t max_key,
                          itemid_t** items, size_t& count, int part_id = -1,
                          int thd_id = 0);

  RC index_remove(idx_key_t key, itemid_t** out_item, int part_id = -1);

  table_t* table;
  std::vector<void*> btree_idx;
};
