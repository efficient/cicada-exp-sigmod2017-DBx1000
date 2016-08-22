#pragma once

// #include "global.h"
// #include "helper.h"
#include "index_base.h"

class table_t;
class itemid_t;
class row_t;

class IndexMBTree : public index_base {
 public:
  RC init(uint64_t part_cnt, table_t* table);
  RC init(uint64_t part_cnt, table_t* table, uint64_t bucket_cnt);

  RC index_insert(idx_key_t key, row_t* row, int part_id);
  // This method ignores the second row_t* argument.
  RC index_remove(idx_key_t key, row_t*, int part_id);

  RC index_read(idx_key_t key, row_t** row, int part_id);
  RC index_read_multiple(idx_key_t key, row_t** rows, size_t& count,
                         int part_id);

  RC index_read_range(idx_key_t min_key, idx_key_t max_key, row_t** rows,
                      size_t& count, int part_id);
  RC index_read_range_rev(idx_key_t min_key, idx_key_t max_key, row_t** rows,
                          size_t& count, int part_id);

  table_t* table;
  std::vector<void*> btree_idx;
};
