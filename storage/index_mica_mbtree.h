#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

#if INDEX_STRUCT == IDX_MICA

class row_t;
class table_t;
class itemid_t;

class IndexMICAMBTree : public index_base {
 public:
  RC init(uint64_t part_cnt, table_t* table);
  RC init(uint64_t part_cnt, table_t* table, uint64_t bucket_cnt);

  RC index_insert(MICATransaction* tx, idx_key_t key, row_t* row, int part_id);
  // This method ignores the second row_t* argument.
  RC index_remove(MICATransaction* tx, idx_key_t key, row_t*, int part_id);

  RC index_read(MICATransaction* tx, idx_key_t key, row_t** row, int part_id);
  RC index_read_multiple(MICATransaction* tx, idx_key_t key, row_t** rows,
                         size_t& count, int part_id);

  RC index_read_range(MICATransaction* tx, idx_key_t min_key, idx_key_t max_key,
                      row_t** rows, size_t& count, int part_id);
  RC index_read_range_rev(MICATransaction* tx, idx_key_t min_key,
                          idx_key_t max_key, row_t** rows, size_t& count,
                          int part_id);

  table_t* table;
  std::vector<void*> btree_idx;
};

#endif
