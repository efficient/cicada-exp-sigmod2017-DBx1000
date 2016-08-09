#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

#if INDEX_STRUCT == IDX_MICA

class IndexMICA : public index_base {
 public:
  RC init(uint64_t bucket_cnt, int part_cnt);
  RC init(int part_cnt, table_t* table, uint64_t bucket_cnt);

  bool index_exist(idx_key_t key);  // check if the key exist.
  RC index_insert(idx_key_t key, itemid_t* item, int part_id = -1);

  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1) {
    return index_read(key, item, part_id, 0);
  }
  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1,
                int thd_id = 0);
  RC index_read_multiple(MICATransaction* tx, idx_key_t key, uint64_t* row_ids,
                         size_t& count, int part_id = -1, int thd_id = 0);

  table_t* table;
  std::vector<MICAIndex*> mica_idx;

 private:
  uint64_t bucket_cnt;
  volatile uint64_t item_cnt;
  bool overload_warning;
};

#endif
