#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"
#include <libcuckoo/cuckoohash_map.hh>

class IndexCuckoo : public index_base {
 public:
  RC init(uint64_t bucket_cnt, int part_cnt);
  RC init(int part_cnt, table_t* table, uint64_t bucket_cnt);
  bool index_exist(idx_key_t key);  // check if the key exist.
  RC index_insert(idx_key_t key, itemid_t* item, int part_id = -1);
  // the following call returns a single item
  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1);
  RC index_read(idx_key_t key, itemid_t*& item, int part_id = -1,
                int thd_id = 0);

 private:
  table_t* table;
  typedef cuckoohash_map<idx_key_t, itemid_t*> MapType;
  MapType** maps_;
};
