#include "global.h"
#include "index_cuckoo.h"
#include "mem_alloc.h"
#include "table.h"
#if CC_ALG == MICA
#include "row.h"
#endif

RC IndexCuckoo::init(uint64_t bucket_cnt, int part_cnt) {
  maps_ = new MapType*[part_cnt];
  for (int i = 0; i < part_cnt; i++) {
    maps_[i] = new MapType(bucket_cnt);
  }
  return RCOK;
}

RC IndexCuckoo::init(int part_cnt, table_t* table, uint64_t bucket_cnt) {
  init(bucket_cnt, part_cnt);
  this->table = table;
  return RCOK;
}

bool IndexCuckoo::index_exist(idx_key_t key) { assert(false); }

RC IndexCuckoo::index_insert(idx_key_t key, itemid_t* item, int part_id) {
#if CC_ALG == MICA
  item->table = ((row_t*)item->location)->table;
  item->row_id = ((row_t*)item->location)->get_row_id();
#endif

  return maps_[part_id]->insert(key, item) ? RCOK : ERROR;
}

RC IndexCuckoo::index_read(idx_key_t key, itemid_t*& item, int part_id) {
  try {
    maps_[part_id]->find(key, item);
    return RCOK;
  } catch (...) {
    return ERROR;
  }
}

RC IndexCuckoo::index_read(idx_key_t key, itemid_t*& item, int part_id,
                           int thd_id) {
  try {
    maps_[part_id]->find(key, item);
    return RCOK;
  } catch (...) {
    return ERROR;
  }
}
