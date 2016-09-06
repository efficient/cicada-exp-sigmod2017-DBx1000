#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog* schema, uint64_t part_cnt) {
  this->table_name = schema->table_name;
  this->schema = schema;
  this->part_cnt = part_cnt;

#if CC_ALG == MICA

  for (uint64_t part_id = 0; part_id < part_cnt; part_id++) {
    uint64_t thread_id = part_id % g_thread_cnt;
    ::mica::util::lcore.pin_thread(thread_id);

    const uint64_t zero_data_sizes[] = {0, 0, 0, 0};
    const uint64_t* data_sizes;
    if (WORKLOAD != TPCC || strcmp(table_name, "ORDER-LINE") != 0) {
      data_sizes = schema->cf_sizes;
    } else {
      // Disable inlining for ORDER-LINE.
      data_sizes = zero_data_sizes;
    }

    char buf[1024];
    int i = 0;
    while (true) {
      sprintf(buf, "%s_%d", table_name, i);
      if (mica_db->create_table(buf, schema->cf_count, data_sizes)) break;
      i++;
    }
    auto p = mica_db->get_table(buf);
    assert(p);

    printf("tbl_name=%s part_id=%" PRIu64 " part_cnt=%" PRIu64 "\n", buf,
           part_id, part_cnt);
	printf("\n");

    mica_tbl.push_back(p);
  }
#endif
}

RC table_t::get_new_row(row_t*& row) {
  // this function is obsolete.
  assert(false);
  return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t*& row, uint64_t part_id, uint64_t& row_id) {
  RC rc = RCOK;

  // XXX: this has a race condition; should be used just for non-critical purposes
  // cur_tab_size++;

#if CC_ALG == MICA
  assert(row != NULL);
// We do not need a new row instance because MICA has it.
#else
  row = (row_t*)mem_allocator.alloc(row_t::alloc_size(this), part_id);
#endif
  rc = row->init(this, part_id, row_id);
  row->init_manager(row);

  return rc;
}
