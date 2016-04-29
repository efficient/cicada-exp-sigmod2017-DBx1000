#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
  	row_cnt = 0;
#if INDEX_STRUCT == IDX_MICA
	RC idx_rc;
	itemid_t idx_item;
	m_item = &idx_item;
#endif

	const uint64_t kColumnSize = MAX_TUPLE_SIZE / 10;

	uint64_t v = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		uint64_t column = req->column;
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) {
			if (iteration == 0) {
#if INDEX_STRUCT != IDX_MICA
				m_item = index_read(_wl->the_index, req->key, part_id);
#else
				idx_rc = index_read(_wl->the_index, req->key, m_item, part_id);
				assert(idx_rc == RCOK);
#endif
			}
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
			row_t * row_local;
			access_t type = req->rtype;

#if CC_ALG != MICA
			row_local = get_row(row, type);
#else
			(void)row;
			row_local = get_row(_wl->the_index, m_item, type);
#endif
			if (row_local == NULL) {
				rc = Abort;
				goto final;
			}

			// Computation //
			// Only do computation when there are more than 1 requests.
            /*if (m_query->request_cnt > 1)*/ {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
					          const char* data = row_local->get_data() + column * kColumnSize;
					          for (uint64_t j = 0; j < kColumnSize; j++)
					            v += static_cast<uint64_t>(data[j]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						//int fid = 0;
						// char * data = row->get_data();
	          char* data = row_local->get_data() + column * kColumnSize;
	          for (uint64_t j = 0; j < kColumnSize; j++) {
	            v += static_cast<uint64_t>(data[j]);
	            data[j] = static_cast<char>(v);
	          }
						//*(uint64_t *)(&data[fid * 10]) = 0;
						// memcpy(data, v, column_size);
//					}
                }
            }


			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}

