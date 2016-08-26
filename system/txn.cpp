#define CONFIG_H "silo/config/config-perf.h"
#include "silo/rcu.h"
#ifdef NDEBUG
#undef NDEBUG
#endif

#include "txn.h"
#include "row.h"
#include "wl.h"
#include "ycsb.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "index_mica.h"
#include "index_mbtree.h"

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	remove_cnt = 0;
	insert_idx_cnt = 0;
	remove_idx_cnt = 0;
	accesses = (Access **) mem_allocator.alloc(sizeof(Access *) * MAX_ROW_PER_TXN, thd_id);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
#if CC_ALG == TICTOC || CC_ALG == SILO
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else
		assert(false);
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_write_copy_ptr = (g_params["write_copy_form"] == "ptr");
	_atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO
	_cur_tid = 0;
#elif CC_ALG == MICA
	// printf("thd_id=%" PRIu64 "\n", thd_id);
	mica_tx = new MICATransaction(h_wl->mica_db->context(thd_id));
	readonly = false;
#endif

}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

void txn_man::set_readonly() {
#if CC_ALG == MICA
	readonly = true;
#endif
}

RC txn_man::apply_index_changes(RC rc) {
	if (rc != RCOK)
	return rc;

#if INDEX_STRUCT != IDX_MICA
	// XXX: This only provides snapshot isolation.  For serializability, the version of all leaf nodes used for search must be used for timestamp calculation and the version of leaf nodes updated by a commit must be bumped to the commit timestamp.

#if RCU_ALLOC
	assert(rcu::s_instance.in_rcu_region());
#endif

	for (size_t i = 0; i < insert_idx_cnt; i++) {
		auto idx = insert_idx_idx[i];
		auto key = insert_idx_key[i];
		auto row = insert_idx_row[i];
		auto part_id = insert_idx_part_id[i];

		assert(part_id != -1);

	  auto rc_insert = idx->index_insert(key, row, part_id);

		if (rc_insert != RCOK) {
			// Roll back previous inserts upon insert failure.

			while (i > 0) {
				i--;
				auto idx = insert_idx_idx[i];
				auto key = insert_idx_key[i];
				// auto row = insert_idx_row[i];
				auto part_id = insert_idx_part_id[i];

				auto rc_remove = idx->index_remove(key, NULL, part_id);
				assert(rc_remove == RCOK);

				// New rows that are not inserted will be freed in cleanup()
			}

			insert_idx_cnt = 0;
			return rc_insert;
		}
	}
	insert_idx_cnt = 0;

	for (size_t i = 0; i < remove_idx_cnt; i++) {
		auto idx = remove_idx_idx[i];
		auto key = remove_idx_key[i];
		auto part_id = remove_idx_part_id[i];
		// printf("remove_idx idx=%p key=%" PRIu64 " part_id=%d\n", idx, key, part_id);

		auto rc_remove = idx->index_remove(key, NULL, part_id);
		assert(rc_remove == RCOK);
	}
	remove_idx_cnt = 0;
#endif

#if CC_ALG != MICA
	// Free deleted rows
	for (size_t i = 0; i < remove_cnt; i++) {
		auto row = remove_rows[i];
		assert(!row->is_deleted);
		row->is_deleted = 1;
		// printf("remove_row row_id=%" PRIu64 " part_id=%" PRIu64 "\n", row->get_row_id(), row->get_part_id());
		// XXX: Freeing the row immediately is unsafe due to concurrent access.
		// We do this only when using RCU.
	  if (RCU_ALLOC) mem_allocator.free(row, row_t::alloc_size(row->get_table()));
		// XXX: We need to perform the following to free up all the resources
// #if CC_ALG != HSTORE && CC_ALG != OCC && CC_ALG != MICA && !defined(USE_INLINED_DATA)
// 			// XXX: Need to find the manager size.
// 			mem_allocator.free(row->manager, 0);
// #endif
// 			row->free_row();
	}
	remove_cnt = 0;
#endif

	return rc;
}

void txn_man::cleanup(RC rc) {
#if CC_ALG == HEKATON || CC_ALG == MICA
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	remove_cnt = 0;
	insert_idx_cnt = 0;
	remove_idx_cnt = 0;

	readonly = false;
	return;
#endif

	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT ||
					CC_ALG == NO_WAIT ||
					CC_ALG == WAIT_DIE))
		{
			orig_r->return_row(type, this, accesses[rid]->orig_data);
		} else {
			orig_r->return_row(type, this, accesses[rid]->data);
		}
#if CC_ALG != TICTOC && CC_ALG != SILO && CC_ALG != MICA
		accesses[rid]->data = NULL;
#endif
	}

	if (rc == Abort) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != OCC && CC_ALG != MICA && !defined(USE_INLINED_DATA)
			// XXX: Need to find the manager size.
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, row_t::alloc_size(row->get_table()));
		}
	}
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	remove_cnt = 0;
	insert_idx_cnt = 0;
	remove_idx_cnt = 0;
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
#if CC_ALG == MICA
	readonly = false;
#endif
}

// index_read methods
template <typename IndexT>
RC
txn_man::index_read(IndexT* index, idx_key_t key, row_t** row, int part_id) {
	return index->index_read(key, row, part_id);
}

#if INDEX_STRUCT == IDX_MICA
template <>
RC
txn_man::index_read(IndexMICA* index, idx_key_t key, row_t** row, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read(mica_tx, key, row, part_id);
}
template <>
RC
txn_man::index_read(OrderedIndexMICA* index, idx_key_t key, row_t** row, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read(mica_tx, key, row, part_id);
}
#endif

template <typename IndexT>
RC
txn_man::index_read_multiple(IndexT* index, idx_key_t key, row_t** rows, size_t& count, int part_id) {
	return index->index_read_multiple(key, rows, count, part_id);
}

#if INDEX_STRUCT == IDX_MICA
template <>
RC
txn_man::index_read_multiple(IndexMICA* index, idx_key_t key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_multiple(mica_tx, key, rows, count, part_id);
}
template <>
RC
txn_man::index_read_multiple(OrderedIndexMICA* index, idx_key_t key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_multiple(mica_tx, key, rows, count, part_id);
}
#endif

template <typename IndexT>
RC
txn_man::index_read_range(IndexT* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	return index->index_read_range(min_key, max_key, rows, count, part_id);
}

#if INDEX_STRUCT == IDX_MICA
template <>
RC
txn_man::index_read_range(IndexMICA* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_range(mica_tx, min_key, max_key, rows, count, part_id);
}
template <>
RC
txn_man::index_read_range(OrderedIndexMICA* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_range(mica_tx, min_key, max_key, rows, count, part_id);
}
#endif

template <typename IndexT>
RC
txn_man::index_read_range_rev(IndexT* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	return index->index_read_range_rev(min_key, max_key, rows, count, part_id);
}

#if INDEX_STRUCT == IDX_MICA
template <>
RC
txn_man::index_read_range_rev(IndexMICA* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_range_rev(mica_tx, min_key, max_key, rows, count, part_id);
}
template <>
RC
txn_man::index_read_range_rev(OrderedIndexMICA* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id) {
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin(readonly);
	return index->index_read_range_rev(mica_tx, min_key, max_key, rows, count, part_id);
}
#endif

// get_row methods
#if CC_ALG != MICA
template <typename IndexT>
row_t* txn_man::get_row(IndexT* index, row_t* row, int part_id, access_t type) {
	(void)index;
	(void)part_id;

	if (type == PEEK)
		return row;

	if (CC_ALG == HSTORE)
		return row;

	// uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) mem_allocator.alloc(sizeof(Access), -1);
		accesses[row_cnt] = access;
#if (CC_ALG == SILO || CC_ALG == TICTOC)
		access->data = (row_t *) mem_allocator.alloc(row_t::max_alloc_size(), -1);
		access->data->init(MAX_TUPLE_SIZE);
		access->orig_data = (row_t *) mem_allocator.alloc(row_t::max_alloc_size(), -1);
		access->orig_data->init(MAX_TUPLE_SIZE);
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		access->orig_data = (row_t *) mem_allocator.alloc(row_t::max_alloc_size(), -1);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		num_accesses_alloc ++;
	}

	// Initial deleted row detection to reduce creating a new local row.
	if (row->is_deleted)
		return NULL;

	rc = row->get_row(type, this, accesses[ row_cnt ]->data);

	if (rc == Abort) {
		return NULL;
	}

	// Check if the original row is deleted after getting the local row.
	// This avoids a race condition so that we can simply use the version check for Silo/TicToc to detect any deletion perfomed by another thread.
	if (row->is_deleted)
		return NULL;

	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
#if CC_ALG == TICTOC
	accesses[row_cnt]->wts = last_wts;
	accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO
	accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
	accesses[row_cnt]->history_entry = history_entry;
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
		accesses[row_cnt]->orig_data->table = row->get_table();
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif

	row_cnt ++;
	if (type == WR)
		wr_cnt ++;

	// uint64_t timespan = get_sys_clock() - starttime;
	// INC_TMP_STATS(get_thd_id(), time_man, timespan);
	return accesses[row_cnt - 1]->data;
}
#else	// CC_ALG == MICA
template <typename IndexT>
row_t *
txn_man::get_row(IndexT* index, row_t* row, int part_id, access_t type)
{
	// printf("1 row_id=%lu\n", item->row_id);
	if (row_cnt == 0 && !mica_tx->has_began())
		mica_tx->begin();

	// uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	assert(row_cnt < MAX_ROW_PER_TXN);
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) mem_allocator.alloc(sizeof(Access), -1);
		accesses[row_cnt] = access;
		access->data = (row_t *) mem_allocator.alloc(row_t::alloc_size(index->table), -1);
		num_accesses_alloc ++;
	}

	// printf("2 row_id=%lu\n", item->row_id);
	rc = row_t::get_row(type, this, index->table, accesses[ row_cnt ]->data, (uint64_t)row, part_id);
	// assert(rc == RCOK);

	if (rc == Abort) {
		return NULL;
	}
	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = nullptr;

	row_cnt ++;
	if (type == WR)
		wr_cnt ++;

	// uint64_t timespan = get_sys_clock() - starttime;
	// INC_TMP_STATS(get_thd_id(), time_man, timespan);
	return accesses[row_cnt - 1]->data;
}
#endif

// search
template <typename IndexT>
row_t* txn_man::search(IndexT* index, uint64_t key, int part_id,
                        access_t type) {
	row_t* row;
  auto ret = index_read(index, key, &row, part_id);
	if (ret != RCOK) return NULL;

  return get_row(index, row, part_id, type);
}

// insert_row/remove_row
bool txn_man::insert_row(table_t* tbl, row_t*& row, int part_id,
                          uint64_t& out_row_id) {
#if CC_ALG != MICA
  if (tbl->get_new_row(row, part_id, out_row_id) != RCOK) return false;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
  return true;
#else
  assert(row != NULL);
  assert(part_id >= 0 && part_id < (int)tbl->mica_tbl.size());
  MICARowAccessHandle rah(mica_tx);
  if (!rah.new_row(tbl->mica_tbl[part_id])) return false;
  out_row_id = rah.row_id();
  row->set_row_id(out_row_id);
  row->set_part_id(part_id);
  row->table = tbl;
  row->data = rah.data();
  return true;
#endif
}

bool txn_man::remove_row(row_t* row) {
#if CC_ALG != MICA
	remove_rows[remove_cnt++] = row;
  return true;
#else
  // MICA tables are directly managed.
	assert(false);
  return false;
#endif
}

// index_insert/index_remove
template <>
bool txn_man::insert_idx(IndexMBTree* index, uint64_t key, row_t* row,
                            int part_id) {
#if CC_ALG == MICA
  row = (row_t*)row->get_row_id();
#endif

	assert(insert_idx_cnt < MAX_ROW_PER_TXN);
	insert_idx_idx[insert_idx_cnt] = index;
	insert_idx_key[insert_idx_cnt] = key;
	insert_idx_row[insert_idx_cnt] = row;
	insert_idx_part_id[insert_idx_cnt] = part_id;
	insert_idx_cnt++;
  return true;
}
#if INDEX_STRUCT == IDX_MICA
template <>
bool txn_man::insert_idx(OrderedIndexMICA* index, uint64_t key, row_t* row,
                            int part_id) {
  row = (row_t*)row->get_row_id();

  auto& mica_idx = index->mica_idx;
  // if (mica_idx[part_id]->insert(mica_tx, make_pair(key, row->row_id), 0) != 1)
  return mica_idx[part_id]->insert(mica_tx, key, (uint64_t)row) == 1;
}
#endif

template <>
bool txn_man::remove_idx(IndexMBTree* index, uint64_t key, row_t* row,
                            int part_id) {
  (void)row;
	assert(remove_idx_cnt < MAX_ROW_PER_TXN);
	remove_idx_idx[remove_idx_cnt] = index;
	remove_idx_key[remove_idx_cnt] = key;
	remove_idx_part_id[remove_idx_cnt] = part_id;
	remove_idx_cnt++;
  return true;
}
#if INDEX_STRUCT == IDX_MICA
template <>
bool txn_man::remove_idx(OrderedIndexMICA* index, uint64_t key, row_t* row, int part_id) {
  auto& mica_idx = index->mica_idx;
  // return mica_idx[part_id]->remove(mica_tx, make_pair(key, row_id), 0) == 1;
  return mica_idx[part_id]->remove(mica_tx, key, (uint64_t)row) == 1;
}
#endif

// template instantiation
template
RC txn_man::index_read(INDEX* index, idx_key_t key, row_t** row, int part_id);
template
RC txn_man::index_read_multiple(INDEX* index, idx_key_t key, row_t** rows, size_t& count, int part_id);
template
RC txn_man::index_read_range(INDEX* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id);
template
RC txn_man::index_read_range_rev(INDEX* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id);
template
row_t* txn_man::get_row(INDEX* index, row_t* row, int part_id, access_t type);
template
row_t* txn_man::search(INDEX* index, size_t key, int part_id, access_t type);
// template
// bool txn_man::insert_idx(INDEX* idx, idx_key_t key, row_t* row, int part_id);
// template
// bool txn_man::remove_idx(INDEX* idx, idx_key_t key, row_t* row, int part_id);

template
RC txn_man::index_read(ORDERED_INDEX* index, idx_key_t key, row_t** row, int part_id);
template
RC txn_man::index_read_multiple(ORDERED_INDEX* index, idx_key_t key, row_t** rows, size_t& count, int part_id);
template
RC txn_man::index_read_range(ORDERED_INDEX* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id);
template
RC txn_man::index_read_range_rev(ORDERED_INDEX* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id);
template
row_t* txn_man::get_row(ORDERED_INDEX* index, row_t* row, int part_id, access_t type);
template
row_t* txn_man::search(ORDERED_INDEX* index, size_t key, int part_id, access_t type);
// template
// bool txn_man::insert_idx(ORDERED_INDEX* idx, idx_key_t key, row_t* row, int part_id);
// template
// bool txn_man::remove_idx(ORDERED_INDEX* idx, idx_key_t key, row_t* row, int part_id);



RC txn_man::finish(RC rc) {
#if CC_ALG == HSTORE
	rc = apply_index_changes(rc);
	return rc;
#endif
	// uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else
		cleanup(rc);
#elif CC_ALG == TICTOC
	if (rc == RCOK)
		rc = validate_tictoc();
	else
		cleanup(rc);
#elif CC_ALG == SILO
	if (rc == RCOK)
		rc = validate_silo();
	else
		cleanup(rc);
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#elif CC_ALG == MICA
	if (rc == RCOK) {
		if (mica_tx->has_began())
			rc = mica_tx->commit() ? RCOK : Abort;
		else
			rc = RCOK;
	}
	else
		if (mica_tx->has_began() && !mica_tx->abort())
			assert(false);
	cleanup(rc);
#else
	rc = apply_index_changes(rc);
	cleanup(rc);
#endif

	// uint64_t timespan = get_sys_clock() - starttime;
	// INC_TMP_STATS(get_thd_id(), time_man,  timespan);
	// INC_STATS(get_thd_id(), time_cleanup,  timespan);
	return rc;
}

void
txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
}
