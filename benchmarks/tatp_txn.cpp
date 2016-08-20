#define CONFIG_H "silo/config/config-perf.h"
#include "silo/rcu.h"
#ifdef NDEBUG
#undef NDEBUG
#endif

#include "tatp.h"
#include "tatp_query.h"
#include "tatp_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_mica.h"
#include "index_mbtree.h"
#include "tatp_const.h"
#include "mem_alloc.h"
#include "catalog.h"

static void FAIL_ON_ABORT() { assert(false); }

// static void FAIL_ON_ABORT() {}

void tatp_txn_man::init(thread_t* h_thd, workload* h_wl, uint64_t thd_id) {
  txn_man::init(h_thd, h_wl, thd_id);
  _wl = (tatp_wl*)h_wl;
}

RC tatp_txn_man::run_txn(base_query* query) {
  RC rc;

  auto m_query = (tatp_query*)query;
  switch (m_query->type) {
    case TATPTxnType::DeleteCallForwarding:
      rc = run_delete_call_forwarding(m_query);
      break;
    case TATPTxnType::GetAccessData:
      rc = run_get_access_data(m_query);
      break;
    case TATPTxnType::GetNewDestination:
      rc = run_get_new_destination(m_query);
      break;
    case TATPTxnType::GetSubscriberData:
      rc = run_get_subscriber_data(m_query);
      break;
    case TATPTxnType::InsertCallForwarding:
      rc = run_insert_call_forwarding(m_query);
      break;
    case TATPTxnType::UpdateLocation:
      rc = run_update_location(m_query);
      break;
    case TATPTxnType::UpdateSubscriberData:
      rc = run_update_subscriber_data(m_query);
      break;
    default:
      rc = ERROR;
      assert(false);
  }

  return rc;
}

bool tatp_txn_man::get_sub_id(const char* sub_nbr, uint32_t* out_s_id) {
  auto index = _wl->i_subscriber_sub_nbr;
  auto key = subscriberSubNbrKey(sub_nbr);
  auto part_id = key_to_part(key);
  auto row = search(index, key, part_id, RD);
  if (row == NULL) return false;
  row->get_value((int)SubscriberConst::s_id, *out_s_id);
  return true;
}

row_t* tatp_txn_man::get_special_facility(uint32_t s_id, uint8_t sf_type) {
  auto index = _wl->i_special_facility;
  auto key = specialFacilityKey(s_id, sf_type);
  auto part_id = key_to_part(s_id);
  return search(index, key, part_id, RD);
}

template <typename Func>
bool tatp_txn_man::enum_call_forwarding(uint32_t s_id, uint8_t sf_type,
                                        const Func& func) {
  auto index = _wl->i_call_forwarding;
  auto key = callForwardingKey(s_id, sf_type, 0);
  auto max_key = callForwardingKey(s_id, sf_type, 23);
  auto part_id = key_to_part(s_id);

  uint64_t cnt = 4;
#if INDEX_STRUCT != IDX_MICA
  itemid_t* items[4];
  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  if (idx_rc == Abort) return false;
#else
  uint64_t row_ids[4];
  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  if (idx_rc == Abort) return false;
#endif
  assert(cnt < 4);

  for (uint64_t i = 0; i < cnt; i++) {
#if INDEX_STRUCT != IDX_MICA
#if CC_ALG != MICA
    auto shared = (row_t*)items[i]->location;
    auto local = get_row(shared, RD);
#else
    auto local = get_row(index, items[i], part_id, RD);
#endif
#else  // INDEX_STRUCT == IDX_MICA
    itemid_t idx_item;
    auto item = &idx_item;
    item->location = reinterpret_cast<void*>(row_ids[i]);
    auto local = get_row(index, item, part_id, RD);
#endif
    if (local == NULL) return false;

    if (!func(local)) break;
  }
  return true;
}

RC tatp_txn_man::run_delete_call_forwarding(tatp_query* query) {
  auto& arg = query->args.delete_call_forwarding;

  uint32_t s_id;
  if (!get_sub_id(arg.sub_nbr, &s_id)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }

  auto index = _wl->i_call_forwarding;
  auto key = callForwardingKey(s_id, arg.sf_type, 0);
  auto max_key = callForwardingKey(s_id, arg.sf_type, 23);
  auto part_id = key_to_part(s_id);

  uint64_t cnt = 4;
#if INDEX_STRUCT != IDX_MICA
  itemid_t* items[4];
  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  if (idx_rc == Abort) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }
#else
  uint64_t row_ids[4];
  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  if (idx_rc == Abort) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }
#endif
  assert(cnt < 4);

  for (uint64_t i = 0; i < cnt; i++) {
#if CC_ALG != MICA
    auto shared = (row_t*)items[i]->location;
    auto local = get_row(shared, RD);

    uint8_t start_time;
    local->get_value((int)CallForwardingConst::start_time, start_time);
    if (start_time != arg.start_time) continue;

    // Get a new local row with WR to allow detecting conflicts.
    local = get_row(shared, WR);
    if (!remove_row(shared)) {
      FAIL_ON_ABORT();
      return finish(Abort);
    }

#else  // CC_ALG == MICA

    // MICA handles row deletion directly without using remove_row().
    auto table = _wl->t_call_forwarding;
#if INDEX_STRUCT != IDX_MICA
    auto row_id = reinterpret_cast<uint64_t>(items[i]->location);
#else
    auto row_id = row_ids[i];
#endif
    MICARowAccessHandle rah(mica_tx);
    if (!rah.peek_row(table->mica_tbl[part_id], row_id, false, true, true) ||
        !rah.read_row()) {
      FAIL_ON_ABORT();
      return finish(Abort);
    }

    row_t tmp_row;
    auto row = &tmp_row;
    row->table = table;
    row->data = rah.data();

    uint8_t start_time;
    row->get_value((int)CallForwardingConst::start_time, start_time);
    if (start_time != arg.start_time) continue;

    if (!rah.write_row() || !rah.delete_row()) {
      FAIL_ON_ABORT();
      return finish(Abort);
    }
#endif
  }
  return finish(RCOK);
}

RC tatp_txn_man::run_get_access_data(tatp_query* query) {
  auto& arg = query->args.get_access_data;

  set_readonly();

  auto index = _wl->i_access_info;
  auto key = accessInfoKey(arg.s_id, arg.ai_type);
  auto part_id = key_to_part(arg.s_id);
  auto row = search(index, key, part_id, RD);
  if (row == NULL) {
    // This may return no matching row.
    return finish(RCOK);
  }

  uint16_t data1;
  uint16_t data2;
  char data3[3];
  char data4[5];
  row->get_value((int)AccessInfoConst::data1, data1);
  row->get_value((int)AccessInfoConst::data2, data2);
  memcpy(data3, row->get_value((int)AccessInfoConst::data3), 3);
  memcpy(data4, row->get_value((int)AccessInfoConst::data4), 5);
  return finish(RCOK);
}

RC tatp_txn_man::run_get_new_destination(tatp_query* query) {
  auto& arg = query->args.get_new_destination;

  set_readonly();

  auto row = get_special_facility(arg.s_id, arg.sf_type);
  if (row == NULL) {
    // This may return no matching row, which is acceptable.
    return finish(RCOK);
  }
  uint8_t is_active;
  row->get_value((int)SpecialFacilityConst::is_active, is_active);
  if (!is_active) {
    // This special facility may not be active.  We accept it.
    return finish(RCOK);
  }

  if (!enum_call_forwarding(arg.s_id, arg.sf_type, [&arg](auto& row) {
        uint8_t start_time;
        row->get_value((int)CallForwardingConst::start_time, start_time);
        if (start_time > arg.start_time) return true;

        uint8_t end_time;
        row->get_value((int)CallForwardingConst::end_time, end_time);
        if (end_time <= arg.end_time) return true;

        char numberx[TATP_SUB_NBR_PADDING_SIZE];
        memcpy(numberx, row->get_value((int)CallForwardingConst::numberx),
               TATP_SUB_NBR_PADDING_SIZE);
        return true;
      })) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }

  return finish(RCOK);
}

RC tatp_txn_man::run_get_subscriber_data(tatp_query* query) {
  auto& arg = query->args.get_subscriber_data;

  set_readonly();

  auto index = _wl->i_subscriber;
  auto key = subscriberKey(arg.s_id);
  auto part_id = key_to_part(arg.s_id);
  auto row = search(index, key, part_id, RD);
  if (row == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }

  char data[_wl->t_subscriber->get_schema()->get_tuple_size()];
  memcpy(data, row->get_value(0), sizeof(data));
  return finish(RCOK);
}

RC tatp_txn_man::run_insert_call_forwarding(tatp_query* query) {
  auto& arg = query->args.insert_call_forwarding;

  uint32_t s_id;
  if (!get_sub_id(arg.sub_nbr, &s_id)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }

  bool valid_foreign_key = false;
  static const uint64_t spe_arr[] = {1, 2, 3, 4};
  // We need to scan (instead of point-query) due to the specification.
  for (size_t i = 0; i < sizeof(spe_arr) / sizeof(spe_arr[0]); i++) {
    auto row = get_special_facility(s_id, spe_arr[i]);
    if (row != NULL && spe_arr[i] == arg.sf_type) {
      valid_foreign_key = true;
      break;
    }
  }
  if (!valid_foreign_key) {
    // Foreign key failre -- accept it without aborting TX.
    return finish(RCOK);
  }

  // Check invariants.
  // Note that the invariant may be broken after this checking, but it will be handled by the concurrency control.
  bool unique = true;
  if (!enum_call_forwarding(s_id, arg.sf_type, [&arg, &unique](auto& row) {
        uint8_t start_time;
        row->get_value((int)CallForwardingConst::start_time, start_time);
        if (start_time != arg.start_time) return true;
        unique = false;
        return false;
      })) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }

  if (!unique) {
    // Unique key failure -- accept it without aborting TX.
    return finish(RCOK);
  }

// Insert a new row.
#if CC_ALG != MICA
  row_t* row = NULL;
#else
  row_t row_stub;
  auto row = &row_stub;
#endif
  uint64_t row_id;
  auto part_id = key_to_part(s_id);
  if (!insert_row(_wl->t_call_forwarding, row, part_id, row_id)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }
  int col = 0;
  row->set_value(col++, s_id);
  row->set_value(col++, arg.sf_type);
  row->set_value(col++, arg.start_time);
  row->set_value(col++, arg.end_time);
  row->set_value(col++, arg.numberx);

  auto idx = _wl->i_call_forwarding;
  auto key = callForwardingKey(s_id, arg.sf_type, arg.start_time);
  if (!insert_idx(idx, key, row, part_id)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };

  return finish(RCOK);
}

RC tatp_txn_man::run_update_location(tatp_query* query) {
  auto& arg = query->args.update_location;

  auto index = _wl->i_subscriber_sub_nbr;
  auto key = subscriberSubNbrKey(arg.sub_nbr);
  auto part_id = key_to_part(key);
  auto row = search(index, key, part_id, WR);
  if (row == NULL) {
    // There must be a matching subscriber.
    FAIL_ON_ABORT();
    return finish(Abort);
  }
  // uint32_t s_id;
  // row->get_value((int)SubscriberConst::s_id, s_id);

  row->set_value((int)SubscriberConst::vlr_location, arg.vlr_location);
  return finish(RCOK);
}

RC tatp_txn_man::run_update_subscriber_data(tatp_query* query) {
  auto& arg = query->args.update_subscriber_data;

  {
    auto index = _wl->i_subscriber;
    auto key = subscriberKey(arg.s_id);
    auto part_id = key_to_part(arg.s_id);
    auto row = search(index, key, part_id, WR);
    if (row == NULL) {
      // There must be a subscriber row.
      FAIL_ON_ABORT();
      return finish(Abort);
    }

    row->set_value((int)SubscriberConst::bit_1, arg.bit_1);
  }

  {
    auto index = _wl->i_special_facility;
    auto key = specialFacilityKey(arg.s_id, arg.sf_type);
    auto part_id = key_to_part(arg.s_id);

// It is OK if there is no row.
#if INDEX_STRUCT != IDX_MICA
    auto item = index_read(index, key, part_id);
    if (item == NULL) return finish(RCOK);
#else
    itemid_t idx_item;
    auto item = &idx_item;
    auto idx_rc = index_read(index, key, item, part_id);
    if (idx_rc != RCOK) return finish(RCOK);
#endif

#if CC_ALG != MICA
    auto shared = (row_t*)item->location;
    auto local = get_row(shared, WR);
#else
    auto local = get_row(index, item, part_id, WR);
#endif
    // We still should be able to write to an existing row.
    if (local == NULL) {
      FAIL_ON_ABORT();
      return finish(Abort);
    }

    local->set_value((int)SpecialFacilityConst::data_a, arg.data_a);
  }

  return finish(RCOK);
}