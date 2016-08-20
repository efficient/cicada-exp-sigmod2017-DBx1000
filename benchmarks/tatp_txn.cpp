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

RC tatp_txn_man::run_delete_call_forwarding(tatp_query* query) {
  auto& arg = query->args.delete_call_forwarding;

  uint32_t s_id;
  {
    auto index = _wl->i_subscriber_sub_nbr;
    auto key = subscriberSubNbrKey(arg.sub_nbr);
    auto part_id = key_to_part(key);
    auto row = search(index, key, part_id, RD);
    assert(row != NULL);
    row->get_value((int)SubscriberConst::s_id, s_id);
  }

  {
    auto index = _wl->i_call_forwarding;
    auto key = callForwardingKey(s_id, arg.sf_type, 0);
    auto max_key = callForwardingKey(s_id, arg.sf_type, 23);
    auto part_id = key_to_part(s_id);

    uint64_t cnt = 4;
#if INDEX_STRUCT != IDX_MICA
    itemid_t* items[4];
    auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
    if (idx_rc == Abort) return finish(Abort);
#else
    uint64_t row_ids[4];
    auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
    if (idx_rc == Abort) return finish(Abort);
#endif
    assert(cnt >= 1 && cnt < 4);

    for (uint64_t i = 0; i < cnt; i++) {
#if CC_ALG != MICA
      auto shared = (row_t*)items[i]->location;
      auto local = get_row(shared, RD);

      uint8_t start_time;
      local->get_value((int)CallForwardingConst::start_time, start_time);
      if (start_time != arg.start_time) continue;

      // Get a new local row with WR to allow detecting conflicts.
      local = get_row(shared, WR);
      if (!remove_row(shared)) return finish(Abort);

#else  // CC_ALG == MICA

      // MICA handles row deletion directly without using remove_row().
      auto table = _wl->t_neworder;
#if INDEX_STRUCT != IDX_MICA
      auto row_id = reinterpret_cast<uint64_t>(items[i]->location);
#else
      auto row_id = row_ids[i];
#endif
      MICARowAccessHandle rah(mica_tx);
      if (!rah.peek_row(table->mica_tbl[part_id], row_id, false, true, true) ||
          !rah.read_row())
        return finish(Abort);

      row_t tmp_row;
      auto row = &tmp_row;
      row->table = table;
      row->data = rah.data();

      uint8_t start_time;
      row->get_value((int)CallForwardingConst::start_time, start_time);
      if (start_time != arg.start_time) continue;

      if (!rah.write_row() || !rah.delete_row()) return finish(Abort);
#endif
    }
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

  {
    auto index = _wl->i_special_facility;
    auto key = specialFacilityKey(arg.s_id, arg.sf_type);
    auto part_id = key_to_part(arg.s_id);
    auto row = search(index, key, part_id, RD);
    if (row == NULL) {
      // This may return no matching row.
      return finish(RCOK);
    }
    uint8_t is_active;
    row->get_value((int)SpecialFacilityConst::is_active, is_active);
    if (!is_active) return finish(RCOK);
  }

  {
    auto index = _wl->i_call_forwarding;
    auto key = callForwardingKey(arg.s_id, arg.sf_type, 0);
    auto max_key = callForwardingKey(arg.s_id, arg.sf_type, 23);
    auto part_id = key_to_part(arg.s_id);

    uint64_t cnt = 4;
#if INDEX_STRUCT != IDX_MICA
    itemid_t* items[4];
    auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
    if (idx_rc == Abort) return finish(Abort);
#else
    uint64_t row_ids[4];
    auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
    if (idx_rc == Abort) return finish(Abort);
#endif
    assert(cnt >= 1 && cnt < 4);

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

      uint8_t start_time;
      local->get_value((int)CallForwardingConst::start_time, start_time);
      if (start_time > arg.start_time) continue;

      uint8_t end_time;
      local->get_value((int)CallForwardingConst::end_time, end_time);
      if (end_time <= arg.end_time) continue;

      char numberx[TATP_SUB_NBR_PADDING_SIZE];
      memcpy(numberx, local->get_value((int)CallForwardingConst::numberx),
             TATP_SUB_NBR_PADDING_SIZE);
    }
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
    assert(false);
    return finish(Abort);
  }

  char data[_wl->t_subscriber->get_schema()->get_tuple_size()];
  memcpy(data, row->get_value(0), sizeof(data));
  return finish(RCOK);
}

RC tatp_txn_man::run_insert_call_forwarding(tatp_query* query) {
  auto& arg = query->args.insert_call_forwarding;
  (void)arg;

  // TODO

  // Inserts may fail -- this requires index validation in non-MICA algorithms.

  return finish(RCOK);
}

RC tatp_txn_man::run_update_location(tatp_query* query) {
  auto& arg = query->args.update_location;
  (void)arg;

  // TODO

  return finish(RCOK);
}

RC tatp_txn_man::run_update_subscriber_data(tatp_query* query) {
  auto& arg = query->args.update_subscriber_data;
  (void)arg;

  // TODO

  return finish(RCOK);
}
