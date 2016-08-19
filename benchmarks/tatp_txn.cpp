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
#include "system/mem_alloc.h"

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
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_get_access_data(tatp_query* query) {
  auto& arg = query->args.get_access_data;
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_get_new_destination(tatp_query* query) {
  auto& arg = query->args.get_new_destination;
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_get_subscriber_data(tatp_query* query) {
  auto& arg = query->args.get_subscriber_data;
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_insert_call_forwarding(tatp_query* query) {
  auto& arg = query->args.insert_call_forwarding;
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_update_location(tatp_query* query) {
  auto& arg = query->args.update_location;
  (void)arg;
  return RCOK;
}

RC tatp_txn_man::run_update_subscriber_data(tatp_query* query) {
  auto& arg = query->args.update_subscriber_data;
  (void)arg;
  return RCOK;
}
