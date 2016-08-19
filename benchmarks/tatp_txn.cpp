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
#if RCU_ALLOC
  scoped_rcu_region guard;
#endif
  return RCOK;
}
