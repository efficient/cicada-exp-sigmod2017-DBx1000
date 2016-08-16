#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_mica.h"
#include "index_mbtree.h"
#include "tpcc_const.h"
#include "system/mem_alloc.h"

void tpcc_txn_man::init(thread_t* h_thd, workload* h_wl, uint64_t thd_id) {
  txn_man::init(h_thd, h_wl, thd_id);
  _wl = (tpcc_wl*)h_wl;
}

RC tpcc_txn_man::run_txn(base_query* query) {
  tpcc_query* m_query = (tpcc_query*)query;
  switch (m_query->type) {
    case TPCC_PAYMENT:
      return run_payment(m_query);
      break;
    case TPCC_NEW_ORDER:
      return run_new_order(m_query);
      break;
#if TPCC_FULL
    case TPCC_ORDER_STATUS:
      return run_order_status(m_query);
      break;
    case TPCC_DELIVERY:
      return run_delivery(m_query);
      break;
    case TPCC_STOCK_LEVEL:
      return run_stock_level(m_query);
      break;
#endif
    default:
      assert(false);
  }
}

// void FAIL_ON_ABORT() { assert(false); }

void FAIL_ON_ABORT() {}

//////////////////////////////////////////////////////
// Helper
//////////////////////////////////////////////////////

template <typename IndexT>
row_t* tpcc_txn_man::search(IndexT* index, uint64_t key, uint64_t part_id,
                            access_t type) {
#if INDEX_STRUCT != IDX_MICA
  auto item = index_read(index, key, part_id);
  if (item == NULL) return NULL;
#else
  itemid_t idx_item;
  auto item = &idx_item;
  auto idx_rc = index_read(index, key, item, part_id);
  if (idx_rc != RCOK) return NULL;
#endif
#if CC_ALG != MICA
  auto shared = (row_t*)item->location;
  auto local = get_row(shared, type);
#else
  auto local = get_row(index, item, type);
#endif
  return local;
}

bool tpcc_txn_man::get_new_row(table_t* tbl, row_t*& row, uint64_t part_id,
                               uint64_t& out_row_id) {
#if CC_ALG != MICA
  if (tbl->get_new_row(row, part_id, out_row_id) != RCOK) return false;
  insert_row(row, tbl);
  return true;
#else
  MICARowAccessHandle rah(mica_tx);
  if (!rah.new_row(tbl->mica_tbl)) return false;
  out_row_id = rah.row_id();
  row->set_row_id(out_row_id);
  row->set_part_id(part_id);
  row->table = tbl;
  row->data = rah.data();  // XXX: This can become dangling when GC is done.
  return true;
#endif
}

template <class IndexT>
bool tpcc_txn_man::index_insert(IndexT* index, uint64_t key, row_t* row,
                                int64_t part_id) {
#if INDEX_STRUCT == IDX_MICA
  // We directly insert it into a MICA index.
  assert(false);
  return false;
#else
  insert_idx(index, key, row, part_id);
  return true;
#endif
}

template <class IndexT>
bool tpcc_txn_man::index_remove(IndexT* index, uint64_t key, int64_t part_id) {
#if INDEX_STRUCT == IDX_MICA
  // We directly insert it into a MICA index.
  assert(false);
  return false;
#else
  remove_idx(index, key, part_id);
  return true;
#endif
}

//////////////////////////////////////////////////////
// Payment
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::payment_getWarehouse(uint64_t w_id) {
  // SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?
  auto index = _wl->i_warehouse;
  auto key = warehouseKey(w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, g_wh_update ? WR : RD);
}

void tpcc_txn_man::payment_updateWarehouseBalance(row_t* row, double h_amount) {
  // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
  double w_ytd;
  row->get_value(W_YTD, w_ytd);
  if (g_wh_update) row->set_value(W_YTD, w_ytd + h_amount);
}

row_t* tpcc_txn_man::payment_getDistrict(uint64_t d_w_id, uint64_t d_id) {
  // SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
  auto index = _wl->i_district;
  auto key = distKey(d_id, d_w_id);
  auto part_id = wh_to_part(d_w_id);
  return search(index, key, part_id, WR);
}

void tpcc_txn_man::payment_updateDistrictBalance(row_t* row, double h_amount) {
  // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?
  double d_ytd;
  row->get_value(D_YTD, d_ytd);
  row->set_value(D_YTD, d_ytd + h_amount);
}

row_t* tpcc_txn_man::payment_getCustomerByCustomerId(uint64_t w_id,
                                                     uint64_t d_id,
                                                     uint64_t c_id) {
  // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, WR);
}

row_t* tpcc_txn_man::payment_getCustomerByLastName(uint64_t w_id, uint64_t d_id,
                                                   const char* c_last,
                                                   uint64_t* out_c_id) {
  // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST;
  // XXX: the list is not sorted. But let's assume it's sorted...
  // The performance won't be much different.
  auto index = _wl->i_customer_last;
  auto key = custNPKey(d_id, w_id, c_last);
  auto part_id = wh_to_part(w_id);
#if INDEX_STRUCT != IDX_MICA
  auto item = index_read(index, key, part_id);
  if (item == NULL) {
    assert(false);
    return NULL;
  }

  uint64_t cnt = 0;
  auto it = item;
  auto mid = item;
  while (it != NULL) {
    cnt++;
    it = it->next;
    if (cnt % 2 == 0) mid = mid->next;
  }
#else
  uint64_t cnt = 100;
  uint64_t row_ids[100];

  auto idx_rc = index_read_multiple(index, key, row_ids, cnt, part_id);
  if (idx_rc == Abort) return NULL;
  assert(idx_rc == RCOK);
  assert(cnt != 0 && cnt != 100);

  itemid_t idx_item;
  auto mid = &idx_item;
  mid->location = reinterpret_cast<void*>(row_ids[cnt / 2]);
#endif

#if CC_ALG != MICA
  auto shared = (row_t*)mid->location;
  auto local = get_row(shared, WR);
#else
  auto local = get_row(index, mid, WR);
#endif
  if (local != NULL) local->get_value(C_ID, *out_c_id);
  // printf("payment_getCustomerByLastName: %" PRIu64 "\n", cnt);
  return local;
}

void tpcc_txn_man::payment_updateCustomer(row_t* row, uint64_t c_id,
                                          uint64_t c_d_id, uint64_t c_w_id,
                                          uint64_t d_id, uint64_t w_id,
                                          double h_amount) {
  // UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  // UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  double c_balance;
  row->get_value(C_BALANCE, c_balance);
  row->set_value(C_BALANCE, c_balance - h_amount);
  double c_ytd_payment;
  row->get_value(C_YTD_PAYMENT, c_ytd_payment);
  row->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
  uint64_t c_payment_cnt;
  row->get_value(C_PAYMENT_CNT, c_payment_cnt);
  row->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

#if TPCC_FULL
#if !TPCC_SMALL
  const char* c_credit = row->get_value(C_CREDIT);
  if (strstr(c_credit, "BC")) {
    char c_new_data[501];
    sprintf(c_new_data, "%4d %2d %4d %2d %4d $%7.2f | ", (int)c_id, (int)c_d_id,
            (int)c_w_id, (int)d_id, (int)w_id, h_amount);
    const char* c_data = row->get_value(C_DATA);
    strncat(c_new_data, c_data, 500 - strlen(c_new_data));
    row->set_value(C_DATA, c_new_data);
  }
#endif
#endif
}

bool tpcc_txn_man::payment_insertHistory(uint64_t c_id, uint64_t c_d_id,
                                         uint64_t c_w_id, uint64_t d_id,
                                         uint64_t w_id, uint64_t h_date,
                                         double h_amount, const char* h_data) {
// INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#if CC_ALG != MICA
  row_t* row = NULL;
#else
  row_t row_stub;
  auto row = &row_stub;
#endif
  uint64_t row_id;
  auto part_id = wh_to_part(w_id);
  if (!get_new_row(_wl->t_history, row, part_id, row_id)) return false;
  row->set_value(H_C_ID, c_id);
  row->set_value(H_C_D_ID, c_d_id);
  row->set_value(H_C_W_ID, c_w_id);
  row->set_value(H_D_ID, d_id);
  row->set_value(H_W_ID, w_id);
  row->set_value(H_DATE, h_date);
  row->set_value(H_AMOUNT, h_amount);
#if !TPCC_SMALL
  row->set_value(H_DATA, const_cast<char*>(h_data));
#endif

  // No index to update.
  return true;
}

RC tpcc_txn_man::run_payment(tpcc_query* query) {
  auto& arg = query->args.payment;

  auto warehouse = payment_getWarehouse(arg.w_id);
  if (warehouse == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  }
  payment_updateWarehouseBalance(warehouse, arg.h_amount);

  auto district = payment_getDistrict(arg.w_id, arg.d_id);
  if (district == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
  payment_updateDistrictBalance(district, arg.h_amount);

  auto c_id = arg.c_id;
  row_t* customer;
  if (!arg.by_last_name)
    customer = payment_getCustomerByCustomerId(arg.w_id, arg.d_id, arg.c_id);
  else
    customer =
        payment_getCustomerByLastName(arg.w_id, arg.d_id, arg.c_last, &c_id);
  if (customer == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
  payment_updateCustomer(customer, c_id, arg.c_d_id, arg.c_w_id, arg.d_id,
                         arg.w_id, arg.h_amount);

#if TPCC_INSERT_ROWS
  char w_name[11];
  char* tmp_str = warehouse->get_value(W_NAME);
  memcpy(w_name, tmp_str, 10);
  w_name[10] = '\0';

  char d_name[11];
  tmp_str = district->get_value(D_NAME);
  memcpy(d_name, tmp_str, 10);
  d_name[10] = '\0';

  char h_data[25];
#if !TPCC_SMALL
  strcpy(h_data, w_name);
  int length = strlen(h_data);
  strcpy(&h_data[length], "    ");
  strcpy(&h_data[length + 4], d_name);
  h_data[length + 14] = '\0';
#endif

  if (!payment_insertHistory(c_id, arg.c_d_id, arg.c_w_id, arg.d_id, arg.w_id,
                             arg.h_date, arg.h_amount, h_data)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
#endif

  return finish(RCOK);
}

//////////////////////////////////////////////////////
// New Order
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::new_order_getWarehouseTaxRate(uint64_t w_id) {
  // SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?
  auto index = _wl->i_warehouse;
  auto key = warehouseKey(w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::new_order_getDistrict(uint64_t d_id, uint64_t d_w_id) {
  // SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?
  auto index = _wl->i_district;
  auto key = distKey(d_id, d_w_id);
  auto part_id = wh_to_part(d_w_id);
  return search(index, key, part_id, WR);
}

void tpcc_txn_man::new_order_incrementNextOrderId(row_t* row,
                                                  int64_t* out_o_id) {
  // UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?
  int64_t o_id;
  row->get_value(D_NEXT_O_ID, o_id);
  // printf("%" PRIi64 "\n", o_id);
  *out_o_id = o_id;
  o_id++;
  row->set_value(D_NEXT_O_ID, o_id);
}

row_t* tpcc_txn_man::new_order_getCustomer(uint64_t w_id, uint64_t d_id,
                                           uint64_t c_id) {
  // SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, RD);
}

bool tpcc_txn_man::new_order_createOrder(int64_t o_id, uint64_t d_id,
                                         uint64_t w_id, uint64_t c_id,
                                         uint64_t o_entry_d,
                                         uint64_t o_carrier_id, uint64_t ol_cnt,
                                         bool all_local) {
// INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#if CC_ALG != MICA
  row_t* row = NULL;
#else
  row_t row_stub;
  auto row = &row_stub;
#endif
  uint64_t row_id;
  auto part_id = wh_to_part(w_id);
  if (!get_new_row(_wl->t_order, row, part_id, row_id)) return false;
  row->set_value(O_ID, o_id);
  row->set_value(O_D_ID, d_id);
  row->set_value(O_W_ID, w_id);
  row->set_value(O_C_ID, c_id);
  row->set_value(O_ENTRY_D, o_entry_d);
  row->set_value(O_CARRIER_ID, o_carrier_id);
  row->set_value(O_OL_CNT, ol_cnt);
  row->set_value(O_ALL_LOCAL, all_local);

#if TPCC_UPDATE_INDEX
#if INDEX_STRUCT != IDX_MICA
  {
    auto idx = _wl->i_order;
    auto key = orderKey(o_id, d_id, w_id);
    if (!index_insert(idx, key, row, part_id)) return false;
  }
  {
    auto idx = _wl->i_order_cust;
    auto key = orderCustKey(o_id, c_id, d_id, w_id);
    if (!index_insert(idx, key, row, part_id)) return false;
  }
#else
  {
    auto mica_idx = _wl->i_order->mica_idx;
    auto key = orderKey(o_id, d_id, w_id);
    // if (mica_idx[part_id]->insert(mica_tx, make_pair(key, row_id), 0) != 1)
    if (mica_idx[part_id]->insert(mica_tx, key, row_id) != 1) return false;
  }
  {
    auto mica_idx = _wl->i_order_cust->mica_idx;
    auto key = orderCustKey(o_id, c_id, d_id, w_id);
    // if (mica_idx[part_id]->insert(mica_tx, make_pair(key, row_id), 0) != 1)
    if (mica_idx[part_id]->insert(mica_tx, key, row_id) != 1) return false;
  }
#endif
#endif
  return true;
}

bool tpcc_txn_man::new_order_createNewOrder(int64_t o_id, uint64_t d_id,
                                            uint64_t w_id) {
// INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)
#if CC_ALG != MICA
  row_t* row = NULL;
#else
  row_t row_stub;
  auto row = &row_stub;
#endif
  uint64_t row_id;
  auto part_id = wh_to_part(w_id);
  if (!get_new_row(_wl->t_neworder, row, part_id, row_id)) return false;
  row->set_value(NO_O_ID, o_id);
  row->set_value(NO_D_ID, d_id);
  row->set_value(NO_W_ID, w_id);

#if TPCC_UPDATE_INDEX
#if INDEX_STRUCT != IDX_MICA
  {
    auto idx = _wl->i_neworder;
    auto key = neworderKey(o_id, d_id, w_id);
    if (!index_insert(idx, key, row, part_id)) return false;
  }
#else
  {
    auto mica_idx = _wl->i_neworder->mica_idx;
    auto key = neworderKey(o_id, d_id, w_id);
    // if (mica_idx[part_id]->insert(mica_tx, make_pair(key, row_id), 0) != 1)
    if (mica_idx[part_id]->insert(mica_tx, key, row_id) != 1) return false;
  }
#endif
#endif
  return true;
}

row_t* tpcc_txn_man::new_order_getItemInfo(uint64_t ol_i_id) {
  // SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?
  auto index = _wl->i_item;
  auto key = itemKey(ol_i_id);
  auto part_id = 0;
  return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::new_order_getStockInfo(uint64_t ol_i_id,
                                            uint64_t ol_supply_w_id) {
  // SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?
  auto index = _wl->i_stock;
  auto key = stockKey(ol_i_id, ol_supply_w_id);
  auto part_id = wh_to_part(ol_supply_w_id);
  return search(index, key, part_id, WR);
}

void tpcc_txn_man::new_order_updateStock(row_t* row, uint64_t ol_quantity,
                                         bool remote) {
  // UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?
  uint64_t s_quantity;
  row->get_value(S_QUANTITY, s_quantity);
#if !TPCC_SMALL
  uint64_t s_ytd;
  uint64_t s_order_cnt;
  row->get_value(S_YTD, s_ytd);
  row->set_value(S_YTD, s_ytd + ol_quantity);
  row->get_value(S_ORDER_CNT, s_order_cnt);
  row->set_value(S_ORDER_CNT, s_order_cnt + 1);
#endif
  if (remote) {
    uint64_t s_remote_cnt;
    row->get_value(S_REMOTE_CNT, s_remote_cnt);
    row->set_value(S_REMOTE_CNT, s_remote_cnt + 1);
  }
  uint64_t quantity;
  if (s_quantity > ol_quantity + 10)
    quantity = s_quantity - ol_quantity;
  else
    quantity = s_quantity - ol_quantity + 91;
  row->set_value(S_QUANTITY, quantity);
}

bool tpcc_txn_man::new_order_createOrderLine(
    int64_t o_id, uint64_t d_id, uint64_t w_id, uint64_t ol_number,
    uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,
    double ol_amount, const char* ol_dist_info) {
// INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#if CC_ALG != MICA
  row_t* row = NULL;
#else
  row_t row_stub;
  auto row = &row_stub;
#endif
  uint64_t row_id;
  auto part_id = wh_to_part(w_id);
  if (!get_new_row(_wl->t_orderline, row, part_id, row_id)) return false;
  row->set_value(OL_O_ID, o_id);
  row->set_value(OL_D_ID, d_id);
  row->set_value(OL_W_ID, w_id);
  row->set_value(OL_NUMBER, ol_number);
  row->set_value(OL_I_ID, ol_i_id);
#if !TPCC_SMALL
  row->set_value(OL_SUPPLY_W_ID, ol_supply_w_id);
  row->set_value(OL_QUANTITY, ol_quantity);
  row->set_value(OL_AMOUNT, ol_amount);
  row->set_value(OL_DIST_INFO, const_cast<char*>(ol_dist_info));
#endif

#if TPCC_UPDATE_INDEX
#if INDEX_STRUCT != IDX_MICA
  {
    auto idx = _wl->i_orderline;
    auto key = orderlineKey(ol_number, o_id, d_id, w_id);
    if (!index_insert(idx, key, row, part_id)) return false;
  }
#else
  {
    auto mica_idx = _wl->i_orderline->mica_idx;
    auto key = orderlineKey(ol_number, o_id, d_id, w_id);
    // if (mica_idx[part_id]->insert(mica_tx, make_pair(key, row_id), 0) != 1)
    if (mica_idx[part_id]->insert(mica_tx, key, row_id) != 1) {
      // printf("ol_i_id=%d o_id=%d d_id=%d w_id=%d\n", (int)ol_i_id, (int)o_id,
      //        (int)d_id, (int)w_id);
      return false;
    }
  }
#endif
#endif
  return true;
}

RC tpcc_txn_man::run_new_order(tpcc_query* query) {
  auto& arg = query->args.new_order;

  row_t* items[15];
  assert(arg.ol_cnt <= sizeof(items) / sizeof(items[0]));
  for (uint64_t ol_number = 1; ol_number <= arg.ol_cnt; ol_number++) {
    items[ol_number - 1] =
        new_order_getItemInfo(arg.items[ol_number - 1].ol_i_id);
    // printf("ol_i_id %d\n", (int)arg.items[ol_number - 1].ol_i_id);
    if (items[ol_number - 1] == NULL) {
      assert(false);
      // FAIL_ON_ABORT();
      return finish(Abort);
    };
  }

  auto warehouse = new_order_getWarehouseTaxRate(arg.w_id);
  if (warehouse == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
  // double w_tax;
  // warehouse->get_value(W_TAX, w_tax);

  auto district = new_order_getDistrict(arg.d_id, arg.w_id);
  if (district == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
  // double d_tax;
  // r_dist_local->get_value(D_TAX, d_tax);

  int64_t o_id;
  new_order_incrementNextOrderId(district, &o_id);

  auto customer = new_order_getCustomer(arg.w_id, arg.d_id, arg.c_id);
  if (customer == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
// uint64_t c_discount;
// customer->get_value(C_DISCOUNT, c_discount);

#if TPCC_INSERT_ROWS
  uint64_t o_carrier_id = 0;
  if (!new_order_createOrder(o_id, arg.d_id, arg.w_id, arg.c_id, arg.o_entry_d,
                             o_carrier_id, arg.ol_cnt, arg.all_local)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
  if (!new_order_createNewOrder(o_id, arg.d_id, arg.w_id)) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };
#endif

  for (uint64_t ol_number = 1; ol_number <= arg.ol_cnt; ol_number++) {
    uint64_t ol_i_id = arg.items[ol_number - 1].ol_i_id;
    uint64_t ol_supply_w_id = arg.items[ol_number - 1].ol_supply_w_id;
    uint64_t ol_quantity = arg.items[ol_number - 1].ol_quantity;

    auto stock = new_order_getStockInfo(ol_i_id, ol_supply_w_id);
    if (stock == NULL) {
      FAIL_ON_ABORT();
      return finish(Abort);
    };
    bool remote = ol_supply_w_id != arg.w_id;
    new_order_updateStock(stock, ol_quantity, remote);

#if TPCC_INSERT_ROWS
    double i_price;
    items[ol_number - 1]->get_value(I_PRICE, i_price);
    double ol_amount = ol_quantity * i_price;
    assert(arg.d_id >= 1 && arg.d_id <= DIST_PER_WARE);
    const char* ol_dist_info = stock->get_value(S_DIST_01 + arg.d_id - 1);
    if (!new_order_createOrderLine(o_id, arg.d_id, arg.w_id, ol_number, ol_i_id,
                                   ol_supply_w_id, ol_quantity, ol_amount,
                                   ol_dist_info)) {
      FAIL_ON_ABORT();
      return finish(Abort);
    };
#endif
  }
  return finish(RCOK);
}

//////////////////////////////////////////////////////
// Order Status
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::order_status_getCustomerByCustomerId(uint64_t w_id,
                                                          uint64_t d_id,
                                                          uint64_t c_id) {
  // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::order_status_getCustomerByLastName(uint64_t w_id,
                                                        uint64_t d_id,
                                                        const char* c_last,
                                                        uint64_t* out_c_id) {
  // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST
  // XXX: the list is not sorted. But let's assume it's sorted...
  // The performance won't be much different.
  auto index = _wl->i_customer_last;
  auto key = custNPKey(d_id, w_id, c_last);
  auto part_id = wh_to_part(w_id);
#if INDEX_STRUCT != IDX_MICA
  auto item = index_read(index, key, part_id);
  assert(item != NULL);

  uint64_t cnt = 0;
  auto it = item;
  auto mid = item;
  while (it != NULL) {
    cnt++;
    it = it->next;
    if (cnt % 2 == 0) mid = mid->next;
  }
#else
  uint64_t cnt = 100;
  uint64_t row_ids[100];

  auto idx_rc = index_read_multiple(index, key, row_ids, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);
  assert(cnt != 0 && cnt != 100);

  itemid_t idx_item;
  auto mid = &idx_item;
  mid->location = reinterpret_cast<void*>(row_ids[cnt / 2]);
#endif

#if CC_ALG != MICA
  auto shared = (row_t*)mid->location;
  auto local = get_row(shared, RD);
#else
  auto local = get_row(index, mid, RD);
#endif
  if (local != NULL) local->get_value(C_ID, *out_c_id);
  // printf("order_status_getCustomerByLastName: %" PRIu64 "\n", cnt);
  return local;
}

row_t* tpcc_txn_man::order_status_getLastOrder(uint64_t w_id, uint64_t d_id,
                                               uint64_t c_id) {
  // SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1
  auto index = _wl->i_order_cust;
  auto key = orderCustKey(g_max_orderline, c_id, d_id, w_id);
  auto max_key = orderCustKey(1, c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

#if INDEX_STRUCT != IDX_MICA
  uint64_t cnt = 1;
  itemid_t* items[1];

  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);

  // printf("order_status_getLastOrder: %" PRIu64 "\n", cnt);
  if (cnt == 0) {
    // There must be at least one order per customer.
    printf("order_status_getLastOrder: w_id=%" PRIu64 " d_id=%" PRIu64
           " c_id=%" PRIu64 " cnt=%" PRIu64 "\n",
           w_id, d_id, c_id, cnt);
    assert(false);
    return NULL;
  }
  if (cnt == 0) return NULL;

#if CC_ALG != MICA
  auto shared = (row_t*)items[0]->location;
  // Use the shared copy because no one is modifying the row.
  // auto local = get_row(shared, RD);
  auto local = shared;
#else
  auto local = get_row(index, items[0], RD);
#endif

#else  // INDEX_STRUCT == IDX_MICA

  uint64_t cnt = 1;
  uint64_t row_ids[1];

  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);

  // printf("order_status_getLastOrder: %" PRIu64 "\n", cnt);
  if (cnt == 0) {
    // There must be at least one order per customer.
    printf("order_status_getLastOrder: w_id=%" PRIu64 " d_id=%" PRIu64
           " c_id=%" PRIu64 " cnt=%" PRIu64 "\n",
           w_id, d_id, c_id, cnt);
    assert(mica_tx->is_peek_only());
    index->mica_idx[part_id]->check(mica_tx);
    assert(false);
    return NULL;
  }
  if (cnt == 0) return NULL;

  itemid_t idx_item;
  auto item = &idx_item;
  item->location = reinterpret_cast<void*>(row_ids[0]);

  auto local = get_row(index, item, RD);
#endif
  assert(local != NULL);
  return local;
}

void tpcc_txn_man::order_status_getOrderLines(uint64_t w_id, uint64_t d_id,
                                              int64_t o_id) {
  // SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?
  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, o_id, d_id, w_id);
  auto max_key = orderlineKey(15, o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

#if INDEX_STRUCT != IDX_MICA

  uint64_t cnt = 16;
  itemid_t* items[16];

  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);
  assert(cnt != 16);

  for (uint64_t i = 0; i < cnt; i++) {
#if CC_ALG != MICA
    auto item = items[i];
    auto shared = (row_t*)item->location;
    // Use the shared copy because no one is modifying the row.
    // auto local = get_row(shared, RD);
    auto local = shared;
#else
    auto local = get_row(index, items[i], RD);
#endif
    assert(local != NULL);

    // int64_t ol_i_id;
    // uint64_t ol_supply_w_id, ol_amount, ol_delivery_d;
    // double ol_amount;
    // local->get_value(OL_I_ID, ol_i_id);
    // local->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    // local->get_value(OL_QUANTITY, ol_quantity);
    // local->get_value(OL_AMOUNT, ol_amount);
    // local->get_value(OL_DELIVERY_D, ol_delivery_d);
    (void)local;
  }

#else  // INDEX_STRUCT == IDX_MICA

  uint64_t cnt = 16;
  uint64_t row_ids[16];

  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);
  assert(cnt != 16);

  itemid_t idx_item;
  auto item = &idx_item;

  for (uint64_t i = 0; i < cnt; i++) {
    item->location = reinterpret_cast<void*>(row_ids[i]);
    auto local = get_row(index, item, RD);
    assert(local != NULL);
    // int64_t ol_i_id;
    // uint64_t ol_supply_w_id, ol_amount, ol_delivery_d;
    // double ol_amount;
    // local->get_value(OL_I_ID, ol_i_id);
    // local->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    // local->get_value(OL_QUANTITY, ol_quantity);
    // local->get_value(OL_AMOUNT, ol_amount);
    // local->get_value(OL_DELIVERY_D, ol_delivery_d);
    (void)local;
  }
#endif
  // printf("order_status_getOrderLines: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
  //        w_id, d_id, o_id, cnt);
}

RC tpcc_txn_man::run_order_status(tpcc_query* query) {
#if TPCC_FULL
  set_readonly();

  auto& arg = query->args.order_status;

  auto c_id = arg.c_id;
  row_t* customer;
  if (!arg.by_last_name)
    customer =
        order_status_getCustomerByCustomerId(arg.w_id, arg.d_id, arg.c_id);
  else
    customer = order_status_getCustomerByLastName(arg.w_id, arg.d_id,
                                                  arg.c_last, &c_id);
  if (customer == NULL) {
    FAIL_ON_ABORT();
    return finish(Abort);
  };

  auto order = order_status_getLastOrder(arg.w_id, arg.d_id, c_id);
  if (order != NULL) {
    int64_t o_id;
    order->get_value(O_ID, o_id);

    order_status_getOrderLines(arg.w_id, arg.d_id, o_id);
  }
#endif

  return finish(RCOK);
}

//////////////////////////////////////////////////////
// Delivery
//////////////////////////////////////////////////////

bool tpcc_txn_man::delivery_getNewOrder_deleteNewOrder(uint64_t d_id,
                                                       uint64_t w_id,
                                                       int64_t* out_o_id) {
  // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
  // DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?

  auto index = _wl->i_neworder;
  // TODO: This may cause a match with other district with a negative order ID.  It is safe for now because the lowest order ID is 1, but we should give more gap (or use tuple keys) to avoid accidental matches.
  auto key = neworderKey(g_max_orderline, d_id, w_id);
  auto max_key = neworderKey(0, d_id, w_id);  // Use key ">= 0" for "> -1"
  auto part_id = wh_to_part(w_id);

#if INDEX_STRUCT != IDX_MICA
  uint64_t cnt = 1;
  itemid_t* items[1];

  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  if (idx_rc == Abort) return false;
  assert(idx_rc == RCOK);

  // printf("delivery_getNewOrder_deleteNewOrder: %" PRIu64 "\n", cnt);
  if (cnt == 0) {
    // No new order; this is acceptable and we do not need to abort TX.
    *out_o_id = -1;
    return true;
  }

  auto item = items[0];

#else  // INDEX_STRUCT == IDX_MICA

  uint64_t cnt = 1;
  uint64_t row_ids[1];

  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  if (idx_rc == Abort) return false;
  assert(idx_rc == RCOK);

  // printf("delivery_getNewOrder_deleteNewOrder: %" PRIu64 "\n", cnt);
  if (cnt == 0) {
    // No new order; this is acceptable and we do not need to abort TX.
    *out_o_id = -1;
    return true;
  }

  itemid_t idx_item;
  auto item = &idx_item;
  item->location = reinterpret_cast<void*>(row_ids[0]);
#endif

#if CC_ALG != MICA
  auto shared = (row_t*)item->location;
  auto local = get_row(shared, WR);
  if (local == NULL) return false;

  int64_t o_id;
  local->get_value(NO_O_ID, o_id);
  *out_o_id = o_id;

  // XXX: DBx1000 CC schemes do not implement removes
  // assert(false);
  return true;
#else
  // auto local = get_row(index, item, WR);
  // Use the raw interface directly for deletion.
  auto table = _wl->t_neworder;
  auto row_id = reinterpret_cast<uint64_t>(item->location);
  // printf("%" PRIu64 "\n", row_id);
  MICARowAccessHandle rah(mica_tx);
  if (!rah.peek_row(table->mica_tbl, row_id, false, true, true) ||
      !rah.read_row() || !rah.write_row()) {
    return false;
  }

  row_t tmp_row;
  auto row = &tmp_row;
  row->table = table;
  row->data = rah.data();

  int64_t o_id;
  row->get_value(NO_O_ID, o_id);
  *out_o_id = o_id;

  if (!rah.delete_row()) return false;
#endif

#if INDEX_STRUCT != IDX_MICA
  {
    auto idx = _wl->i_neworder;
    auto key = neworderKey(o_id, d_id, w_id);
    index_remove(idx, key, part_id);
    return true;
  }
#else
  {
    // printf("w_id=%" PRIu64 " d_id=%" PRIu64 " o_id=%" PRIu64 "\n", w_id, d_id, o_id);
    auto mica_idx = _wl->i_neworder->mica_idx;
    auto key = neworderKey(o_id, d_id, w_id);
    // return mica_idx[part_id]->remove(mica_tx, make_pair(key, row_id), 0) == 1;
    return mica_idx[part_id]->remove(mica_tx, key, row_id) == 1;
  }
#endif
}

row_t* tpcc_txn_man::delivery_getCId(int64_t no_o_id, uint64_t d_id,
                                     uint64_t w_id) {
  // SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  auto index = _wl->i_order;
  auto key = orderKey(no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, WR);
}

void tpcc_txn_man::delivery_updateOrders(row_t* row, uint64_t o_carrier_id) {
  // UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  row->set_value(O_CARRIER_ID, o_carrier_id);
}

bool tpcc_txn_man::delivery_updateOrderLine_sumOLAmount(uint64_t o_entry_d,
                                                        int64_t no_o_id,
                                                        uint64_t d_id,
                                                        uint64_t w_id,
                                                        double* out_ol_total) {
  // UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?
  // SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
  double ol_total = 0.0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, no_o_id, d_id, w_id);
  auto max_key = orderlineKey(15, no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

#if INDEX_STRUCT != IDX_MICA

  uint64_t cnt = 16;
  itemid_t* items[16];

  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  if (idx_rc != RCOK) return false;
  assert(cnt != 16);

  for (uint64_t i = 0; i < cnt; i++) {
#if CC_ALG != MICA
    auto shared = (row_t*)items[i]->location;
    auto local = get_row(shared, WR);
#else
    auto local = get_row(index, items[i], WR);
#endif
    if (local == NULL) return false;
    double ol_amount;
    local->get_value(OL_AMOUNT, ol_amount);
    local->set_value(OL_DELIVERY_D, o_entry_d);
    ol_total += ol_amount;
  }

#else  // INDEX_STRUCT == IDX_MICA

  uint64_t cnt = 16;
  uint64_t row_ids[16];

  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  if (idx_rc != RCOK) return false;
  assert(cnt != 16);

  itemid_t idx_item;
  auto item = &idx_item;

  for (uint64_t i = 0; i < cnt; i++) {
    item->location = reinterpret_cast<void*>(row_ids[i]);
    auto local = get_row(index, item, WR);
    if (local == NULL) return false;
    double ol_amount;
    local->get_value(OL_AMOUNT, ol_amount);
    local->set_value(OL_DELIVERY_D, o_entry_d);
    ol_total += ol_amount;
  }
#endif
  // printf("delivery_updateOrderLine_sumOLAmount: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
  //        w_id, d_id, no_o_id, cnt);
  *out_ol_total = ol_total;
  return true;
}

bool tpcc_txn_man::delivery_updateCustomer(double ol_total, uint64_t c_id,
                                           uint64_t d_id, uint64_t w_id) {
  // UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  auto row = search(index, key, part_id, WR);
  if (row == NULL) return false;

  double c_balance;
  row->get_value(C_BALANCE, c_balance);
  row->set_value(C_BALANCE, c_balance + ol_total);
  return true;
}

RC tpcc_txn_man::run_delivery(tpcc_query* query) {
#if TPCC_FULL
  auto& arg = query->args.delivery;

  for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
    int64_t o_id;
    if (!delivery_getNewOrder_deleteNewOrder(arg.w_id, d_id, &o_id)) {
      FAIL_ON_ABORT();
      // printf("oops0\n");
      return finish(Abort);
    }
    // No new order for this district.
    if (o_id == -1) {
      // printf("skipping w_id=%" PRIu64 " d_id=%" PRIu64 " for no new order\n",
      //        arg.w_id, d_id);
      continue;
    }

    auto order = delivery_getCId(o_id, d_id, arg.w_id);
    if (order == NULL) {
      // There is no guarantee that we will see a order row even after seeing a related new_order row in this read-write transaction.  We ignore this (as in Silo) and try later.
      continue;
      // FAIL_ON_ABORT();
      // printf("oops1\n");
      // return finish(Abort);
    }
    uint64_t c_id;
    order->get_value(O_C_ID, c_id);

    delivery_updateOrders(order, arg.o_carrier_id);

    double ol_total;
    if (!delivery_updateOrderLine_sumOLAmount(arg.ol_delivery_d, o_id, d_id,
                                              arg.w_id, &ol_total)) {
      FAIL_ON_ABORT();
      // printf("oops2\n");
      return finish(Abort);
    }

    if (!delivery_updateCustomer(ol_total, c_id, d_id, arg.w_id)) {
      FAIL_ON_ABORT();
      // printf("oops3\n");
      return finish(Abort);
    }
  }
#endif

  return finish(RCOK);
}

//////////////////////////////////////////////////////
// Stock Level
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::stock_level_getOId(uint64_t d_w_id, uint64_t d_id) {
  // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
  auto index = _wl->i_district;
  auto key = distKey(d_id, d_w_id);
  auto part_id = wh_to_part(d_w_id);
  return search(index, key, part_id, RD);
}

bool tpcc_txn_man::stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
                                             int64_t ol_o_id, uint64_t s_w_id,
                                             uint64_t threshold,
                                             uint64_t* out_distinct_count) {
  // SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
  // WHERE OL_W_ID = ?
  //   AND OL_D_ID = ?
  //   AND OL_O_ID < ?
  //   AND OL_O_ID >= ?
  //   AND S_W_ID = ?
  //   AND S_I_ID = OL_I_ID
  //   AND S_QUANTITY < ?

  // 20 orders * 15 items = 300; use 301 to check any errors.
  uint64_t ol_i_id_list[301];
  size_t list_size = 0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, ol_o_id - 1, ol_d_id, ol_w_id);
  auto max_key = orderlineKey(15, ol_o_id - 20, ol_d_id, ol_w_id);
  auto part_id = wh_to_part(ol_w_id);

#if INDEX_STRUCT != IDX_MICA
  uint64_t cnt = 301;
  itemid_t* items[301];

  auto idx_rc = index_read_range(index, key, max_key, items, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);

  for (uint64_t i = 0; i < cnt; i++) {
#if CC_ALG != MICA
    // Use the shared copy because no one is modifying the row.
    auto orderline_shared = (row_t*)items[i]->location;
    auto orderline = orderline_shared;
#else
    auto orderline = get_row(index, items[i], RD);
#endif
    assert(orderline != NULL);

    uint64_t ol_i_id, ol_supply_w_id;
    orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    if (ol_supply_w_id != s_w_id) continue;

    orderline->get_value(OL_I_ID, ol_i_id);

    assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
    ol_i_id_list[list_size] = ol_i_id;
    list_size++;
  }

#else  // INDEX_STRUCT == IDX_MICA

  uint64_t cnt = 301;
  uint64_t row_ids[301];

  auto idx_rc = index_read_range(index, key, max_key, row_ids, cnt, part_id);
  assert(idx_rc != Abort);
  assert(idx_rc == RCOK);

  itemid_t idx_item;
  auto item = &idx_item;

  for (uint64_t i = 0; i < cnt; i++) {
    item->location = reinterpret_cast<void*>(row_ids[i]);

    auto orderline = get_row(index, item, RD);
#if CC_ALG != MICA
    if (orderline == NULL) return false;
#else
    assert(orderline != NULL);
#endif

    uint64_t ol_i_id, ol_supply_w_id;
    orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    if (ol_supply_w_id != s_w_id) continue;

    orderline->get_value(OL_I_ID, ol_i_id);

    assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
    ol_i_id_list[list_size] = ol_i_id;
    list_size++;
  }
#endif
  assert(list_size <= 300);

  uint64_t distinct_ol_i_id_list[300];
  uint64_t distinct_ol_i_id_count = 0;
  uint64_t result = 0;

  for (uint64_t i = 0; i < list_size; i++) {
    uint64_t ol_i_id = ol_i_id_list[i];

    bool duplicate = false;
    for (uint64_t j = 0; j < distinct_ol_i_id_count; j++)
      if (distinct_ol_i_id_list[j] == ol_i_id) {
        duplicate = true;
        break;
      }
    if (duplicate) continue;

    distinct_ol_i_id_list[distinct_ol_i_id_count++] = ol_i_id;

    auto key = stockKey(ol_i_id, s_w_id);
    auto index = _wl->i_stock;
    auto part_id = wh_to_part(s_w_id);

#if INDEX_STRUCT != IDX_MICA
    auto item = index_read(index, key, part_id);
    assert(item != NULL);
#else
    idx_rc = index_read(index, key, item, part_id);
    assert(idx_rc != Abort);
    assert(idx_rc == RCOK);
#endif

#if CC_ALG != MICA
    auto shared = (row_t*)item->location;
    auto local = get_row(shared, RD);
#else
    auto local = get_row(index, item, RD);
#endif
#if CC_ALG != MICA
    if (local == NULL) return false;
#else
    assert(local != NULL);
#endif

    uint64_t s_quantity;
    local->get_value(S_QUANTITY, s_quantity);
    if (s_quantity < threshold) result++;
  }

  // printf("stock_level_getStockCount: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " s_w_id=%" PRIu64 " list_size=%" PRIu64
  //        " distinct_cnt=%" PRIu64 " result=%" PRIu64 "\n",
  //        ol_w_id, ol_d_id, ol_o_id, s_w_id, list_size, distinct_ol_i_id_count,
  //        result);
  *out_distinct_count = result;
  return true;
}

RC tpcc_txn_man::run_stock_level(tpcc_query* query) {
#if TPCC_FULL
  set_readonly();

  auto& arg = query->args.stock_level;

  auto district = stock_level_getOId(arg.w_id, arg.d_id);
#if CC_ALG != MICA
  if (district == NULL) return finish(Abort);
#else
  assert(district != NULL);
#endif
  int64_t o_id;
  district->get_value(D_NEXT_O_ID, o_id);

  uint64_t distinct_count;
  if (!stock_level_getStockCount(arg.w_id, arg.d_id, o_id, arg.w_id,
                                 arg.threshold, &distinct_count))
    return finish(Abort);
  (void)distinct_count;
#endif

  return finish(RCOK);
}
