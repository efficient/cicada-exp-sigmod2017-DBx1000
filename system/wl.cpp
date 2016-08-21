#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_mica.h"
#include "index_mbtree.h"
#include "catalog.h"
#include "mem_alloc.h"
#include <thread>

RC workload::init() {
  sim_done = false;

  mica_sw.init_start();
  mica_sw.init_end();

  gCPUFreq = mica_sw.c_1_sec() / 1000000000.;
  printf("detected CPU frequency = %.3f GHz\n", gCPUFreq);

#if CC_ALG == MICA
  auto config = ::mica::util::Config::load_file("test_tx.json");
  mica_alloc = new MICAAlloc(config.get("alloc"));
  // auto page_pool_size = 76 * uint64_t(1073741824);
  auto page_pool_size = 95 * uint64_t(1073741824);
  if (g_thread_cnt == 1) {
    mica_page_pools[0] = new MICAPagePool(mica_alloc, page_pool_size / 2, 0);
    mica_page_pools[1] = nullptr;
  } else {
    mica_page_pools[0] = new MICAPagePool(mica_alloc, page_pool_size / 2, 0);
    mica_page_pools[1] = new MICAPagePool(mica_alloc, page_pool_size / 2, 1);
  }
  mica_logger = new MICALogger();
  mica_db = new MICADB(mica_page_pools, mica_logger, &mica_sw, g_thread_cnt);
#endif
  return RCOK;
}

RC workload::init_schema(string schema_file) {
#if CC_ALG == MICA
  // mica_db->activate(static_cast<uint64_t>(::mica::util::lcore.lcore_id()));
  std::vector<std::thread> threads;
  for (uint64_t thread_id = 0; thread_id < g_thread_cnt; thread_id++) {
    threads.emplace_back([&, thread_id] {
      ::mica::util::lcore.pin_thread(thread_id);
      mem_allocator.register_thread(thread_id);
      mica_db->activate(thread_id);
    });
  }
  while (threads.size() > 0) {
    threads.back().join();
    threads.pop_back();
  }
#endif

  assert(sizeof(uint64_t) == 8);
  assert(sizeof(double) == 8);
  string line;
  ifstream fin(schema_file);
  Catalog* schema;
  while (getline(fin, line)) {
    if (line.compare(0, 6, "TABLE=") == 0) {
      string tname;
      tname = &line[6];
      schema = (Catalog*)mem_allocator.alloc(sizeof(Catalog), -1);
      getline(fin, line);
      int col_count = 0;
      // Read all fields for this table.
      vector<string> lines;
      while (line.length() > 1) {
        lines.push_back(line);
        getline(fin, line);
      }
      schema->init(tname.c_str(), lines.size());
      for (UInt32 i = 0; i < lines.size(); i++) {
        string line = lines[i];
        size_t pos = 0;
        string token;
        int elem_num = 0;
        int size = 0;
        string type;
        string name;
        while (line.length() != 0) {
          pos = line.find(",");
          if (pos == string::npos) pos = line.length();
          token = line.substr(0, pos);
          line.erase(0, pos + 1);
          switch (elem_num) {
            case 0:
              size = atoi(token.c_str());
              break;
            case 1:
              type = token;
              break;
            case 2:
              name = token;
              break;
            default:
              assert(false);
          }
          elem_num++;
        }
#if WORKLOAD == YCSB
        size = MAX_TUPLE_SIZE;
#endif
        assert(elem_num == 3);
        schema->add_col((char*)name.c_str(), size, (char*)type.c_str());
        col_count++;
      }

      int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
#if WORKLOAD == TPCC
      if (tname == "ITEM") part_cnt = 1;
#endif

#if WORKLOAD == YCSB
      assert(schema->get_tuple_size() == MAX_TUPLE_SIZE);
#endif

      table_t* cur_tab = (table_t*)mem_allocator.alloc(sizeof(table_t), -1);
			new (cur_tab) table_t;
#if CC_ALG == MICA
      cur_tab->mica_db = mica_db;
#endif
      cur_tab->init(schema, part_cnt);
      assert(schema->get_tuple_size() <= MAX_TUPLE_SIZE);
      tables[tname] = cur_tab;
    } else if (!line.compare(0, 6, "INDEX=")) {
      string iname;
      iname = &line[6];
      getline(fin, line);

      vector<string> items;
      string token;
      size_t pos;
      while (line.length() != 0) {
        pos = line.find(",");
        if (pos == string::npos) pos = line.length();
        token = line.substr(0, pos);
        items.push_back(token);
        line.erase(0, pos + 1);
      }

      string tname(items[0]);

      int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
#if WORKLOAD == TPCC
      if (tname == "ITEM") part_cnt = 1;
#endif

      uint64_t table_size;
#if WORKLOAD == YCSB
      table_size = g_synth_table_size;
#elif WORKLOAD == TPCC
      if (tname == "ITEM")
        table_size = stoi(items[1]);
      else
        table_size = stoi(items[1]) * g_num_wh;
#endif

      if (strncmp(iname.c_str(), "ORDERED_", 8) != 0) {
        INDEX* index = (INDEX*)mem_allocator.alloc(sizeof(INDEX), -1);
        new (index) INDEX();

#if INDEX_STRUCT == IDX_HASH || INDEX_STRUCT == IDX_MICA
        index->init(part_cnt, tables[tname], table_size * 2);
#else
        index->init(part_cnt, tables[tname]);
#endif
        indexes[iname] = index;
      } else {
        ORDERED_INDEX* index =
            (ORDERED_INDEX*)mem_allocator.alloc(sizeof(ORDERED_INDEX), -1);
        new (index) ORDERED_INDEX();

        index->init(part_cnt, tables[tname]);
        ordered_indexes[iname] = index;
      }
    }
  }
  fin.close();

#if CC_ALG == MICA
  // mica_db->deactivate(static_cast<uint64_t>(::mica::util::lcore.lcore_id()));
  for (uint64_t thread_id = 0; thread_id < g_thread_cnt; thread_id++) {
    threads.emplace_back([&, thread_id] {
      ::mica::util::lcore.pin_thread(thread_id);
      mem_allocator.register_thread(thread_id);
      mica_db->deactivate(thread_id);
    });
  }
  while (threads.size() > 0) {
    threads.back().join();
    threads.pop_back();
  }
#endif
  return RCOK;
}

void workload::index_insert(string index_name, uint64_t key, row_t* row) {
  assert(false);
  if (strncmp(index_name.c_str(), "ORDERED_", 8) != 0) {
    INDEX* index = (INDEX*)indexes[index_name];
    index_insert(index, key, row);
  } else {
    ORDERED_INDEX* index = (ORDERED_INDEX*)ordered_indexes[index_name];
    index_insert(index, key, row);
  }
}

template <class INDEX_T>
void workload::index_insert(INDEX_T* index, uint64_t key, row_t* row,
                            int64_t part_id) {
  uint64_t pid = part_id;
  if (part_id == -1) pid = get_part_id(row);
  itemid_t* m_item = (itemid_t*)mem_allocator.alloc(sizeof(itemid_t), pid);
  m_item->init();
  m_item->type = DT_row;
  m_item->location = row;
  m_item->valid = true;

  auto rc = index->index_insert(key, m_item, pid);
  assert(rc == RCOK);

#if INDEX_STRUCT == IDX_MICA
  // We do not need to keep the index item.
  mem_allocator.free(m_item, sizeof(itemid_t));
#endif
}

template void workload::index_insert(index_btree* index, uint64_t key,
                                     row_t* row, int64_t part_id);
template void workload::index_insert(IndexHash* index, uint64_t key, row_t* row,
                                     int64_t part_id);
template void workload::index_insert(IndexMBTree* index, uint64_t key,
                                     row_t* row, int64_t part_id);

#if INDEX_STRUCT == IDX_MICA
template void workload::index_insert(IndexMICA* index, uint64_t key, row_t* row,
                                     int64_t part_id);
template void workload::index_insert(OrderedIndexMICA* index, uint64_t key,
                                     row_t* row, int64_t part_id);
#endif
