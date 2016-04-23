#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_mica.h"
#include "catalog.h"
#include "mem_alloc.h"

RC workload::init() {
	sim_done = false;

	mica_sw.init_start();
	mica_sw.init_end();
#if CC_ALG == MICA
	auto config = ::mica::util::Config::load_file("test_tx.json");
	mica_alloc = new MICAAlloc(config.get("alloc"));
	mica_page_pools[0] = new MICAPagePool(mica_alloc, 24 * uint64_t(1073741824) / 2, 0);
	mica_page_pools[1] = new MICAPagePool(mica_alloc, 24 * uint64_t(1073741824) / 2, 1);
	mica_logger = new MICALogger();
	mica_db = new MICADB(mica_page_pools, mica_logger, &mica_sw, THREAD_CNT);
#endif
	return RCOK;
}

RC workload::init_schema(string schema_file) {
#if CC_ALG == MICA
	mica_db->activate(static_cast<uint64_t>(::mica::util::lcore.lcore_id()));
#endif

    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname;
			tname = &line[6];
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), lines.size() );
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
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					default: assert(false);
					}
					elem_num ++;
				}
#if WORKLOAD == YCSB
				size = MAX_TUPLE_SIZE;
#endif
				assert(elem_num == 3);
                schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
				col_count ++;
			}

#if WORKLOAD == YCSB
			assert(schema->get_tuple_size() == MAX_TUPLE_SIZE);
#endif

			table_t * cur_tab = (table_t *) _mm_malloc(sizeof(table_t), CL_SIZE);
			cur_tab->init(schema);
#if CC_ALG == MICA
			printf("creating table %s\n", tname.c_str());
			uint64_t data_size = schema->get_tuple_size();
		  bool ret = mica_db->create_table(tname, data_size);
		  assert(ret);
		  (void)ret;
		  cur_tab->mica_db = mica_db;
		  cur_tab->mica_tbl = mica_db->get_table(tname);
			assert(cur_tab->mica_tbl);
#endif
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
				if (pos == string::npos)
					pos = line.length();
	    		token = line.substr(0, pos);
				items.push_back(token);
		    	line.erase(0, pos + 1);
			}

			string tname(items[0]);
			INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
			new(index) INDEX();
			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM")
				part_cnt = 1;
#if INDEX_STRUCT == IDX_HASH || INDEX_STRUCT == IDX_MICA
	#if WORKLOAD == YCSB
			index->init(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			// index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
			index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt * g_num_wh);
	#endif
#else
			index->init(part_cnt, tables[tname]);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();

#if CC_ALG == MICA
	mica_db->deactivate(static_cast<uint64_t>(::mica::util::lcore.lcore_id()));
#endif
	return RCOK;
}



void workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void workload::index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

    assert( index->index_insert(key, m_item, pid) == RCOK );
}


