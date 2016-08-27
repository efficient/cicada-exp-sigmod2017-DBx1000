#!/usr/bin/python

import os
import sys
import re
import time
import shutil
import subprocess
import pprint


def replace_def(conf, name, value):
  pattern = r'^#define %s\s+.+$' % re.escape(name)
  repl = r'#define %s %s' % (name, value)
  s, n = re.subn(pattern, repl, conf, flags=re.MULTILINE)
  assert n == 1, 'failed to replace def: %s=%s' % (name, value)
  return s


def set_alg(conf, alg, **kwargs):
  conf = replace_def(conf, 'CC_ALG', alg.partition('-')[0].partition('+')[0])
  conf = replace_def(conf, 'ISOLATION_LEVEL', 'SERIALIZABLE')

  if alg == 'SILO':
    conf = replace_def(conf, 'VALIDATION_LOCK', '"waiting"')
    conf = replace_def(conf, 'PRE_ABORT', '"false"')
  else:
    conf = replace_def(conf, 'VALIDATION_LOCK', '"no-wait"')
    conf = replace_def(conf, 'PRE_ABORT', '"true"')

  if alg == 'MICA':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')
  elif alg == 'MICA+INDEX':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')
  elif alg == 'MICA+FULLINDEX':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'true')
  else:
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')

  # conf = replace_def(conf, 'RCU_ALLOC', 'false')
  conf = replace_def(conf, 'RCU_ALLOC', 'true')
  if alg.startswith('MICA'):
      # ~8192 huge pages (16 GiB) for RCU
    conf = replace_def(conf, 'RCU_ALLOC_SIZE', str(int(8192 * 0.99) * 2 * 1048576) + 'UL')
  else:
    conf = replace_def(conf, 'RCU_ALLOC_SIZE', str(int(hugepage_count[alg] * 0.99) * 2 * 1048576) + 'UL')

  return conf


def set_ycsb(conf, thread_count, total_count, record_size, req_per_query, read_ratio, zipf_theta, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'YCSB')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(record_size))
  conf = replace_def(conf, 'INIT_PARALLELISM', str(thread_count))
  conf = replace_def(conf, 'PART_CNT', str(min(2, thread_count))) # try to use both NUMA node, but do not create too many partitions

  conf = replace_def(conf, 'SYNTH_TABLE_SIZE', str(total_count))
  conf = replace_def(conf, 'REQ_PER_QUERY', str(req_per_query))
  conf = replace_def(conf, 'READ_PERC', str(read_ratio))
  conf = replace_def(conf, 'WRITE_PERC', str(1. - read_ratio))
  conf = replace_def(conf, 'SCAN_PERC', 0)
  conf = replace_def(conf, 'ZIPF_THETA', str(zipf_theta))

  return conf


def set_tpcc(conf, thread_count, bench, warehouse_count, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'TPCC')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(704))
  conf = replace_def(conf, 'NUM_WH', str(warehouse_count))
  # INIT_PARALLELISM does not affect tpcc initialization
  conf = replace_def(conf, 'INIT_PARALLELISM', str(warehouse_count))
  conf = replace_def(conf, 'PART_CNT', str(warehouse_count))

  if bench == 'TPCC':
    conf = replace_def(conf, 'TPCC_INSERT_ROWS', 'false')
    conf = replace_def(conf, 'TPCC_DELETE_ROWS', 'false')
    conf = replace_def(conf, 'TPCC_INSERT_INDEX', 'false')
    conf = replace_def(conf, 'TPCC_DELETE_INDEX', 'false')
    conf = replace_def(conf, 'TPCC_FULL', 'false')
  elif bench == 'TPCC-FULL':
    conf = replace_def(conf, 'TPCC_INSERT_ROWS', 'true')
    conf = replace_def(conf, 'TPCC_DELETE_ROWS', 'true')
    conf = replace_def(conf, 'TPCC_INSERT_INDEX', 'true')
    conf = replace_def(conf, 'TPCC_DELETE_INDEX', 'true')
    conf = replace_def(conf, 'TPCC_FULL', 'true')
  else:
    assert False

  return conf


def set_tatp(conf, thread_count, scale_factor, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'TATP')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(67))
  conf = replace_def(conf, 'TATP_SCALE_FACTOR', str(scale_factor))
  conf = replace_def(conf, 'INIT_PARALLELISM', str(thread_count))
  conf = replace_def(conf, 'PART_CNT', str(min(2, thread_count))) # try to use both NUMA node, but do not create too many partitions

  return conf


def set_threads(conf, thread_count, **kwargs):
  return replace_def(conf, 'THREAD_CNT', thread_count)


def set_mica_confs(conf, **kwargs):
  if 'no_tsc' in kwargs:
    conf = replace_def(conf, 'MICA_NO_TSC', 'true')
  if 'no_preval' in kwargs:
    conf = replace_def(conf, 'MICA_NO_PRE_VALIDATION', 'true')
  if 'no_newest' in kwargs:
    conf = replace_def(conf, 'MICA_NO_INSERT_NEWEST_VERSION_ONLY', 'true')
  if 'no_wsort' in kwargs:
    conf = replace_def(conf, 'MICA_NO_SORT_WRITE_SET_BY_CONTENTION', 'true')
  if 'no_tscboost' in kwargs:
    conf = replace_def(conf, 'MICA_NO_STRAGGLER_AVOIDANCE', 'true')
  if 'no_wait' in kwargs:
    conf = replace_def(conf, 'MICA_NO_WAIT_FOR_PENDING', 'true')
  if 'no_inlining' in kwargs:
    conf = replace_def(conf, 'MICA_NO_INLINING', 'true')
  if 'no_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_NO_BACKOFF', 'true')
  if 'fixed_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_USE_FIXED_BACKOFF', 'true')
    conf = replace_def(conf, 'MICA_FIXED_BACKOFF', str(kwargs['fixed_backoff']))
  if 'slow_gc' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SLOW_GC', 'true')
    conf = replace_def(conf, 'MICA_SLOW_GC', str(kwargs['slow_gc']))
  if 'column_count' in kwargs:
    conf = replace_def(conf, 'MICA_COLUMN_COUNT', str(kwargs['column_count']))
  if 'max_scan_len' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SCAN', 'true')
    conf = replace_def(conf, 'MICA_MAX_SCAN_LEN', str(kwargs['max_scan_len']))
  if 'full_table_scan' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SCAN', 'true')
    conf = replace_def(conf, 'MICA_USE_FULL_TABLE_SCAN', 'true')
  return conf


dir_name = None
old_dir_name = None

node_count = None
max_thread_count = None

prefix = ''
suffix = ''
total_seqs = 5

hugepage_count = {
  # 32 GiB
  'SILO-REF': 32 * 1024 / 2,
  'SILO': 32 * 1024 / 2,
  'TICTOC': 32 * 1024 / 2,
  'NO_WAIT': 32 * 1024 / 2,
  # 32 GiB + (16 GiB for RCU)
  'MICA': (32 + 16) * 1024 / 2,
  'MICA+INDEX': (32 + 16) * 1024 / 2,
  # 96 GiB
  'HEKATON': 96 * 1024 / 2,
}

def gen_filename(exp):
  s = ''
  for key in sorted(exp.keys()):
    s += key
    s += '@'
    s += str(exp[key])
    s += '__'
  return prefix + s.rstrip('__') + suffix


def parse_filename(filename):
  assert filename.startswith(prefix)
  assert filename.endswith(suffix)
  d = {}
  filename = filename[len(prefix):]
  if len(suffix) != 0:
    filename = filename[:-len(suffix)]
  for entry in filename.split('__'):
    key, _, value = entry.partition('@')
    if key in ('thread_count', 'total_count', 'record_size', 'req_per_query', 'tx_count', 'seq', 'warehouse_count', 'slow_gc', 'column_count', 'max_scan_len', 'scale_factor'):
      p_value = int(value)
    elif key in ('read_ratio', 'zipf_theta', 'fixed_backoff'):
      p_value = float(value)
    elif key in ('no_tsc', 'no_preval', 'no_newest', 'no_wsort', 'no_tscboost', 'no_wait', 'no_inlining', 'no_backoff', 'full_table_scan'):
      p_value = 1
    elif key in ('bench', 'alg', 'tag'):
      p_value = value
    else: assert False, key
    assert value == str(p_value), key
    d[key] = p_value
  return d


def remove_stale():
  exps = []
  for seq in range(total_seqs):
    exps += list(enum_exps(seq))

  valid_filenames = set([gen_filename(exp) for exp in exps])

  for filename in os.listdir(dir_name):
    if filename.endswith('.old'):
      continue
    if filename.endswith('.failed'):
      continue
    if not (filename.startswith(prefix) and filename.endswith(suffix)):
      continue
    if filename in valid_filenames:
      continue

    if not os.path.exists(old_dir_name):
      os.mkdir(old_dir_name)
    print('stale file: %s' % filename)
    os.rename(dir_name + '/' + filename, old_dir_name + '/' + filename)


def comb_dict(*dicts):
  d = {}
  for dict in dicts:
    d.update(dict)
  return d


def format_exp(exp):
  return pprint.pformat(exp).replace('\n', '')


def enum_exps(seq):
  all_algs = ['MICA', 'MICA+INDEX', #'MICA+FULLINDEX',
              'SILO', 'TICTOC', 'HEKATON', 'NO_WAIT',
              'SILO-REF',
              #'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF',
              ]

  macrobenchs = ['macrobench']
  factors = ['factor']
  # macrobenchs = ['macrobench', 'native-macrobench']
  # factors = ['factor', 'native-factor']

  for tag in macrobenchs:
    for alg in all_algs:
      if tag == 'macrobench' and alg in ('MICA+FULLINDEX',):
      # if tag == 'macrobench' and alg in ('MICA+INDEX', 'MICA+FULLINDEX'):
        continue
      if tag == 'native-macrobench' and alg not in ('MICA', 'MICA+INDEX', 'MICA+FULLINDEX'):
        continue

      for thread_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # YCSB
        if alg not in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
          ycsb = dict(common)
          total_count = 10 * 1000 * 1000
          ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

          record_size = 1000
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            # for zipf_theta in [0.00, 0.90, 0.99]:
            for zipf_theta in [0.00, 0.99]:
              if zipf_theta >= 0.95:
                if read_ratio == 0.50 and alg == 'NO_WAIT': continue
                if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          record_size = 1000
          req_per_query = 1
          tx_count = 2000000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            # for zipf_theta in [0.00, 0.90, 0.99]:
            for zipf_theta in [0.00, 0.99]:
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

        # TPCC
        if alg not in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

          # for warehouse_count in [1, 4, 16, max_thread_count]:
          for warehouse_count in [1, 4, max_thread_count]:
            if tag != 'macrobench': continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

          for warehouse_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
            if tag != 'macrobench': continue
            if thread_count not in [max_thread_count, warehouse_count]: continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        # full TPCC
        # if alg in ('MICA', 'MICA+INDEX', 'MICA+FULLINDEX'):
        #if alg in ('MICA+INDEX', 'MICA+FULLINDEX', 'SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
        # if True:
        if alg not in ('MICA',):  # MICA must use the native index
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC-FULL', 'tx_count': tx_count })

          # for warehouse_count in [1, 4, 16, max_thread_count]:
          for warehouse_count in [1, 4, max_thread_count]:
            if tag != 'macrobench': continue
            # if alg in ('ERMIA-SI-REF', 'ERMIA-SI_SSN-REF') and warehouse_count < thread_count:
            #   # Seem to be broken in ERMIA
            #   continue;
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

          for warehouse_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
            if tag != 'macrobench': continue
            if thread_count not in [max_thread_count, warehouse_count]: continue
            # if alg in ('ERMIA-SI-REF', 'ERMIA-SI_SSN-REF') and warehouse_count < thread_count:
            #   # Seem to be broken in ERMIA
            #   continue;
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        # TATP
        if alg not in ('MICA', 'SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
          tatp = dict(common)
          tx_count = 200000
          tatp.update({ 'bench': 'TATP', 'tx_count': tx_count })

          # for scale_factor in [1, 2, 5, 10, 20, 50, 100]:
          # for scale_factor in [1, 10]:
          for scale_factor in [1]:
            if tag != 'macrobench': continue
            tatp.update({ 'scale_factor': scale_factor })
            yield dict(tatp)

      for thread_count in [max_thread_count]:
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # YCSB
        if alg not in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
          ycsb = dict(common)
          total_count = 10 * 1000 * 1000
          ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

          record_size = 1000
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            for zipf_theta in [0.00, 0.40, 0.60, 0.80, 0.90, 0.95, 0.99]:
              if zipf_theta >= 0.95:
                if read_ratio == 0.50 and alg == 'NO_WAIT': continue
                if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

  tag = 'inlining'
  for alg in all_algs:
  # for alg in ['MICA', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
    for thread_count in [max_thread_count]:
      if alg not in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # YCSB
        ycsb = dict(common)
        total_count = 10 * 1000 * 1000
        ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

        for record_size in [10, 20, 40, 100, 200, 400, 1000, 2000]:
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          read_ratio = 0.95
          zipf_theta = 0.00
          ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
          yield dict(ycsb)

          if alg in ['MICA', 'MICA+INDEX']:
            ycsb.update({ 'no_inlining': 1 })
            yield dict(ycsb)
            del ycsb['no_inlining']

  tag = 'singlekey'
  # for alg in all_algs:
  # others disabled because Silo/TicToc makes too much skewed throughput across threads
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in []:
    for thread_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 1
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      record_size = 16
      req_per_query = 1
      if thread_count <= 1:
        tx_count = 8000000
      elif thread_count <= 4:
        tx_count = 4000000
      else:
        tx_count = 2000000

      ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

      read_ratio = 0.00
      zipf_theta = 0.00
      ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      yield dict(ycsb)

  def _common_exps(common):
    if common['tag'] in ('backoff', 'factor', 'native-factor'):
      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      record_size = 1000
      req_per_query = 16
      tx_count = 200000
      ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

      # if common['tag'] == 'backoff':
      #   # for read_ratio in [0.50, 0.95]:
      #   # for zipf_theta in [0.00, 0.99]:
      #   read_ratio = 0.50
      #   zipf_theta = 0.90
      #   ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      #   yield dict(ycsb)

      read_ratio = 0.50
      zipf_theta = 0.99
      ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      yield dict(ycsb)

      if common['tag'] == 'backoff':
        record_size = 1000
        req_per_query = 1
        tx_count = 2000000
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        # for read_ratio in [0.50, 0.95]:
        # for zipf_theta in [0.00, 0.99]:
        read_ratio = 0.50
        zipf_theta = 0.99
        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

      if common['tag'] in ('factor', 'native-factor'):
        record_size = 1000
        req_per_query = 1
        tx_count = 2000000
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        # for read_ratio in [0.50, 0.95]:
        # for zipf_theta in [0.00, 0.99]:
        # read_ratio = 0.95
        # zipf_theta = 0.00
        # ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        # yield dict(ycsb)

        read_ratio = 0.95
        zipf_theta = 0.99
        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

    if common['tag'] in ('gc', 'backoff', 'factor'):
      # TPCC
      tpcc = dict(common)
      tx_count = 200000
      tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

      warehouse_count = 4
      tpcc.update({ 'warehouse_count': warehouse_count })
      yield dict(tpcc)

      # if common['tag'] == 'gc':
      #   warehouse_count = 1
      #   tpcc.update({ 'warehouse_count': warehouse_count })
      #   yield dict(tpcc)

      # if common['tag'] in ('gc', 'factor'):
      if common['tag'] in ('gc',):
        warehouse_count = max_thread_count
        tpcc.update({ 'warehouse_count': warehouse_count })
        yield dict(tpcc)


  tag = 'backoff'
  # for alg in ['MICA', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in ['MICA+INDEX']:
    thread_count = max_thread_count
    for backoff in [round(1.25 ** v - 1.0, 2) for v in range(24)]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'fixed_backoff': backoff }

      for exp in _common_exps(common): yield exp


  for tag in factors:
    # for alg in ['MICA', 'MICA+INDEX']:
    for alg in ['MICA+INDEX']:
      thread_count = max_thread_count
      for i in range(7):
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # if i >= 1: common['no_wsort'] = 1
        # if i >= 2: common['no_preval'] = 1
        # if i >= 3: common['no_newest'] = 1
        # if i >= 4: common['no_wait'] = 1
        # if i >= 5: common['no_tscboost'] = 1
        # if i >= 6: common['no_tsc'] = 1
        #
        # for exp in _common_exps(common): yield exp

        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        if i == 1: common['no_wsort'] = 1
        if i == 2: common['no_preval'] = 1
        if i == 3: common['no_newest'] = 1
        if i == 4: common['no_wait'] = 1
        if i == 5: common['no_tscboost'] = 1
        if i == 6: common['no_tsc'] = 1

        for exp in _common_exps(common): yield exp


  tag = 'gc'
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in ['MICA+INDEX']:
    thread_count = max_thread_count
    for slow_gc in [1, 2, 4,
                    10, 20, 40,
                    100, 200, 400,
                    1000, 2000, 4000,
                    10000, 20000, 40000,
                    100000]:

      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'slow_gc': slow_gc }

      for exp in _common_exps(common): yield exp


  tag = 'native-scan'
  for alg in ['MICA+INDEX']:
    for thread_count in [max_thread_count]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      for max_scan_len in [100]:
        for record_size in [10, 100, 1000]:
          req_per_query = 1
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          ycsb.update({ 'max_scan_len': max_scan_len })
          if record_size in [10, 100]:
            ycsb.update({ 'column_count': 1 })

          read_ratio = 0.95
          zipf_theta = 0.99

          ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
          yield dict(ycsb)

          ycsb.update({ 'no_inlining': 1 })
          yield dict(ycsb)
          del ycsb['no_inlining']

          if record_size in [10, 100]:
            del ycsb['column_count']


  tag = 'native-full-table-scan'
  for alg in ['MICA+INDEX']:
    for thread_count in [max_thread_count]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      for record_size in [10, 100, 1000]:
        req_per_query = 1
        tx_count = 20
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        ycsb.update({ 'full_table_scan': 1 })
        if record_size in [10, 100]:
          ycsb.update({ 'column_count': 1 })

        read_ratio = 0.95
        zipf_theta = 0.99

        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

        ycsb.update({ 'no_inlining': 1 })
        yield dict(ycsb)
        del ycsb['no_inlining']

        if record_size in [10, 100]:
          del ycsb['column_count']


def update_conf(conf, exp):
  conf = set_alg(conf, **exp)
  conf = set_threads(conf, **exp)
  if exp['bench'] == 'YCSB':
    conf = set_ycsb(conf, **exp)
  elif exp['bench'] in ('TPCC', 'TPCC-FULL'):
    conf = set_tpcc(conf, **exp)
  elif exp['bench'] == 'TATP':
    conf = set_tatp(conf, **exp)
  else: assert False
  if exp['alg'].startswith('MICA') or exp['tag'] == 'backoff':
    conf = set_mica_confs(conf, **exp)
  return conf


def sort_exps(exps):
  def _exp_pri(exp):
    pri = 0

    # prefer microbench
    if exp['tag'] == 'gc': pri -= 2
    if exp['tag'] == 'backoff': pri -= 2
    # then macrobench
    if exp['tag'] == 'macrobench': pri -= 1
    # factor analysis is not prioritized

    # prefer fast schemes
    if exp['alg'] == 'MICA': pri -= 2
    if exp['alg'] == 'MICA+INDEX': pri -= 2
    # if exp['alg'].startswith('MICA'): pri -= 2
    if exp['alg'] == 'SILO' or exp['alg'] == 'TICTOC': pri -= 1

    # prefer max cores
    if exp['thread_count'] in (max_thread_count, max_thread_count * 2): pri -= 1

    # prefer write-intensive workloads
    if exp['bench'] == 'YCSB' and exp['read_ratio'] == 0.50: pri -= 1
    # prefer standard skew
    if exp['bench'] == 'YCSB' and exp['zipf_theta'] in (0.00, 0.90, 0.99): pri -= 1

    # prefer (warehouse count) = (thread count)
    if exp['bench'] == 'TPCC' and exp['thread_count'] == exp['warehouse_count']: pri -= 1
    # prefer standard warehouse counts
    if exp['bench'] == 'TPCC' and exp['warehouse_count'] in (1, 4, 16, max_thread_count, max_thread_count * 2): pri -= 1

    # run exps in their sequence number
    return (exp['seq'], pri)

  exps = list(exps)
  exps.sort(key=_exp_pri)
  return exps


def unique_exps(exps):
  l = []
  for exp in exps:
    if exp in l: continue
    assert exp['alg'] in hugepage_count
    l.append(exp)
  return l

def skip_done(exps):
  for exp in exps:
    if os.path.exists(dir_name + '/' + gen_filename(exp)): continue
    if os.path.exists(dir_name + '/' + gen_filename(exp) + '.failed'): continue
    # if exp['alg'] == 'MICA': continue
    yield exp

def find_exps_to_run(exps, pats):
  for exp in exps:
    if pats:
      for pat in pats:
        key, _, value = pat.partition('@')
        if key not in exp or str(exp[key]) != value:
          break
      else:
        yield exp
    else:
      yield exp


def validate_result(exp, output):
  if exp['alg'] in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
    return output.find('txn breakdown: ') != -1
  elif not exp['tag'].startswith('native-'):
    return output.find('[summary] tput=') != -1
  else:
    return output.find('cleaning up') != -1


def make_silo_cmd(exp):
  cmd = 'silo/out-perf.masstree/benchmarks/dbtest'
  cmd += ' --verbose'
  cmd += ' --parallel-loading'
  cmd += ' --pin-cpus'
  cmd += ' --retry-aborted-transactions'
  # cmd += ' --backoff-aborted-transactions'  # Better for 1 warehouse (> 1000 Tps), worse for 4+ warehouses for TPC-C
  cmd += ' --bench tpcc'
  cmd += ' --scale-factor %d' % exp['warehouse_count']
  cmd += ' --num-threads %d' % exp['thread_count']
  # cmd += ' --ops-per-worker %d' % exp['tx_count']
  cmd += ' --runtime 30'
  cmd += ' --bench-opts="--enable-separate-tree-per-partition"'
  cmd += ' --numa-memory %dG' % int(int(hugepage_count[exp['alg']] * 0.99) * 2 / 1024)
  return cmd

def make_ermia_cmd(exp):
  tmpfs_dir = '/tmp'
  log_dir = '/tmp/ermia-log'
  if os.path.exists(log_dir):
    shutil.rmtree(log_dir)
  os.mkdir(log_dir)

   # --parallel-loading seems to be broken
   # --ops-per-worker is somehow very slow

  if exp['alg'] == 'ERMIA-SI-REF':
    cmd = 'ermia/dbtest-SI'
  elif exp['alg'] == 'ERMIA-SI_SSN-REF':
    cmd = 'ermia/dbtest-SI_SSN'
  else: assert False
  cmd += ' --verbose'
  # cmd += ' --parallel-loading'  # Broken in the current code
  cmd += ' --retry-aborted-transactions'
  # cmd += ' --backoff-aborted-transactions'  # For consistency with SILO-REF
  cmd += ' --bench tpcc'
  cmd += ' --scale-factor %d' % exp['warehouse_count']
  cmd += ' --num-threads %d' % exp['thread_count']
  # cmd += ' --ops-per-worker %d' % exp['tx_count']
  cmd += ' --runtime 30'
  # cmd += ' --bench-opts="--enable-separate-tree-per-partition"' # Unstable/do not finish
  cmd += ' --node-memory-gb %d' % int(hugepage_count[exp['alg']] * 2 / 1024 / node_count * 0.9) # reduce it slightly because it often gets stuck if this is too tight to the available hugepages (competing with jemalloc?)
  cmd += ' --enable-gc'
  cmd += ' --tmpfs-dir %s' % tmpfs_dir
  cmd += ' --log-dir %s' % log_dir
  cmd += ' --log-buffer-mb 512'
  cmd += ' --log-segment-mb 8192'
  cmd += ' --null-log-device'
  return cmd


hugepage_status = None

def run(exp, prepare_only):
  global hugepage_status

  # update config
  if not exp['tag'].startswith('native-'):
    conf = open('config-std.h').read()
    conf = update_conf(conf, exp)
    open('config.h', 'w').write(conf)

    shutil.copy('../src/mica/test/test_tx_conf_org.h',
                '../src/mica/test/test_tx_conf.h')
  else:
    shutil.copy('config-std.h', 'config.h')

    conf = open('../src/mica/test/test_tx_conf_org.h').read()
    conf = update_conf(conf, exp)
    open('../src/mica/test/test_tx_conf.h', 'w').write(conf)

  # clean up
  os.system('make clean -j > /dev/null')
  os.system('rm -f ./rundb')

  # if exp['alg'].startswith('MICA') or exp['alg'] in ('ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
  # if True:
  if hugepage_status != (hugepage_count[exp['alg']], exp['alg']):
    os.system('../script/setup.sh %d %d > /dev/null' % \
      (hugepage_count[exp['alg']] / 2, hugepage_count[exp['alg']] / 2))
    hugepage_status = (hugepage_count[exp['alg']], exp['alg'])
  # else:
  #   if hugepage_status != (0, ''):
  #     os.system('../script/setup.sh 0 0 > /dev/null')
  #     hugepage_status = (0, '')

  # os.system('sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"')
  os.system('sudo bash -c "echo always > /sys/kernel/mm/transparent_hugepage/enabled"')
  os.system('sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/defrag"')

  # cmd
  filename = dir_name + '/' + gen_filename(exp)

  if exp['alg'] == 'SILO-REF':
    assert exp['bench'] == 'TPCC-FULL'
    cmd = make_silo_cmd(exp)
  elif exp['alg'] in ('ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
    assert exp['bench'] == 'TPCC-FULL'
    cmd = make_ermia_cmd(exp)
  elif not exp['tag'].startswith('native-'):
    # cmd = 'sudo ./rundb | tee %s' % (filename + '.tmp')
    cmd = 'sudo ./rundb'
  else:
    cmd = 'sudo ../build/test_tx 0 0 0 0 0 0'

  print('  ' + cmd)


  if prepare_only: return

  # compile
  if exp['alg'] in ('SILO-REF', 'ERMIA-SI-REF', 'ERMIA-SI_SSN-REF'):
    ret = 0
  elif not exp['tag'].startswith('native-'):
    ret = os.system('make -j > /dev/null')
  else:
    pdir = os.getcwd()
    os.chdir('../build')
    ret = os.system('make -j > /dev/null')
    os.chdir(pdir)
  assert ret == 0, 'failed to compile for %s' % exp
  os.system('date')
  os.system('sudo sync')
  os.system('sudo sync')
  time.sleep(1)

  # run
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = p.communicate()
  stdout = stdout.decode('utf-8')
  stderr = stderr.decode('utf-8')
  output = stdout + '\n\n' + stderr
  if p.returncode != 0:
    print('failed to run exp for %s' % format_exp(exp))
    open(filename + '.failed', 'w').write(output)
    return
  if not validate_result(exp, output):
    print('validation failed for %s' % format_exp(exp))
    open(filename + '.failed', 'w').write(output)
    return

  # finalize
  open(filename, 'w').write(output)

def run_all(pats, prepare_only):
  exps = []
  for seq in range(total_seqs):
    exps += list(enum_exps(seq))
  exps = list(unique_exps(exps))

  total_count = len(exps)
  print('total %d exps' % total_count)

  count_per_tag = {}
  for exp in exps:
    count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
  for tag in sorted(count_per_tag.keys()):
    print('  %s: %d' % (tag, count_per_tag[tag]))
  print('')

  if not prepare_only:
    exps = list(skip_done(exps))
  exps = list(find_exps_to_run(exps, pats))
  skip_count = total_count - len(exps)
  print('%d exps skipped' % skip_count)
  print('')

  exps = list(sort_exps(exps))
  print('total %d exps to run' % len(exps))

  count_per_tag = {}
  for exp in exps:
    count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
  for tag in sorted(count_per_tag.keys()):
    print('  %s: %d' % (tag, count_per_tag[tag]))
  print('')

  first = time.time()

  for i, exp in enumerate(exps):
    start = time.time()
    print('exp %d/%d: %s' % (i + 1, len(exps), format_exp(exp)))

    run(exp, prepare_only)
    if prepare_only: break

    now = time.time()
    print('elapsed = %.2f seconds' % (now - start))
    print('remaining = %.2f hours' % ((now - first) / (i + 1) * (len(exps) - i - 1) / 3600))
    print('')


def update_filenames():
  for filename in os.listdir(dir_name):
    if not filename.startswith(prefix): continue
    if not filename.endswith(suffix): continue
    exp = parse_filename(filename)
    exp['tag'] = 'macrobench'
    new_filename = gen_filename(exp)
    print(filename, ' => ', new_filename)
    os.rename(dir_name + '/' + filename, dir_name + '/' + new_filename)
  sys.exit(0)


def detect_core_count():
  global node_count
  global max_thread_count

  # simple core count detection
  core_id = None
  node_id = None
  max_core_id = -1
  max_node_id = -1

  for line in open('/proc/cpuinfo').readlines():
    line = line.strip()
    if line.startswith('processor'):
      core_id = int(line.partition(':')[2])
    elif line.startswith('physical id'):
      node_id = int(line.partition(':')[2])
    elif line == '':
      max_core_id = max(max_core_id, core_id)
      max_node_id = max(max_node_id, node_id)
      core_id = None
      node_id = None

  node_count = max_node_id + 1
  max_thread_count = max_core_id + 1
  max_thread_count = int(max_thread_count / 2)   # hyperthreading
  # print('total %d nodes, %d cores' % (node_count, max_thread_count))


if __name__ == '__main__':
  argv = list(sys.argv)
  if len(argv) < 3:
    print('%s dir_name [RUN | RUN patterns | PREPARE patterns]' % argv[0])
    sys.exit(1)

  detect_core_count()

  dir_name = argv[1]
  old_dir_name = 'old_' + dir_name

  argv = argv[2:]

  if not os.path.exists(dir_name):
    os.mkdir(dir_name)

  remove_stale()
  # update_filenames()

  if argv[0].upper() == 'RUN':
    if len(argv) == 1:
      run_all(None, False)
    else:
      run_all(argv[1].split('__'), False)
  elif argv[0].upper() == 'PREPARE':
    run_all(argv[1].split('__'), True)
  else:
    assert False
