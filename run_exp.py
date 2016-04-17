#!/usr/bin/python

import os
import sys
import re
import time
import shutil
import subprocess


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

  return conf


def set_ycsb(conf, total_count, record_size, req_per_query, read_ratio, zipf_theta, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'YCSB')
  conf = replace_def(conf, 'WARMUP', str(tx_count))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'INIT_PARALLELISM', '2')
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(record_size))

  conf = replace_def(conf, 'SYNTH_TABLE_SIZE', str(total_count))
  conf = replace_def(conf, 'REQ_PER_QUERY', str(req_per_query))
  conf = replace_def(conf, 'READ_PERC', str(read_ratio))
  conf = replace_def(conf, 'WRITE_PERC', str(1. - read_ratio))
  conf = replace_def(conf, 'SCAN_PERC', 0)
  conf = replace_def(conf, 'ZIPF_THETA', str(zipf_theta))

  return conf


def set_tpcc(conf, warehouse_count, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'TPCC')
  conf = replace_def(conf, 'WARMUP', str(tx_count))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(704))

  conf = replace_def(conf, 'NUM_WH', str(warehouse_count))

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
  if 'no_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_NO_BACKOFF', 'true')
  if 'fixed_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_USE_FIXED_BACKOFF', 'true')
    conf = replace_def(conf, 'MICA_FIXED_BACKOFF', str(kwargs['fixed_backoff']))
  if 'slow_gc' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SLOW_GC', 'true')
    conf = replace_def(conf, 'MICA_SLOW_GC', str(kwargs['slow_gc']))
  return conf


dir_name = 'exp_data'
old_dir_name = 'old_exp_data'
prefix = ''
suffix = ''
total_seqs = 5

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
    if key in ('thread_count', 'total_count', 'record_size', 'req_per_query', 'tx_count', 'seq', 'warehouse_count', 'slow_gc'):
      p_value = int(value)
    elif key in ('read_ratio', 'zipf_theta', 'fixed_backoff'):
      p_value = float(value)
    elif key in ('no_tsc', 'no_preval', 'no_newest', 'no_wsort', 'no_tscboost', 'no_wait', 'no_backoff'):
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
    print('stale file: %s' % filename)
    os.rename(dir_name + '/' + filename, old_dir_name + '/' + filename)


def comb_dict(*dicts):
  d = {}
  for dict in dicts:
    d.update(dict)
  return d


def enum_exps(seq):
  all_algs = ['MICA', 'MICA+INDEX', 'MICA+FULLINDEX',
              'SILO', 'TICTOC', 'HEKATON', 'NO_WAIT']

  # for tag in ['macrobench', 'native-macrobench']:
  for tag in ['macrobench']:
      for alg in all_algs:
        if tag == 'macrobench' and alg in ('MICA+INDEX', 'MICA+FULLINDEX'):
          continue
        if tag == 'native-macrobench' and alg not in ('MICA', 'MICA+INDEX', 'MICA+FULLINDEX'):
          continue

        for thread_count in [1, 2, 4, 8, 12, 16, 20, 24, 28]:
          common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

          # YCSB
          ycsb = dict(common)
          total_count = 10 * 1000 * 1000
          ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

          record_size = 1000
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            for zipf_theta in [0.00, 0.90, 0.99]:
              if zipf_theta >= 0.95:
                if alg == 'NO_WAIT': continue
                if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          record_size = 1000
          req_per_query = 1
          tx_count = 2000000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            for zipf_theta in [0.00, 0.90, 0.99]:
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          # TPCC
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

          for warehouse_count in [1, 2, 4, 8, 12, 16, 20, 24, 28]:
            if tag != 'macrobench': continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        for thread_count in [28]:
          common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

          # YCSB
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
                if alg == 'NO_WAIT': continue
                if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          for record_size in [10, 20, 40, 100, 200, 400, 1000, 2000]:
            if alg not in ['MICA', 'SILO', 'TICTOC']: continue
            req_per_query = 16
            tx_count = 200000
            ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

            read_ratio = 0.95
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

      # for read_ratio in [0.50, 0.95]:
      # for zipf_theta in [0.00, 0.99]:
      read_ratio = 0.50
      zipf_theta = 0.90
      ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      yield dict(ycsb)

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
        read_ratio = 0.95
        zipf_theta = 0.00
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

      if common['tag'] == 'gc':
        warehouse_count = 1
        tpcc.update({ 'warehouse_count': warehouse_count })
        yield dict(tpcc)

        warehouse_count = 28
        tpcc.update({ 'warehouse_count': warehouse_count })
        yield dict(tpcc)


  tag = 'backoff'
  for alg in ['MICA', 'SILO', 'TICTOC']:
    thread_count = 28
    for backoff in [round(1.25 ** v - 1.0, 2) for v in range(20)]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'fixed_backoff': backoff }

      for exp in _common_exps(common): yield exp


  # for tag in ['factor', 'native-factor']:
  for tag in ['factor']:
    alg = 'MICA'
    thread_count = 28
    for i in range(7):
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      if i >= 1: common['no_wsort'] = 1
      if i >= 2: common['no_preval'] = 1
      if i >= 3: common['no_newest'] = 1
      if i >= 4: common['no_wait'] = 1
      if i >= 5: common['no_tscboost'] = 1
      if i >= 6: common['no_tsc'] = 1

      for exp in _common_exps(common): yield exp

      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      if i == 1: common['no_wsort'] = 1
      if i == 2: common['no_preval'] = 1
      if i == 3: common['no_newest'] = 1
      if i == 4: common['no_wait'] = 1
      if i == 5: common['no_tscboost'] = 1
      if i == 6: common['no_tsc'] = 1

      for exp in _common_exps(common): yield exp


  tag = 'gc'
  alg = 'MICA'
  thread_count = 28
  for slow_gc in [1, 2, 4,
                  10, 20, 40,
                  100, 200, 400,
                  1000, 2000, 4000,
                  10000, 20000, 40000,
                  100000]:

    common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'slow_gc': slow_gc }

    for exp in _common_exps(common): yield exp


def update_conf(conf, exp):
  conf = set_alg(conf, **exp)
  conf = set_threads(conf, **exp)
  if exp['bench'] == 'YCSB':
    conf = set_ycsb(conf, **exp)
  elif exp['bench'] == 'TPCC':
    conf = set_tpcc(conf, **exp)
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
    # if exp['alg'].startswith('MICA'): pri -= 2
    if exp['alg'] == 'SILO' or exp['alg'] == 'TICTOC': pri -= 1

    # prefer max cores
    if exp['thread_count'] in (28, 56): pri -= 1

    # prefer write-intensive workloads
    if exp['bench'] == 'YCSB' and exp['read_ratio'] == 0.50: pri -= 1
    # prefer standard skew
    if exp['bench'] == 'YCSB' and exp['zipf_theta'] in (0.00, 0.90, 0.99): pri -= 1

    # prefer (warehouse count) = (thread count)
    if exp['bench'] == 'TPCC' and exp['thread_count'] == exp['warehouse_count']: pri -= 1
    # prefer standard warehouse counts
    if exp['bench'] == 'TPCC' and exp['warehouse_count'] in (1, 4, 16, 28, 56): pri -= 1

    # run exps in their sequence number
    return (exp['seq'], pri)

  exps = list(exps)
  exps.sort(key=_exp_pri)
  return exps


def unique_exps(exps):
  l = []
  for exp in exps:
    if exp in l: continue
    l.append(exp)
  return l

def skip_done(exps):
  for exp in exps:
    if os.path.exists(dir_name + '/' + gen_filename(exp)): continue
    if os.path.exists(dir_name + '/' + gen_filename(exp) + '.failed'): continue
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
  if not exp['tag'].startswith('native-'):
    return output.find('[summary] tput=') != -1
  else:
    return output.find('cleaning up') != -1


hugepage_status = -1

def run(exp, prepare_only):
  global hugepage_status

  # update config
  if not exp['tag'].startswith('native-'):
    conf = open('config-std.h').read()
    conf = update_conf(conf, exp)
    open('config.h', 'w').write(conf)
  else:
    conf = open('../src/mica/test/test_tx_conf_org.h').read()
    conf = update_conf(conf, exp)
    open('../src/mica/test/test_tx_conf.h', 'w').write(conf)

  # clean up
  os.system('make clean > /dev/null')
  os.system('rm -f ./rundb')

  if exp['alg'].startswith('MICA'):
    if hugepage_status != 16384:
      os.system('../script/setup.sh 16384 16384 > /dev/null')   # 64 GiB
      hugepage_status = 16384
  else:
    if hugepage_status != 0:
      os.system('../script/setup.sh 0 0 > /dev/null')
      hugepage_status = 0

  if prepare_only: return

  # compile
  if not exp['tag'].startswith('native-'):
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
  filename = dir_name + '/' + gen_filename(exp)

  if not exp['tag'].startswith('native-'):
    # cmd = 'sudo ./rundb | tee %s' % (filename + '.tmp')
    cmd = 'sudo ./rundb'
  else:
    cmd = 'sudo ../build/test_tx 0 0 0 0 0 0'
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = p.communicate()
  stdout = stdout.decode('utf-8')
  stderr = stderr.decode('utf-8')
  if p.returncode != 0:
    print('failed to run exp for %s' % exp)
    open(filename + '.failed', 'w').write(stdout + '\n' + stderr)
    return
  if not validate_result(exp, stdout):
    print('validation failed for %s' % exp)
    open(filename + '.failed', 'w').write(stdout + '\n' + stderr)
    return

  # finalize
  open(filename, 'w').write(stdout)

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
    print('exp %d/%d: %s' % (i + 1, len(exps), exp))

    run(exp, prepare_only)

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


if __name__ == '__main__':
  if not os.path.exists(dir_name):
    os.mkdir(dir_name)
  if not os.path.exists(old_dir_name):
    os.mkdir(old_dir_name)

  remove_stale()
  # update_filenames()

  if len(sys.argv) == 1:
    print('%s [RUN | RUN patterns | PREPARE patterns]' % sys.argv[0])
    sys.exit(1)

  if sys.argv[1].upper() == 'RUN':
    if len(sys.argv) == 2:
      run_all(None, False)
    else:
      run_all(sys.argv[2].split('__'), False)
  elif sys.argv[1].upper() == 'PREPARE':
    run_all(sys.argv[2].split('__'), True)
  else:
    assert False

