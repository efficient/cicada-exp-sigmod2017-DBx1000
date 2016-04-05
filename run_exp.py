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


def set_alg(conf, alg):
  conf = replace_def(conf, 'CC_ALG', alg.partition('-')[0])
  conf = replace_def(conf, 'ISOLATION_LEVEL', 'SERIALIZABLE')

  if alg == 'SILO-ORG':
    conf = replace_def(conf, 'VALIDATION_LOCK', '"waiting"')
    conf = replace_def(conf, 'PRE_ABORT', '"false"')
  else:
    conf = replace_def(conf, 'VALIDATION_LOCK', '"no-wait"')
    conf = replace_def(conf, 'PRE_ABORT', '"true"')

  if alg == 'MICA-SIMPLE':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_NOINLINE', 'true')
  elif alg == 'MICA-NOINDEX':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_NOINLINE', 'false')
  elif alg == 'MICA-NOINLINE':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_NOINLINE', 'true')
  elif alg == 'MICA-FULL' or alg == 'MICA-TEST':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_NOINLINE', 'false')
  else:
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')

  return conf


def set_ycsb(conf, total_count, req_per_query, read_ratio, zipf_theta):
  conf = replace_def(conf, 'WORKLOAD', 'YCSB')
  if req_per_query <= 2:
    conf = replace_def(conf, 'WARMUP', '1000000')
    conf = replace_def(conf, 'MAX_TXN_PER_PART', '1000000')
  else:
    conf = replace_def(conf, 'WARMUP', '100000')
    conf = replace_def(conf, 'MAX_TXN_PER_PART', '100000')
  conf = replace_def(conf, 'INIT_PARALLELISM', '2')
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(100))

  conf = replace_def(conf, 'SYNTH_TABLE_SIZE', str(total_count))
  conf = replace_def(conf, 'REQ_PER_QUERY', str(req_per_query))
  conf = replace_def(conf, 'READ_PERC', str(read_ratio))
  conf = replace_def(conf, 'WRITE_PERC', str(1. - read_ratio))
  conf = replace_def(conf, 'SCAN_PERC', 0)
  conf = replace_def(conf, 'ZIPF_THETA', str(zipf_theta))

  return conf


def set_tpcc(conf, warehouse_count):
  conf = replace_def(conf, 'WORKLOAD', 'TPCC')
  conf = replace_def(conf, 'WARMUP', '100000')
  conf = replace_def(conf, 'MAX_TXN_PER_PART', '100000')
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(704))

  conf = replace_def(conf, 'NUM_WH', str(warehouse_count))

  return conf


def set_threads(conf, thread_count):
  return replace_def(conf, 'THREAD_CNT', thread_count)


prefix = 'output_'
suffix = '.txt'

def gen_filename(exp):
  s = ''
  for field in exp:
    s += str(field).replace('_', '-') + '_'
  return prefix + s.rstrip('_') + suffix


def parse_filename(filename):
  assert filename.startswith(prefix)
  assert filename.endswith(suffix)
  return filename[len(prefix):-len(suffix)].split('_')


def remove_stale():
  valid_filenames = set([gen_filename(exp) for exp in enum_exps()])

  for filename in os.listdir('.'):
    if not (filename.startswith(prefix) and filename.endswith(suffix)):
      continue
    if filename in valid_filenames:
      continue
    print('stale file: %s' % filename)
    os.rename(filename, filename + '.old')


def enum_exps():
  thread_counts = [1, 4, 8, 16, 28, 42, 56]
  warehouse_counts = [1, 4, 8, 16, 28, 42, 56]
  # all_algs = ['MICA-SIMPLE', 'MICA-NOINDEX', 'MICA-NOINLINE', 'MICA-FULL',
  #             'SILO-ORG', 'TICTOC', 'HEKATON', 'NO_WAIT']
  all_algs = ['MICA-SIMPLE', 'MICA-NOINDEX',
              'SILO-ORG', 'TICTOC', 'HEKATON', 'NO_WAIT']
  # all_algs.append('MICA-TEST')
  total_seqs = 3

  for seq in range(total_seqs):
    for alg in all_algs:
      for thread_count in thread_counts:
        # YCSB: total_count, req_per_query, read_ratio, zipf_theta
        total_count = 10 * 1000 * 1000
        req_per_query = 16
        common = [alg, thread_count, 'YCSB', total_count, req_per_query]
        yield common + [0.95, 0.00] + [seq]
        yield common + [0.50, 0.00] + [seq]
        if alg not in ('NO_WAIT',):
          yield common + [0.95, 0.99] + [seq]
        if alg not in ('HEKATON', 'NO_WAIT'):
          yield common + [0.50, 0.99] + [seq]

        if thread_count in (28, 56):
          yield common + [0.95, 0.40] + [seq]
          yield common + [0.50, 0.40] + [seq]
          yield common + [0.95, 0.60] + [seq]
          yield common + [0.50, 0.60] + [seq]
          yield common + [0.95, 0.80] + [seq]
          yield common + [0.50, 0.80] + [seq]
          yield common + [0.95, 0.90] + [seq]
          yield common + [0.50, 0.90] + [seq]
          if alg not in ('NO_WAIT',):
            yield common + [0.95, 0.95] + [seq]
          if alg not in ('HEKATON', 'NO_WAIT'):
            yield common + [0.50, 0.95] + [seq]

        req_per_query = 1
        common = [alg, thread_count, 'YCSB', total_count, req_per_query]
        yield common + [0.95, 0.00] + [seq]
        yield common + [0.50, 0.00] + [seq]
        yield common + [0.95, 0.99] + [seq]
        yield common + [0.50, 0.99] + [seq]

        # TPCC: warehouse_count
        common = [alg, thread_count, 'TPCC']
        # yield common + [thread_count] + [seq]

        for warehouse_count in warehouse_counts:
          if thread_count >= warehouse_count:
            yield common + [warehouse_count] + [seq]


def update_conf(conf, exp):
  conf = set_alg(conf, exp[0])
  conf = set_threads(conf, exp[1])
  if exp[2] == 'YCSB':
    conf = set_ycsb(conf, *exp[3:-1])
  elif exp[2] == 'TPCC':
    conf = set_tpcc(conf, *exp[3:-1])
  else: assert False
  return conf


def sort_exps(exps):
  def _exp_pri(exp):
    pri = 0
    # prefer fast schemes
    if exp[0].startswith('MICA'): pri -= 2
    if exp[0] == 'MICA-TEST': pri -= 1
    if exp[0] == 'SILO-ORG' or exp[0] == 'TICTOC': pri -= 1

    # prefer max cores
    if exp[1] in (28, 56): pri -= 1

    # prefer write-intensive workloads
    if exp[2] == 'YCSB' and exp[3] == 0.50: pri -= 1
    # prefer standard skew
    if exp[2] == 'YCSB' and exp[4] in (0.00, 0.99): pri -= 1

    # prefer (warehouse count) = (thread count)
    if exp[2] == 'TPCC' and exp[1] == exp[3]: pri -= 1
    # prefer standard warehouse counts
    if exp[2] == 'TPCC' and exp[3] in (1, 4, 16, 28, 56): pri -= 1

    # run exps in their sequence number
    return (exp[-1], pri)

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
    if os.path.exists(gen_filename(exp)): continue
    if os.path.exists(gen_filename(exp) + '.failed'): continue
    yield exp

def find_exps_to_run(exps, pat):
  for exp in exps:
    filename = gen_filename(exp)
    if filename.find(pat) == -1: continue
    yield exp


def validate_result(output):
  return output.find('[summary] tput=') != -1


hugepage_status = -1

def run(exp, prepare_only):
  global hugepage_status

  # update config
  conf = open('config-std.h').read()
  conf = update_conf(conf, exp)
  open('config.h', 'w').write(conf)

  # clean up
  os.system('make clean > /dev/null')
  os.system('rm -f ./rundb')

  if exp[0].startswith('MICA'):
    if hugepage_status != 16384:
      os.system('../script/setup.sh 16384 16384 > /dev/null')   # 64 GiB
      hugepage_status = 16384
  else:
    if hugepage_status != 0:
      os.system('../script/setup.sh 0 0 > /dev/null')
      hugepage_status = 0

  if prepare_only: return

  # compile
  ret = os.system('make -j > /dev/null')
  assert ret == 0, 'failed to compile for %s' % exp
  os.system('sudo sync')
  os.system('sudo sync')
  time.sleep(10)

  # run
  filename = gen_filename(exp)

  # cmd = 'sudo ./rundb | tee %s' % (filename + '.tmp')
  cmd = 'sudo ./rundb'
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  stdout = p.communicate()[0].decode('utf-8')
  if p.returncode != 0:
    print('failed to run exp for %s' % exp)
    open(filename + '.failed', 'w').write(stdout)
    return
  if not validate_result(stdout):
    print('validation failed for %s' % exp)
    open(filename + '.failed', 'w').write(stdout)
    return

  # finalize
  open(filename, 'w').write(stdout)

def run_all(pat, prepare_only):
  exps = list(enum_exps())
  exps = list(unique_exps(exps))
  total_count = len(exps)
  print('total %d exps' % total_count)

  if not prepare_only:
    exps = list(skip_done(exps))
  exps = list(find_exps_to_run(exps, pat))
  skip_count = total_count - len(exps)
  print('%d exps skipped' % skip_count)

  exps = list(sort_exps(exps))
  print('total %d exps to run' % len(exps))
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


if __name__ == '__main__':
  # remove_stale()

  if len(sys.argv) == 2:
    print('%s [RUN | RUN pattern(s) | PREPARE pattern]' % sys.argv[0])
    sys.exit(1)

  if sys.argv[1].upper() == 'RUN':
    if len(sys.argv) == 2:
      run_all('', False)
    else:
      for pat in sys.argv[2:]:
        run_all(pat, False)
  elif sys.argv[1].upper() == 'PREPARE':
    run_all(sys.argv[2], True)
  else:
    assert False

