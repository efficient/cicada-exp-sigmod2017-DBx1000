#!/bin/bash

../script/setup.sh 0 0

cd ermia || exit 1

echo Build: SI
#make clean &> /dev/null
MODE=perf DEBUG=0 NDEBUG=1 make -j8 dbtest
mv ./out-perf.masstree/benchmarks/dbtest ./dbtest-SI

echo "add to /etc/security/limits.conf: (replace [user] with the username)"
echo "[user] - memlock unlimited"
