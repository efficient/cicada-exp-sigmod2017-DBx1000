#!/bin/bash

pushd ermia/
./build-all.sh
popd

echo "mlock limit must be set to unlimited"
echo "add to /etc/security/limits.conf: (replace [user] with the username)"
echo "[user] soft memlock unlimited"
echo "[user] hard memlock unlimited"

