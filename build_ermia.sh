#!/bin/bash

cd ermia || exit 1
./build-all.sh

echo "add to /etc/security/limits.conf: (replace [user] with the username)"
echo "[user] - memlock unlimited"

