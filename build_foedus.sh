#!/bin/sh

cd foedus_code || exit 1

[ ! -d build ] && mkdir bulid
cd build

cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j
