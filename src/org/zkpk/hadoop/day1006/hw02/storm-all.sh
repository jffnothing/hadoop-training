#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

"$bin"/storm nimbus </dev/null 2<&1 &
"$bin"/storm supervisor </dev/null 2<&1 &
"$bin"/storm ui </dev/null 2<&1 &
"$bin"/ssh-sv
