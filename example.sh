#!/bin/bash

set -u

oredb='example.redb'
tabname='kvpairs'

wsm='./ipc-strings2redb.wasm'
hostd="${PWD}"
guestd='/guest.d'
redb4guest="${guestd}/${oredb}"

run_wasi(){
  cat /dev/stdin |
  wasmtime \
    run \
    --dir "${hostd}::${guestd}" \
    --env ENV_TABLE_NAME="${tabname}" \
    --env ENV_OUT_REDB_PATH="${redb4guest}" \
    "${wsm}"
}

input1(){
  echo strkey,val
  echo helo,wrld
  echo hell,world
}

strings2ipc_strm(){
  imports=""

  imports="${imports}import sys;"
  imports="${imports}import io;"
  imports="${imports}import functools;"
  imports="${imports}import operator;"

  imports="${imports}import pyarrow as pa;"
  imports="${imports}import pyarrow.csv as pc;"

  cat /dev/stdin |
    python3 -c "${imports}"' functools.reduce(
      lambda state, f: f(state),
      [
        pc.open_csv,
        lambda rdr: (
          pa.ipc.new_stream(sys.stdout.buffer, rdr.schema),
          rdr,
        ),
        lambda pair: (
          map(
            pair[0].write_batch,
            pair[1],
          ),
          pair[0],
        ),
        lambda pair: (sum(1 for _ in pair[0]), pair[1]),
        operator.itemgetter(1),
        operator.methodcaller("close"),
      ],
      sys.stdin.buffer,
    )'
}

input1 |
  strings2ipc_strm |
  run_wasi
