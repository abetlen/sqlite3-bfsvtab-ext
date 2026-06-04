#!/bin/bash

valgrind --leak-check=full --show-leak-kinds=all --keep-debuginfo=yes -s sqlite3 < test/bfsvtab.sql

cmp <(sqlite3 < test/rcte.sql) <(sqlite3 < test/bfsvtab.sql)

DIFF=$(sqlite3 < test/regression-distance.sql)
if [ "$DIFF" -ne 0 ]; then
  echo "Regression check failed: bounded distance query mismatch (diff_count=${DIFF})."
  exit 1
fi
