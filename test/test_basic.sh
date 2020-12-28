#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILENAME="$(basename $0)"

echo "$FILENAME"

cmp <(sqlite3 < $DIR/test_basic_rcte.sql) <(sqlite3 < $DIR/test_basic_bfsvtab.sql)
