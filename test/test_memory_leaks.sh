#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILENAME="$(basename $0)"

echo "$FILENAME"

valgrind --quiet --child-silent-after-fork=yes --leak-check=full --show-leak-kinds=all --keep-debuginfo=yes sqlite3 < $DIR/test_memory_leaks.sql > /dev/null
