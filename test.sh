#!/bin/bash

valgrind --leak-check=full --show-leak-kinds=all --keep-debuginfo=yes -s sqlite3 < test/bfsvtab.sql

cmp <(sqlite3 < test/rcte.sql) <(sqlite3 < test/bfsvtab.sql)
