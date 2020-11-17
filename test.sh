#!/bin/bash

cmp <(sqlite3 < test/rcte.sql) <(sqlite3 < test/bfsvtab.sql)
