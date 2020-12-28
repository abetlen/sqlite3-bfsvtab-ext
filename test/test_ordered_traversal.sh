#!/bin/bash

set -e

FILENAME="$(basename $0)"

echo "$FILENAME"

GOT="$(sqlite3 <<EOF
.load ./bfsvtab
create table edges(src integer, dst integer, key integer);
insert into edges(src, dst, key) values 
    (0, 1, 3),
    (0, 2, 2),
    (0, 3, 1);
select id from bfsvtab 
where 
    tablename = 'edges' 
    and fromcolumn = 'src' 
    and tocolumn = 'dst' 
    and root = 0 and id <> root 
    and order_by_column = 'key';
EOF
)"
EXPECTED="3 2 1"

cmp <(echo $GOT) <(echo $EXPECTED)
