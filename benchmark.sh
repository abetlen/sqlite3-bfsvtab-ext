#!/bin/bash

dbpath="./benchmark.db"

python3 scripts/gentest.py -o $dbpath -n 16

echo "Recursive Common Table Expression"
time (sqlite3 ${dbpath} <<EOF
with recursive
    bfs(id, parent, distance) as (
        select 1, null, 0
        union all
        select edges.toNode, bfs.id, bfs.distance + 1
        from edges, bfs
        where edges.fromNode = bfs.id
        order by 2
    )
select id, parent, min(distance) as distance from bfs
group by id
order by distance;
EOF
) > /dev/null

echo

echo "BFSVTAB"
time (sqlite3 ${dbpath} <<EOF
.load ./bfsvtab.so
select id, parent, distance 
from bfsvtab 
where 
    tablename='edges' and
    fromcolumn='fromNode' and
    tocolumn='toNode' and
    root = 1;
EOF
) > /dev/null

rm $dbpath
