.read ./test/fixture.sql
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
