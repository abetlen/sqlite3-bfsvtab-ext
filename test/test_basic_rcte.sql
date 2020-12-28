.read ./test/test_basic_fixture.sql
with recursive
    bfs(id, parent, shortest_path, distance) as (
        select 1, null, '/' || 1 || '/',  0
        union all
        select edges.toNode, bfs.id, bfs.shortest_path || edges.toNode || '/', bfs.distance + 1
        from edges, bfs
        where edges.fromNode = bfs.id
        order by 2
    )
select id, parent, shortest_path, min(distance) as distance from bfs
group by id
order by distance;
