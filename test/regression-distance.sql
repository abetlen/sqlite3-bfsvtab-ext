/* Regression: enforce distance constraints in bfsvtab results. */
.load ./bfsvtab
.read ./test/fixture.sql

create virtual table bfs using bfsvtab(
  tablename='edges',
  fromcolumn='fromNode',
  tocolumn='toNode'
);

CREATE TEMP TABLE bfsvtab_result AS
SELECT id, parent, shortest_path, distance
FROM bfs
WHERE root = 1
  AND distance <= 2
;

CREATE TEMP TABLE rcte_result AS
WITH RECURSIVE
    rcte_bfs(id, parent, shortest_path, distance) as (
        select 1, null, '/' || 1 || '/',  0
        union all
        select edges.toNode, rcte_bfs.id, rcte_bfs.shortest_path || edges.toNode || '/', rcte_bfs.distance + 1
        from edges, rcte_bfs
        where edges.fromNode = rcte_bfs.id
        order by 2
    )
SELECT id, parent, shortest_path, min(distance) as distance from rcte_bfs
where distance <= 2
group by id
order by distance;

SELECT
  (SELECT count(*) FROM (
      SELECT id, parent, shortest_path, distance FROM bfsvtab_result
      EXCEPT
      SELECT id, parent, shortest_path, distance FROM rcte_result
  ))
  +
  (SELECT count(*) FROM (
      SELECT id, parent, shortest_path, distance FROM rcte_result
      EXCEPT
      SELECT id, parent, shortest_path, distance FROM bfsvtab_result
  )) AS diff_count;
