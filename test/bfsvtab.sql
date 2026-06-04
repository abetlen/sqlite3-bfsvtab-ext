.load ./bfsvtab
.read ./test/fixture.sql
create virtual table bfs using bfsvtab(
  tablename='edges',
  fromcolumn='fromNode',
  tocolumn='toNode',
);
select id, parent, shortest_path, distance from bfs where root = 1;
