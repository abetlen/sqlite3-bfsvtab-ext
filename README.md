# bfsvtab: Breadth First Search Virtual Table Extension for Sqlite3

This extension allows Sqlite3 to perform a breadth first search queries against graph data.

**Can't you do this with recursive common table expressions (RCTEs)?**

Yes, but in practice RCTEs become extremely very slow for non-tree graphs.

**How does it work**

This extension defines a virtual table called `bfsvtab`.
The virtual table requires 4 SQL constraints to be set:
- `tablename`: The name of the table or view which contains the graph edges (can be any table or view in the database).
- `fromcolumn`: The node id column where an edge starts from (must be integer).
- `tocolumn`: The node id column where an edge goes to (must be integer).
- `root`: The root node id of the breadth first traversal.

Check out the examples below for more details.

## Build from source

```bash
$ git clone git@github.com:abetlen/sqlite3-bfsvtab-ext.git
$ cd sqlite3-bfsvtab-ext
$ make
$ make test
```

## Basic Examples

```sql
.load ./bfsvtab

create table edges(fromNode integer, toNode integer);
insert into edges(fromNode, toNode) values
  (1, 2),
  (1, 3),
  (2, 4),
  (3, 4);

select 
  id, distance 
  from bfsvtab 
  where 
    tablename  = 'edges'    and
    fromcolumn = 'fromNode' and
    tocolumn   = 'toNode'   and
    root       = 1          and
    id         = 4;
```

