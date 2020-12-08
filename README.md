# bfsvtab: A virtual table extension for breadth-first search queries in Sqlite3

![Automated Testing](https://github.com/abetlen/sqlite3-bfsvtab-ext/workflows/Automated%20Testing/badge.svg)

This extension allows Sqlite3 to perform breadth-first search queries against graph data from any table or view in an existing database.

**Can't you do this with recursive common table expressions (RCTEs)?**

Yes, but in practice RCTEs are very slow for non-tree graphs.

**How does it work?**

This extension defines a virtual table called `bfsvtab`.

The virtual table requires 4 SQL constraints to be set for all queries:
- `tablename`: The name of the table or view which contains the graph edges (can be any table or view in the database).
- `fromcolumn`: The node id column where an edge starts from (must be integer).
- `tocolumn`: The node id column where an edge goes to (must be integer).
- `root`: The root node id of the breadth-first traversal.

As well as an optional constraint to determine the order of node neighbour traversal.
- `order_by_column`: The name of the column used to determine the order of node neighbour traversal.

The virtual table also provides the following columns that can be returned or used as contraints:
- `id`: The id of the current node being visited.
- `distance`: The shortest distance to the current node from the root node.
- `parent`: The id of the parent node to the current node in the spanning tree rooted at the root node.
- `shortest_path`: Slash delimited string containing the shortest path from the root to the given node.

Check out the examples below for more details.

## Build From Source

```bash
$ git clone git@github.com:abetlen/sqlite3-bfsvtab-ext.git
$ cd sqlite3-bfsvtab-ext
$ make
$ make test
```

## Basic Examples

```sql
/* Load the compiled extension from the current directory */
.load ./bfsvtab

/* Insert for the following examples */
create table edges(fromNode integer, toNode integer);
insert into edges(fromNode, toNode) values
  (1, 2),
  (1, 3),
  (2, 4),
  (3, 4);

/* Find the minimum distance from node 1 to node 4 */
select 
  id, distance 
  from bfsvtab 
  where 
    tablename  = 'edges'    and
    fromcolumn = 'fromNode' and
    tocolumn   = 'toNode'   and
    root       = 1          and
    id         = 4;

/* Find the shortest path from node 1 to node 4 */
select 
  shortest_path
  from bfsvtab 
  where 
    tablename  = 'edges'    and
    fromcolumn = 'fromNode' and
    tocolumn   = 'toNode'   and
    root       = 1          and
    id         = 4;

/* Find the minimum distance from node 1 to all nodes */
select 
  id, distance
  from bfsvtab 
  where 
    tablename  = 'edges'    and
    fromcolumn = 'fromNode' and
    tocolumn   = 'toNode'   and
    root       = 1;

/* Return the edge set of a spanning tree rooted at node 1 */
select 
  parent, id 
  from bfsvtab 
  where 
    tablename  = 'edges'    and
    fromcolumn = 'fromNode' and
    tocolumn   = 'toNode'   and
    root       = 1          and
    parent     is not null;
```
