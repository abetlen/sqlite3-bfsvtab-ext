create table edges(fromNode integer, toNode integer, primary key(fromNode, toNode));
insert into edges(fromNode, toNode) values
    (1, 2),
    (1, 3),
    (2, 4),
    (3, 4),
    (4, 5),
    (4, 6),
    (5, 7),
    (6, 7),
    (7, 8),
    (7, 9),
    (8, 10),
    (9, 10);
