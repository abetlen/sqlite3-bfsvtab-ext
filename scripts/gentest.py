import sqlite3

import argparse


def main(args):
    conn = sqlite3.connect(args.output_db)
    cur = conn.cursor()

    cur.execute("drop table if exists edges")
    cur.execute(
        """
    create table edges(fromNode integer, toNode integer, primary key(fromNode, toNode))
    """
    )

    n = args.num_levels
    mid = 1
    top = 2
    bottom = 3

    for i in range(n):
        cur.execute("insert into edges(fromNode, toNode) values (?, ?)", (mid, top))
        cur.execute("insert into edges(fromNode, toNode) values (?, ?)", (mid, bottom))

        cur.execute(
            "insert into edges(fromNode, toNode) values (?, ?)", (top, bottom + 1)
        )
        cur.execute(
            "insert into edges(fromNode, toNode) values (?, ?)", (bottom, bottom + 1)
        )

        mid = bottom + 1
        top = mid + 1
        bottom = top + 1

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-levels", type=int, default=16)
    parser.add_argument("-o", "--output-db", type=str, default="benchmark.db")
    args = parser.parse_args()
    main(args)
