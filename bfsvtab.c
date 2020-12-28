/*
** 2020-10-22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code for a virtual table that performs a breadth-first 
** search of any graph represented in a real or virtual table.
** The virtual table is called "bfsvtab".
**
** A bfsvtab virtual table is created liske this:
**
**     CREATE VIRTUAL TABLE x USING bfsvtab(
**        tablename=<tablename>,
**        fromcolumn=<columname>,
**        tocolumn=<columname>,
**     )
**
** This bfsvtab extension is minimal, in the sense that it uses only the required
** methods on the sqlite3_module object.  As a result, bfsvtab is
** a read-only and eponymous-only table.  Those limitation can be removed
** by adding new methods.
**
**     SELECT id, parent, distance
**     FROM bfsvtab 
**     WHERE 
**         tablename=<tablename> and
**         fromcolumn=<fromcolumn> and
**         tocolumn=<tocolumn> and
**         root=?;
**
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include <ctype.h>

typedef struct bfsvtab_avl bfsvtab_avl;
typedef struct bfsvtab_node bfsvtab_node;
typedef struct bfsvtab_queue bfsvtab_queue;

/*****************************************************************************
** AVL Tree implementation
*/
/*
** Objects that want to be members of the AVL tree should embedded an
** instance of this structure.
*/
struct bfsvtab_avl {
    sqlite3_int64 id;     /* Id of this entry in the table */
    sqlite3_int64 parent; /* Id of this nodes parent. parent_id == id for root node */
    bfsvtab_avl *pBefore; /* Other elements less than id */
    bfsvtab_avl *pAfter;  /* Other elements greater than id */
    bfsvtab_avl *pUp;     /* Parent element */
    short int height;     /* Height of this node.  Leaf==1 */
    short int imbalance;  /* Height difference between pBefore and pAfter */
};

/* Recompute the bfsvtab_avl.height and bfsvtab_avl.imbalance fields for p.
** Assume that the children of p have correct heights.
*/
static void bfsvtabAvlRecomputeHeight(bfsvtab_avl *p) {
    short int hBefore = p->pBefore ? p->pBefore->height : 0;
    short int hAfter = p->pAfter ? p->pAfter->height : 0;
    p->imbalance = hBefore - hAfter;  /* -: pAfter higher.  +: pBefore higher */
    p->height = (hBefore>hAfter ? hBefore : hAfter)+1;
}

/*
**     P                B
**    / \              / \
**   B   Z    ==>     X   P
**  / \                  / \
** X   Y                Y   Z
**
*/
static bfsvtab_avl *bfsvtabAvlRotateBefore(bfsvtab_avl *pP) {
    bfsvtab_avl *pB = pP->pBefore;
    bfsvtab_avl *pY = pB->pAfter;
    pB->pUp = pP->pUp;
    pB->pAfter = pP;
    pP->pUp = pB;
    pP->pBefore = pY;
    if (pY) {
        pY->pUp = pP;
    }
    bfsvtabAvlRecomputeHeight(pP);
    bfsvtabAvlRecomputeHeight(pB);
    return pB;
}

/*
**     P                A
**    / \              / \
**   X   A    ==>     P   Z
**      / \          / \
**     Y   Z        X   Y
**
*/
static bfsvtab_avl *bfsvtabAvlRotateAfter(bfsvtab_avl *pP) {
    bfsvtab_avl *pA = pP->pAfter;
    bfsvtab_avl *pY = pA->pBefore;
    pA->pUp = pP->pUp;
    pA->pBefore = pP;
    pP->pUp = pA;
    pP->pAfter = pY;
    if (pY) {
        pY->pUp = pP;
    }
    bfsvtabAvlRecomputeHeight(pP);
    bfsvtabAvlRecomputeHeight(pA);
    return pA;
}

/*
** Return a pointer to the pBefore or pAfter pointer in the parent
** of p that points to p.  Or if p is the root node, return pp.
*/
static bfsvtab_avl **bfsvtabAvlFromPtr(bfsvtab_avl *p, bfsvtab_avl **pp) {
    bfsvtab_avl *pUp = p->pUp;
    if (pUp == 0) {
        return pp;
    }
    if (pUp->pAfter == p) {
        return &pUp->pAfter;
    }
    return &pUp->pBefore;
}

/*
** Rebalance all nodes starting with p and working up to the root.
** Return the new root.
*/
static bfsvtab_avl *bfsvtabAvlBalance(bfsvtab_avl *p) {
    bfsvtab_avl *pTop = p;
    bfsvtab_avl **pp;
    while (p) {
        bfsvtabAvlRecomputeHeight(p);
        if (p->imbalance >= 2) {
            bfsvtab_avl *pB = p->pBefore;
            if (pB->imbalance < 0) {
                p->pBefore = bfsvtabAvlRotateAfter(pB);
            }
            pp = bfsvtabAvlFromPtr(p,&p);
            p = *pp = bfsvtabAvlRotateBefore(p);
        } else if (p->imbalance <= (-2)) {
            bfsvtab_avl *pA = p->pAfter;
            if (pA->imbalance > 0) {
                p->pAfter = bfsvtabAvlRotateBefore(pA);
            }
            pp = bfsvtabAvlFromPtr(p, &p);
            p = *pp = bfsvtabAvlRotateAfter(p);
        }
        pTop = p;
        p = p->pUp;
    }
    return pTop;
}

/* Search the tree rooted at p for an entry with id.  Return a pointer
** to the entry or return NULL.
*/
static bfsvtab_avl *bfsvtabAvlSearch(bfsvtab_avl *p, sqlite3_int64 id) {
    while(p && id != p->id) {
        p = (id < p->id) ? p->pBefore : p->pAfter;
    }
    return p;
}

/* Find the first node (the one with the smallest key).
*/
static bfsvtab_avl *bfsvtabAvlFirst(bfsvtab_avl *p) {
    if (p) {
        while (p->pBefore) {
            p = p->pBefore;
        }
    }
    return p;
}

/* Return the node with the next larger key after p.
*/
bfsvtab_avl *bfsvtabAvlNext(bfsvtab_avl *p) {
    bfsvtab_avl *pPrev = 0;
    while (p && p->pAfter == pPrev) {
        pPrev = p;
        p = p->pUp;
    }
    if(p && pPrev == 0) {
        p = bfsvtabAvlFirst(p->pAfter);
    }
    return p;
}

/* Insert a new node pNew.  Return NULL on success.  If the key is not
** unique, then do not perform the insert but instead leave pNew unchanged
** and return a pointer to an existing node with the same key.
*/
static bfsvtab_avl *bfsvtabAvlInsert(
    bfsvtab_avl **ppHead,  /* Head of the tree */
    bfsvtab_avl *pNew      /* New node to be inserted */
) {
    bfsvtab_avl *p = *ppHead;
    if (p == 0) {
        p = pNew;
        pNew->pUp = 0;
    } else {
        while (p) {
            if (pNew->id < p->id) {
                if (p->pBefore) {
                    p = p->pBefore;
                } else {
                    p->pBefore = pNew;
                    pNew->pUp = p;
                    break;
                }
            } else if (pNew->id > p->id) {
                if(p->pAfter) {
                    p = p->pAfter;
                } else {
                    p->pAfter = pNew;
                    pNew->pUp = p;
                    break;
                }
            } else {
                return p;
            }
        }
    }
    pNew->pBefore = 0;
    pNew->pAfter = 0;
    pNew->height = 1;
    pNew->imbalance = 0;
    *ppHead = bfsvtabAvlBalance(p);
    return 0;
}

/* Walk the tree can call xDestroy on each node
*/
static void bfsvtabAvlDestroy(bfsvtab_avl *p, void (*xDestroy)(bfsvtab_avl*)) {
    if (p) {
        bfsvtabAvlDestroy(p->pBefore, xDestroy);
        bfsvtabAvlDestroy(p->pAfter, xDestroy);
        xDestroy(p);
    }
}
/*
** End of the AVL Tree implementation
******************************************************************************/

struct bfsvtab_node {
    sqlite3_int64 id;
    sqlite3_int64 parent;
    sqlite3_int64 distance;

    struct bfsvtab_node *pList;
};

/* A queue of nodes */
struct bfsvtab_queue {
  bfsvtab_node *pFirst;       /* Oldest node on the queue */
  bfsvtab_node *pLast;        /* Youngest node on the queue */
};

/*
** Add a node to the end of the queue
*/
static void queuePush(bfsvtab_queue *pQueue, bfsvtab_node *pNode){
  pNode->pList = 0;
  if( pQueue->pLast ){
    pQueue->pLast->pList = pNode;
  }else{
    pQueue->pFirst = pNode;
  }
  pQueue->pLast = pNode;
}

/*
** Extract the oldest element (the front element) from the queue.
*/
static bfsvtab_node *queuePull(bfsvtab_queue *pQueue){
  bfsvtab_node *p = pQueue->pFirst;
  if( p ){
    pQueue->pFirst = p->pList;
    if( pQueue->pFirst==0 ) pQueue->pLast = 0;
  }
  return p;
}

static void queueDestroy(bfsvtab_queue *pQueue, void (*xDestroy)(bfsvtab_node*)) {
    bfsvtab_node *p = pQueue->pFirst, *next;
    while (p) {
        next = p->pList;
        xDestroy(p);
        p = next;
    }
}

/*
** This function converts an SQL quoted string into an unquoted string
** and returns a pointer to a buffer allocated using sqlite3_malloc() 
** containing the result. The caller should eventually free this buffer
** using sqlite3_free.
**
** Examples:
**
**     "abc"   becomes   abc
**     'xyz'   becomes   xyz
**     [pqr]   becomes   pqr
**     `mno`   becomes   mno
*/
static char *bfsvtabDequote(const char *zIn) {
    sqlite3_int64 nIn;              /* Size of input string, in bytes */
    char *zOut;                     /* Output (dequoted) string */

    nIn = strlen(zIn);
    zOut = sqlite3_malloc64(nIn + 1);
    if (zOut) {
        char q = zIn[0];              /* Quote character (if any ) */

        if (q != '[' && q != '\'' && q != '"' && q != '`') {
            memcpy(zOut, zIn, (size_t)(nIn + 1));
        } else {
            int iOut = 0;               /* Index of next byte to write to output */
            int iIn;                    /* Index of next byte to read from input */

            if (q == '[') {
                q = ']';
            }
            for (iIn = 1; iIn < nIn; iIn++) {
                if (zIn[iIn] == q) {
                    iIn++;
                }
                zOut[iOut++] = zIn[iIn];
            }
        }
        assert((int)strlen(zOut) <= nIn);
    }
    return zOut;
}

/*
** Check to see if the argument is of the form:
**
**       KEY = VALUE
**
** If it is, return a pointer to the first character of VALUE.
** If not, return NULL.  Spaces around the = are ignored.
*/
static const char *bfsvtabValueOfKey(const char *zKey, const char *zStr){
    int nKey = (int)strlen(zKey);
    int nStr = (int)strlen(zStr);
    int i;
    if (nStr < nKey + 1) {
        return 0;
    }
    if (memcmp(zStr, zKey, nKey) != 0) {
        return 0;
    }
    for (i = nKey; isspace((unsigned char)zStr[i]); i++) {
        ;
    }
    if (zStr[i] != '=') {
        return 0;
    }
    i++;
    while (isspace((unsigned char)zStr[i])) { 
        i++; 
    }
    return zStr + i;
}

/* bfsvtab_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct bfsvtab_vtab bfsvtab_vtab;
struct bfsvtab_vtab {
    sqlite3_vtab base;  /* Base class - must be first */
    /* Add new fields here, as necessary */
    char *zDb;
    char *zSelf;
    char *zTableName;
    char *zFromColumn;
    char *zToColumn;
    char *zOrderByColumn;

    sqlite3 *db;
};

/* bfsvtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct bfsvtab_cursor bfsvtab_cursor;
struct bfsvtab_cursor {
    sqlite3_vtab_cursor base;  /* Base class - must be first */
    /* Insert new fields here.  For this bfsvtab we only keep track
    ** of the rowid */
    sqlite3_int64 iRowid;      /* The rowid */
    sqlite3_stmt *pStmt;       /* Prepared statement to return node neighbours */

    bfsvtab_vtab *pVtab;       /* The virtual table this cursor belongs to */
    char *zTableName;          /* Name of table holding edge relation */
    char *zFromColumn;         /* Name of from column of zTableName */
    char *zToColumn;           /* Name of to column of zTableName */
    char *zOrderByColumn;      /* Name of column to order neighbours by during BFS traversal */

    bfsvtab_avl *pVisited;     /* Set of Visited Nodes */

    bfsvtab_queue pQueue;      /* Queue of next Nodes */
    bfsvtab_node *pCurrent;     /* Current element of output */
    sqlite3_int64 root;

};

/*
** Deallocate a bfsvtab_vtab object
*/
static void bfsvtabFree(bfsvtab_vtab *p) {
    if (p) {
        sqlite3_free(p->zDb);
        sqlite3_free(p->zSelf);
        sqlite3_free(p->zTableName);
        sqlite3_free(p->zFromColumn);
        sqlite3_free(p->zToColumn);
        sqlite3_free(p->zOrderByColumn);
        memset(p, 0, sizeof(*p));
        sqlite3_free(p);
    }
}

/*
** The bfsvtabConnect() method is invoked to create a new
** bfs virtual table.
**
** Think of this routine as the constructor for bfsvtab_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the bfsvtab_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int bfsvtabConnect(
    sqlite3 *db,
    void *pAux,
    int argc, const char *const*argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
) {
    bfsvtab_vtab *pNew;
    int i;
    int rc;
    const char *zVal;

    (void) pAux;
    *ppVtab = 0;
    rc = SQLITE_OK;

    pNew = sqlite3_malloc(sizeof(*pNew));
    if (pNew == 0) {
        return SQLITE_NOMEM;
    }
    memset(pNew, 0, sizeof(*pNew));

    pNew->db = db;
    pNew->zDb = sqlite3_mprintf("%s", argv[1]);
    if (pNew->zDb == 0) {
        rc = SQLITE_NOMEM;
        goto connectError;
    }

    pNew->zSelf = sqlite3_mprintf("%s", argv[2]);
    if (pNew->zSelf == 0) {
        rc = SQLITE_NOMEM;
        goto connectError;
    }

    for (i = 3; i < argc; i++) {
        zVal = bfsvtabValueOfKey("tablename", argv[i]);
        if (zVal) {
            sqlite3_free(pNew->zTableName);
            pNew->zTableName = bfsvtabDequote(zVal);
            if (pNew->zTableName == 0) {
                rc = SQLITE_NOMEM;
                goto connectError;
            }
            continue;
        }
        zVal = bfsvtabValueOfKey("fromcolumn", argv[i]);
        if (zVal) {
            sqlite3_free(pNew->zFromColumn);
            pNew->zFromColumn = bfsvtabDequote(zVal);
            if (pNew->zFromColumn == 0) {
                rc = SQLITE_NOMEM;
                goto connectError;
            }
            continue;
        }
        zVal = bfsvtabValueOfKey("tocolumn", argv[i]);
        if (zVal) {
            sqlite3_free(pNew->zToColumn);
            pNew->zToColumn = bfsvtabDequote(zVal);
            if (pNew->zToColumn == 0) {
                rc = SQLITE_NOMEM;
                goto connectError;
            }
            continue;
        }
        zVal = bfsvtabValueOfKey("order_by_column", argv[i]);
        if (zVal) {
            sqlite3_free(pNew->zOrderByColumn);
            pNew->zOrderByColumn = bfsvtabDequote(zVal);
            if (pNew->zOrderByColumn == 0) {
                rc = SQLITE_NOMEM;
                goto connectError;
            }
            continue;
        }

        *pzErr = sqlite3_mprintf("unrecognized argument: [%s]\n", argv[i]);
        bfsvtabFree(pNew);
        return SQLITE_ERROR;
    }

    rc = sqlite3_declare_vtab(db,
       "CREATE TABLE x(id,parent,distance,shortest_path,root HIDDEN,"
                       "tablename HIDDEN,fromcolumn HIDDEN,"
                       "tocolumn HIDDEN, order_by_column HIDDEN)"
    );
    /* For convenience, define symbolic names for the index to each column. */
#define BFSVTAB_COL_ID              0
#define BFSVTAB_COL_PARENT          1
#define BFSVTAB_COL_DISTANCE        2
#define BFSVTAB_COL_SHORTEST_PATH   3
#define BFSVTAB_COL_ROOT            4
#define BFSVTAB_COL_TABLENAME       5
#define BFSVTAB_COL_FROMCOLUMN      6
#define BFSVTAB_COL_TOCOLUMN        7
#define BFSVTAB_COL_ORDER_BY_COLUMN 8
    if (rc != SQLITE_OK) {
        bfsvtabFree(pNew);
    }
    *ppVtab = (sqlite3_vtab*)pNew;
    return rc;

connectError:
    bfsvtabFree(pNew);
    return rc;
}

/*
** This method is the destructor for bfsvtab_vtab objects.
*/
static int bfsvtabDisconnect(sqlite3_vtab *pVtab) {
    bfsvtab_vtab *p = (bfsvtab_vtab*)pVtab;
    bfsvtabFree(p);
    return SQLITE_OK;
}

/*
** Constructor for a new bfsvtab_cursor object.
*/
static int bfsvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    bfsvtab_vtab *pVtab = (bfsvtab_vtab*)p;
    bfsvtab_cursor *pCur;
    pCur = sqlite3_malloc(sizeof(*pCur));
    if (pCur == 0) {
        return SQLITE_NOMEM;
    }
    memset(pCur, 0, sizeof(*pCur));
    pCur->pVtab = pVtab;
    *ppCursor = &pCur->base;
    return SQLITE_OK;
}

static void bfsvtabClearCursor(bfsvtab_cursor *pCur) {
  bfsvtabAvlDestroy(pCur->pVisited, (void(*)(bfsvtab_avl*))sqlite3_free);
  queueDestroy(&pCur->pQueue, (void(*)(bfsvtab_node*))sqlite3_free);
  memset(&pCur->pQueue, 0, sizeof(pCur->pQueue));

  sqlite3_free(pCur->zTableName);
  sqlite3_free(pCur->zFromColumn);
  sqlite3_free(pCur->zToColumn);
  sqlite3_free(pCur->zOrderByColumn);

  sqlite3_finalize(pCur->pStmt);

  pCur->zTableName = 0;
  pCur->zFromColumn = 0;
  pCur->zToColumn = 0;
  pCur->zOrderByColumn = 0;
  pCur->pCurrent = 0;
  pCur->pVisited = 0;
}

/*
** Destructor for a bfsvtab_cursor.
*/
static int bfsvtabClose(sqlite3_vtab_cursor *cur) {
    bfsvtab_cursor *pCur = (bfsvtab_cursor*)cur;
    bfsvtabClearCursor(pCur);
    sqlite3_free(pCur);
    return SQLITE_OK;
}

/*
** Advance a bfsvtab_cursor to its next row of output.
*/
static int bfsvtabNext(sqlite3_vtab_cursor *cur) {
    int rc;
    bfsvtab_avl *newAvlNode;
    bfsvtab_cursor *pCur = (bfsvtab_cursor*)cur;
    if (pCur->pCurrent) {
        sqlite3_free(pCur->pCurrent);
    }
    pCur->pCurrent = queuePull(&pCur->pQueue);
    if (pCur->pCurrent == 0) {
        return SQLITE_OK;
    }
    rc = sqlite3_bind_int64(pCur->pStmt, 1, pCur->pCurrent->id);
    if (rc) {
        return rc;
    }
    while (rc == SQLITE_OK && sqlite3_step(pCur->pStmt) == SQLITE_ROW) {
        if (sqlite3_column_type(pCur->pStmt, 0) == SQLITE_INTEGER) {
            sqlite3_int64 iNew = sqlite3_column_int64(pCur->pStmt, 0);
            if (bfsvtabAvlSearch(pCur->pVisited, iNew) != 0) {
                continue;
            }
            bfsvtab_node *node = sqlite3_malloc(sizeof(*node));
            if (node == 0) {
                return SQLITE_NOMEM;
            }
            memset(node, 0, sizeof(*node));
            node->id = iNew;
            node->parent = pCur->pCurrent->id;
            node->distance = pCur->pCurrent->distance + 1;
            queuePush(&pCur->pQueue, node);

            newAvlNode = sqlite3_malloc(sizeof(*newAvlNode));
            if (newAvlNode == 0) {
                return SQLITE_NOMEM;
            }
            memset(newAvlNode, 0, sizeof(*newAvlNode));
            newAvlNode->id = iNew;
            newAvlNode->parent = pCur->pCurrent->id;
            bfsvtabAvlInsert(&pCur->pVisited, newAvlNode);
        }
    }
    rc = sqlite3_clear_bindings(pCur->pStmt);
    if (rc) {
        return rc;
    }
    rc = sqlite3_reset(pCur->pStmt);
    if (rc) {
        return rc;
    }
    return rc;
}

/*
** Recursively builds a node path string.
*/
int bfsvtabBuildShortestPathStr(sqlite3_str *str, bfsvtab_avl *visited, sqlite3_int64 id) {
    int rc;
    bfsvtab_avl *node = bfsvtabAvlSearch(visited, id);
    if (node == 0) {
        return SQLITE_OK;
    }
    if (node->parent != id) {
        rc = bfsvtabBuildShortestPathStr(str, visited, node->parent);
        if (rc != SQLITE_OK) {
            return rc;
        }
        sqlite3_str_appendf(str, "%d/", id);
        rc = sqlite3_str_errcode(str);
        return rc;
    }
    sqlite3_str_appendf(str, "/%d/", id);
    rc = sqlite3_str_errcode(str);
    return rc;
}

/*
** Return values of columns for the row at which the bfsvtab_cursor
** is currently pointing.
*/
static int bfsvtabColumn(
    sqlite3_vtab_cursor *cur,   /* The cursor */
    sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
    int i                       /* Which column to return */
) {
    int rc;
    sqlite3_str *s;
    char *c;
    bfsvtab_cursor *pCur = (bfsvtab_cursor*)cur;
    switch (i) {
        case BFSVTAB_COL_ID:
            sqlite3_result_int64(ctx, pCur->pCurrent->id);
            break;
        case BFSVTAB_COL_PARENT:
            if (pCur->pCurrent->id == pCur->root) {
                sqlite3_result_null(ctx);
            } else {
                sqlite3_result_int(ctx, pCur->pCurrent->parent);
            }
            break;
        case BFSVTAB_COL_DISTANCE:
            sqlite3_result_int(ctx, pCur->pCurrent->distance);
            break;
        case BFSVTAB_COL_SHORTEST_PATH:
            s = sqlite3_str_new(pCur->pVtab->db);
            rc = sqlite3_str_errcode(s);
            if (rc != SQLITE_OK) {
                sqlite3_str_finish(s);
                return rc;
            }
            rc = bfsvtabBuildShortestPathStr(s, pCur->pVisited, pCur->pCurrent->id);
            if (rc != SQLITE_OK) {
                sqlite3_str_finish(s);
                return rc;
            }
            c = sqlite3_str_finish(s);
            sqlite3_result_text(ctx, c, -1, SQLITE_TRANSIENT);
            sqlite3_free(c);
            break;
        case BFSVTAB_COL_ROOT:
            sqlite3_result_int(ctx, pCur->root);
            break;
        case BFSVTAB_COL_TABLENAME:
            sqlite3_result_text(ctx,
                    pCur->zTableName ?
                        pCur->zTableName : pCur->pVtab->zTableName,
                    -1, SQLITE_TRANSIENT);
            break;
        case BFSVTAB_COL_FROMCOLUMN:
            sqlite3_result_text(ctx,
                    pCur->zFromColumn ?
                        pCur->zFromColumn : pCur->pVtab->zFromColumn,
                    -1, SQLITE_TRANSIENT);
            break;
        case BFSVTAB_COL_TOCOLUMN:
            sqlite3_result_text(ctx,
                    pCur->zToColumn ?
                        pCur->zToColumn : pCur->pVtab->zToColumn,
                    -1, SQLITE_TRANSIENT);
            break;
        default:
            assert( i==BFSVTAB_COL_ORDER_BY_COLUMN );
            sqlite3_result_text(ctx,
                    pCur->zOrderByColumn ?
                        pCur->zOrderByColumn : pCur->pVtab->zOrderByColumn,
                    -1, SQLITE_TRANSIENT);
            break;
    }
    return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int bfsvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    bfsvtab_cursor *pCur = (bfsvtab_cursor*)cur;
    *pRowid = pCur->pCurrent->id;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int bfsvtabEof(sqlite3_vtab_cursor *cur) {
    bfsvtab_cursor *pCur = (bfsvtab_cursor*)cur;
    return pCur->pCurrent == 0 && pCur->pQueue.pFirst == 0;
}

/*
** This method is called to "rewind" the bfsvtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to bfsvtabColumn() or bfsvtabRowid() or 
** bfsvtabEof().
*/
static int bfsvtabFilter(
    sqlite3_vtab_cursor *pVtabCursor, 
    int idxNum, const char *idxStr,
    int argc, sqlite3_value **argv
){
    int rc = SQLITE_OK;
    bfsvtab_cursor *pCur = (bfsvtab_cursor *)pVtabCursor;
    bfsvtab_vtab *pVtab = (bfsvtab_vtab *)pCur->pVtab;
    char *zSql;
    const char *zTableName = pVtab->zTableName;
    const char *zFromColumn = pVtab->zFromColumn;
    const char *zToColumn = pVtab->zToColumn;
    const char *zOrderByColumn = pVtab->zOrderByColumn;
    bfsvtab_node *root;
    bfsvtab_avl *rootAvlNode;

    (void)idxStr;
    (void)argc;
    bfsvtabClearCursor(pCur);
    if ((idxNum & 1) == 0) {
        /* No root=$root in the WHERE clause.  Return an empty set */
        return SQLITE_OK;
    }
    if( (idxNum & 0x00f00) != 0) {
        zTableName = (const char*)sqlite3_value_text(argv[(idxNum>>8)&0x0f]);
        pCur->zTableName = sqlite3_mprintf("%s", zTableName);
    }
    if( (idxNum & 0x0f000) != 0) {
        zFromColumn = (const char*)sqlite3_value_text(argv[(idxNum>>12)&0x0f]);
        pCur->zFromColumn = sqlite3_mprintf("%s", zFromColumn);
    }
    if((idxNum & 0x0f0000) != 0) {
        zToColumn = (const char*)sqlite3_value_text(argv[(idxNum>>16)&0x0f]);
        pCur->zToColumn = sqlite3_mprintf("%s", zToColumn);
    }
    if((idxNum & 0xf00000) != 0) {
        zOrderByColumn = (const char*)sqlite3_value_text(argv[(idxNum>>20)&0x0f]);
        pCur->zOrderByColumn = sqlite3_mprintf("%s", zOrderByColumn);
    }

    if (zOrderByColumn) {
        zSql = sqlite3_mprintf(
            "SELECT \"%w\".\"%w\" FROM \"%w\" WHERE \"%w\".\"%w\"=?1 ORDER BY \"%w\".\"%w\"",
            zTableName, zToColumn, zTableName, zTableName, zFromColumn, zTableName, zOrderByColumn);
    } else {
        zSql = sqlite3_mprintf(
            "SELECT \"%w\".\"%w\" FROM \"%w\" WHERE \"%w\".\"%w\"=?1",
            zTableName, zToColumn, zTableName, zTableName, zFromColumn);
    }
    if (zSql == 0) {
        return SQLITE_NOMEM;
    }

    rc = sqlite3_prepare_v2(pVtab->db, zSql, -1, &pCur->pStmt, 0);
    sqlite3_free(zSql);
    if (rc) {
      sqlite3_free(pVtab->base.zErrMsg);
      pVtab->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(pVtab->db));
      return rc;
    }

    root = sqlite3_malloc(sizeof(*root));
    if (root == 0) {
        return SQLITE_NOMEM;
    }
    memset(root, 0, sizeof(*root));
    root->distance = 0;
    root->id = sqlite3_value_int64(argv[0]);
    root->parent = root->id;
    queuePush(&pCur->pQueue, root);

    pCur->pCurrent = 0;
    pCur->root = root->id;

    rootAvlNode = sqlite3_malloc(sizeof(*rootAvlNode));
    if (rootAvlNode == 0) {
        return SQLITE_NOMEM;
    }
    memset(rootAvlNode, 0, sizeof(*rootAvlNode));
    rootAvlNode->id = root->id;
    rootAvlNode->parent = root->id;
    pCur->pVisited = rootAvlNode;

    return bfsvtabNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
/*
** Search for terms of these forms:
**
**   (A)    root = $root
**   (B1)   distance < $distance
**   (B2)   distance <= $distance
**   (B3)   distance = $distance
**   (C)    tablename = $tablename
**   (D)    idcolumn = $idcolumn
**   (E)    parentcolumn = $parentcolumn
**
** 
**
**   idxNum       meaning
**   ----------   ------------------------------------------------------
**   0x00000001   Term of the form (A) found
**   0x00000002   The term of bit-2 is like (B1)
**   0x000000f0   Index in filter.argv[] of $depth.  0 if not used.
**   0x00000f00   Index in filter.argv[] of $tablename.  0 if not used.
**   0x0000f000   Index in filter.argv[] of $idcolumn.  0 if not used
**   0x000f0000   Index in filter.argv[] of $parentcolumn.  0 if not used.
**
** There must be a term of type (A).  If there is not, then the index type
** is 0 and the query will return an empty set.
*/
static int bfsvtabBestIndex(
    sqlite3_vtab *tab,
    sqlite3_index_info *pIdxInfo
) {
    int iPlan = 0;
    int i;
    int idx = 1;
    const struct sqlite3_index_constraint *pConstraint;
    bfsvtab_vtab *pVtab = (bfsvtab_vtab*)tab;
    double rCost = 10000000.0;

    pConstraint = pIdxInfo->aConstraint;
    for (i=0; i<pIdxInfo->nConstraint; i++, pConstraint++) {
        if (pConstraint->usable == 0) {
            continue;
        }
        if ((iPlan & 1) == 0 
            && pConstraint->iColumn == BFSVTAB_COL_ROOT
            && pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ) {

            iPlan |= 1;
            pIdxInfo->aConstraintUsage[i].argvIndex = 1;
            pIdxInfo->aConstraintUsage[i].omit = 1;
            rCost /= 100.0;
        }
        if ((iPlan & 0x0000f0) == 0
            && pConstraint->iColumn == BFSVTAB_COL_DISTANCE
            && (pConstraint->op == SQLITE_INDEX_CONSTRAINT_LT
            || pConstraint->op == SQLITE_INDEX_CONSTRAINT_LE
            || pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ)
            ) {

            iPlan |= idx<<4;
            pIdxInfo->aConstraintUsage[i].argvIndex = ++idx;
            if( pConstraint->op==SQLITE_INDEX_CONSTRAINT_LT ) iPlan |= 0x000002;
            rCost /= 5.0;
        }
        if ((iPlan & 0x000f00) == 0
            && pConstraint->iColumn == BFSVTAB_COL_TABLENAME
            && pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
            ) {

            iPlan |= idx<<8;
            pIdxInfo->aConstraintUsage[i].argvIndex = ++idx;
            pIdxInfo->aConstraintUsage[i].omit = 1;
            rCost /= 5.0;
        }
        if((iPlan & 0x00f000) == 0
            && pConstraint->iColumn == BFSVTAB_COL_FROMCOLUMN
            && pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
            ) {

            iPlan |= idx<<12;
            pIdxInfo->aConstraintUsage[i].argvIndex = ++idx;
            pIdxInfo->aConstraintUsage[i].omit = 1;
        }
        if ((iPlan & 0x0f0000) == 0
            && pConstraint->iColumn == BFSVTAB_COL_TOCOLUMN
            && pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
            ) {
            iPlan |= idx<<16;
            pIdxInfo->aConstraintUsage[i].argvIndex = ++idx;
            pIdxInfo->aConstraintUsage[i].omit = 1;
        }
        if ((iPlan & 0xf00000) == 0
            && pConstraint->iColumn == BFSVTAB_COL_ORDER_BY_COLUMN
            && pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
            ) {
            iPlan |= idx<<20;
            pIdxInfo->aConstraintUsage[i].argvIndex = ++idx;
            pIdxInfo->aConstraintUsage[i].omit = 1;
        }
    }
    if ((pVtab->zTableName == 0       && (iPlan & 0x000f00) == 0)
        || (pVtab->zFromColumn == 0     && (iPlan & 0x00f000) == 0)
        || (pVtab->zToColumn == 0 && (iPlan & 0x0f0000) == 0)
        ) {

        /* All of tablename, fromcolumn, and tocolumn must be specified
        ** in either the CREATE VIRTUAL TABLE or in the WHERE clause constraints
        ** or else the result is an empty set. */
        iPlan = 0;
    }
    if ((iPlan & 1) == 0) {
        /* If there is no usable "root=?" term, then set the index-type to 0.
        ** Also clear any argvIndex variables already set. This is necessary
        ** to prevent the core from throwing an "xBestIndex malfunction error"
        ** error (because the argvIndex values are not contiguously assigned
        ** starting from 1).  */
        rCost *= 1e30;
        for (i=0; i<pIdxInfo->nConstraint; i++, pConstraint++) {
            pIdxInfo->aConstraintUsage[i].argvIndex = 0;
        }
        iPlan = 0;
    }
    pIdxInfo->idxNum = iPlan;
    pIdxInfo->estimatedCost = rCost;

    return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module bfsvtabModule = {
    /* iVersion    */ 0,
    /* xCreate     */ bfsvtabConnect,
    /* xConnect    */ bfsvtabConnect,
    /* xBestIndex  */ bfsvtabBestIndex,
    /* xDisconnect */ bfsvtabDisconnect,
    /* xDestroy    */ bfsvtabDisconnect,
    /* xOpen       */ bfsvtabOpen,
    /* xClose      */ bfsvtabClose,
    /* xFilter     */ bfsvtabFilter,
    /* xNext       */ bfsvtabNext,
    /* xEof        */ bfsvtabEof,
    /* xColumn     */ bfsvtabColumn,
    /* xRowid      */ bfsvtabRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_bfsvtab_init(
    sqlite3 *db, 
    char **pzErrMsg, 
    const sqlite3_api_routines *pApi
) {
    int rc = SQLITE_OK;
    (void)pzErrMsg;
    SQLITE_EXTENSION_INIT2(pApi);
    rc = sqlite3_create_module(db, "bfsvtab", &bfsvtabModule, 0);
    return rc;
}
