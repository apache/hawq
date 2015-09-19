#ifndef BALANCED_BINARY_SEARCH_TREE_H
#define BALANCED_BINARY_SEARCH_TREE_H

/********************************************************************************
 * OVERVIEW of balanced binary search tree (BBST)
 *
 * This header file defines necessary data structures and APIs for operating  one
 * balanced binary search tree (BBST). This tree is generally used by dynamic re-
 * source manager for quickly get range of a list of nodes. This can also be used
 * for another purposes.
 *
 * BBST guarantees that whenever the caller inserts/deletes the nodes,the tree is
 * kept balanced.  Therefore,  whenever the caller wants to get the range [begin,
 * end] of the node and search for top N range. LOG(n) complexity is guaranteed.
 *******************************************************************************/

#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/hashtable.h"

struct BBSTNodeData;
struct BBSTData;

typedef struct BBSTNodeData *BBSTNode;
typedef struct BBSTNodeData  BBSTNodeData;

typedef struct BBSTData     *BBST;
typedef struct BBSTData		 BBSTData;


/* Node of the BST. */
struct BBSTNodeData {
	void 	   	   *Data;               /* The pointer to the actual data    */

	BBSTNode		Left;               /* Left child                        */
	BBSTNode		Right;              /* Right child                       */
	BBSTNode		Parent;             /* Parent for fast traverse          */
	int		 		NodeCount;          /* Child count including itself      */
	int				Depth;				/* Maximum child tree depth	 		 */
};

/* BBST Comparison method and arguments. */
typedef int  (* CompareFunctionType  )(void *,void *,void *);

/* A BBST instance. */
struct BBSTData {
	MCTYPE					Context;				/* Memory context        */
	BBSTNode                Root;					/* Root of BBST          */
    BBSTNode                Free;                   /* BBST node for reuse   */
	CompareFunctionType     DataCompFunc;       	/* Compare function      */
	void				   *DataCompArg;			/* Compare function arg  */
	HASHTABLE				NodeIndex; 				/* Index to each node	 */
};

#define UTIL_BBST_DUPLICATE_VALUE  1
#define UTIL_BBST_NOT_IN_THIS_TREE 2

/* Create BBST */
BBST     createBBST(MCTYPE         		  context,
                    void				 *comparg,
                    CompareFunctionType   compfunc);

void initializeBBST(BBST				  tree,
					MCTYPE         		  context,
                    void				 *comparg,
                    CompareFunctionType   compfunc);

int      getBBSTNodeCount(BBST tree);
int  	 getBalanceFactor(BBSTNode node);

/* Node creation/free helper */
BBSTNode createBBSTNode(BBST tree, void *data);
void     freeBBSTNode(BBST tree, BBSTNode node);
BBSTNode getBBSTNode(BBST tree, void *data);

/* Insert / Remove one node identified by the node address node */
int      insertBBSTNode(BBST tree, BBSTNode  node);
int      removeBBSTNode(BBST tree, BBSTNode *node);

int		 reorderBBSTNodeData(BBST tree, void *data);

/* Get count of node with no less value than specified criterion */
int countBBSTNodeNoLessThan(BBST tree, void * critirion);

void     freeBBST(BBST tree);
void     clearBBST(BBST tree);
void	 freeBBSTFreeNodes(BBST tree);

/* Get node */
BBSTNode getRightMostNode(BBST tree);
BBSTNode getLeftMostNode(BBST tree);

int getMaxDepthofBBST(BBST tree);
int traverseBBSTPreOrder(BBST tree, DQueue lines, int maxcount);
int traverseBBSTMidOrder(BBST tree, DQueue lines);

#endif //BALANCED_BINARY_SEARCH_TREE_H
