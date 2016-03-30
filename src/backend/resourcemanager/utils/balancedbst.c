/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "utils/balancedbst.h"
#include "envswitch.h"
#include "errorcode.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "utils/memutilities.h"

/*******************************************************************************
 * BBST function implementation.
 ******************************************************************************/
/* Internal BBST macros and function declaration. */

#define                                     \
connectAsLeftChild(parent, child)           \
    (parent)->Left = (child); 				\
    if ( (child) != NULL ) 					\
        (child)->Parent = (parent);

#define                                     \
connectAsRightChild(parent, child)          \
    (parent)->Right = (child);              \
    if ( (child) != NULL )                  \
        (child)->Parent = (parent);

#define                                                                        \
disconnectChild(parent, child)                                                 \
    Assert( (child) != NULL &&                                                 \
            (parent) != NULL &&                                                \
            (child)->Parent == (parent));                                      \
    (parent)->Left  = (parent)->Left  == (child) ? NULL : (parent)->Left;      \
    (parent)->Right = (parent)->Right == (child) ? NULL : (parent)->Right;     \
    (child)->Parent = NULL;

#define                                                                        \
calculateNodeCount(node)                                                       \
    Assert( (node) != NULL );                                                  \
    (node)->NodeCount = ( (node)->Left  ? (node)->Left ->NodeCount : 0 ) +     \
                        ( (node)->Right ? (node)->Right->NodeCount : 0 ) +     \
                        1;

#define                                                                        \
calculateNodeDepth(node)                                                       \
    Assert( (node) != NULL );                                                  \
    (node)->Depth = (node)->Left ? (node)->Left->Depth : 0;					   \
	if ( (node)->Right ) {													   \
		(node)->Depth = (node)->Right->Depth > (node)->Depth ? 				   \
						(node)->Right->Depth :								   \
						(node)->Depth;										   \
	}																		   \
	(node)->Depth++;

#define                                                                        \
shiftupCalculateChildCountAndDepth(node)                                       \
    while( (node) != NULL ) {                                                  \
        calculateNodeCount((node));                                            \
        calculateNodeDepth((node));											   \
        (node) = (node)->Parent;											   \
    }

void rotateNodeBelowRight(BBSTNode nodea, BBSTNode *nodeptr);
void rotateNodeBelowLeft(BBSTNode nodea, BBSTNode *nodeptr);
void rebalanceBBST(BBST tree, BBSTNode node);

/*
 * Get balance factor of one BBST node, if the factor is less than 0, the right
 * child tree is heavier; Return value 0 means exactly balanced; The return
 * value larger than 0 means the left child tree is heavier.
 */
int getBalanceFactor(BBSTNode node)
{
    
	Assert( node != NULL );
    
	int ccleft  = node->Left  ? node->Left->Depth : 0;
	int ccright = node->Right ? node->Right->Depth : 0;
    
	return ccleft - ccright;
}

/* Rotate the tree nodea from right to left. */
void rotateNodeBelowLeft(BBSTNode nodea, BBSTNode *nodeptr)
{
    BBSTNode aparent = NULL;
    BBSTNode nodeb = NULL;
    BBSTNode nodec = NULL;
	int rightbfactor = 0;
    
    Assert( nodea != NULL );
    Assert( nodeptr != NULL );
    Assert( nodea->Right != NULL );
    
	rightbfactor = getBalanceFactor(nodea->Right);
	aparent = nodea->Parent;
	nodeb = nodea->Right;
    
    /****** case 1 ******
     *  A            C
     * a \          / \
     *    B  --->  A   B
     *   / b      a c d b
     *  C
     * c d
     ********************/
	if ( rightbfactor > 0 ) {
        /* Reorganize tree structure among A, B, C */
        nodec = nodeb->Left;
        connectAsRightChild(nodea,nodec->Left);
        connectAsLeftChild(nodeb,nodec->Right);
        connectAsRightChild(nodec,nodeb);
        connectAsLeftChild(nodec,nodea);
        /* Handle the parent node of tree A, B, C */
        *nodeptr = nodec;
        nodec->Parent = aparent;
        /* Handle children count number */
        calculateNodeCount(nodeb);
        calculateNodeCount(nodea);
        calculateNodeCount(nodec);
        /* Handle children depth number */
        calculateNodeDepth(nodeb);
        calculateNodeDepth(nodea);
        calculateNodeDepth(nodec);

	}
    
    /****** case 2 ******
     *  A            B
     * a \          / \
     *    B  --->  A   C
     *   b \      a b c d
     *      C
     *     c d
     ********************/
    else {
        /* Reorganize tree structure among A, B, C */
        nodec = nodeb->Right;
        connectAsRightChild(nodea,nodeb->Left);
        connectAsLeftChild(nodeb,nodea);
        /* Handle the parent node of tree A, B, C */
        *nodeptr = nodeb;
        nodeb->Parent = aparent;
        /* Handle children count number */
        calculateNodeCount(nodea);
        calculateNodeCount(nodeb);
        /* Handle children depth number */
        calculateNodeDepth(nodea);
        calculateNodeDepth(nodeb);
    }
}

/* Rotate the tree nodea from left to right. */
void rotateNodeBelowRight(BBSTNode nodea, BBSTNode *nodeptr)
{
	
	BBSTNode aparent = NULL;
	BBSTNode nodeb = NULL;
	BBSTNode nodec = NULL;
	int leftbfactor = 0;
    
	Assert( nodea != NULL );
	Assert( nodeptr != NULL );
	Assert( nodea->Left != NULL );
    
	
	leftbfactor = getBalanceFactor(nodea->Left);
	aparent = nodea->Parent;
	nodeb = nodea->Left;
    
	/****** case 1 ******
	 *    A          C
	 *   / a        / \
	 *  B     ---> B   A
 	 * b \        b c d a
	 *    C
	 *   c d
	 ********************/
	if ( leftbfactor < 0 ) {
		/* Reorganize tree structure among A, B, C */
		nodec = nodeb->Right;
		connectAsLeftChild(nodea,nodec->Right);
		connectAsRightChild(nodeb,nodec->Left);
		connectAsLeftChild(nodec,nodeb);
		connectAsRightChild(nodec,nodea);
		/* Handle the parent node of tree A, B, C */
		*nodeptr = nodec;
		nodec->Parent = aparent;
		/* Handle children count number */
		calculateNodeCount(nodeb);
		calculateNodeCount(nodea);
		calculateNodeCount(nodec);
		/* Handle children depth number */
		calculateNodeDepth(nodeb);
		calculateNodeDepth(nodea);
		calculateNodeDepth(nodec);
	}
    
	/****** case 2 ******
     *      A        B
     *     / a      / \
     *    B   ---> C   A
     *   / b      c d b a
     *  C
	 * c d
     ********************/
	else {
        /* Reorganize tree structure among A, B, C */
        nodec = nodeb->Left;
		connectAsLeftChild(nodea,nodeb->Right);
        connectAsRightChild(nodeb,nodea);
        /* Handle the parent node of tree A, B, C */
        *nodeptr = nodeb;
        nodeb->Parent = aparent;
        /* Handle children count number */
        calculateNodeCount(nodea);
        calculateNodeCount(nodeb);
        /* Handle children depth number */
        calculateNodeDepth(nodea);
        calculateNodeDepth(nodeb);
	}
	
}

/* 
 * Check and balance the tree node after inserting or erasing node.
 */
void rebalanceBBST(BBST tree, BBSTNode node)
{
    
	int 	  balancefact  = 0;
	BBSTNode  p 			= node;
	BBSTNode *childptr 		= NULL;
    
	Assert( tree != NULL );
	Assert( node != NULL );
	Assert( tree->Root != NULL );
    
	while ( p != NULL ) {
		balancefact = getBalanceFactor(p);
		if ( balancefact > 1 || balancefact < -1 ) {
			/* Decide the pointer pointing to the node to rotate. */
			childptr = NULL;
			if ( p->Parent != NULL ) {
				if ( p->Parent->Left == p ) {
					childptr = &(p->Parent->Left);
				}
				else {
					childptr = &(p->Parent->Right);
				}
			}
			else {
				childptr = &tree->Root;
			}
			Assert( childptr != NULL );
            
			if ( balancefact > 1 )
				rotateNodeBelowRight(p, childptr);
			else if ( balancefact < -1 )
				rotateNodeBelowLeft(p, childptr);
		}
		
		p = p->Parent;
	}
}


/*******************************************************************************
 * External BBST function implementation 
 ******************************************************************************/

/*
 * Create BBST.
 */
BBST createBBST(MCTYPE         		  context,
                void				 *comparg,
                CompareFunctionType   compfunc)
{
    BBST res = NULL;

    Assert(compfunc != NULL);

    res = (BBST)rm_palloc0(context, sizeof(struct BBSTData));
    initializeBBST(res, context, comparg, compfunc);
    return res;
}

void initializeBBST(BBST				  tree,
					MCTYPE         		  context,
                    void				 *comparg,
                    CompareFunctionType   compfunc)
{

	tree->Context 		 = context;
	tree->NodeIndex		 = createHASHTABLE(context,
    									   HASHTABLE_SLOT_VOLUME_DEFAULT,
    									   HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
    									   HASHTABLE_KEYTYPE_VOIDPT,
    									   NULL); /* No need to free value from
    									   	         hash table. */
    tree->DataCompFunc 	 = compfunc;
    tree->DataCompArg	 = comparg;
}

/*
 * Node creation helper.
 */
BBSTNode createBBSTNode(BBST tree, void *data)
{
    BBSTNode        res         = NULL;
    
	Assert( tree != NULL );
	Assert( data != NULL );

    /* Check if freed nodes can be reused to avoid memory allocation cost. */
    if ( tree->Free != NULL ) {
        res = tree->Free;
        tree->Free = tree->Free->Right;
        res->Right = NULL;
    }
    else {
        res = (BBSTNode)
        	  rm_palloc0( tree->Context,
        			  	  	    sizeof(struct BBSTNodeData));
        /* Expect that palloc0 set the attributes: Left, Right, Parent to NULLs.
         */
    }
    
    /* Set node properties. */
    res->Data 		= data;
    res->NodeCount  = 1;
    res->Depth 		= 1;
    return res;
}

/*
 * Free one BBST node and save it in the free list of the BBST.
 */

/*
void freeBBSTNode(BBST tree, BBSTNode node)
{

    Assert( tree != NULL );
    Assert( node != NULL );
    
    node->Data 		= NULL;
    node->Left 		= NULL;
    node->Parent 	= NULL;
    node->NodeCount = -1;
    node->Depth 	= 0;
    
    node->Right = tree->Free;
    tree->Free = node->Right;
    
}
*/

BBSTNode getBBSTNode(BBST tree, void *data)
{
	PAIR pair = getHASHTABLENode(tree->NodeIndex, data);
	if ( pair == NULL )
		return NULL;
	return pair->Value;
}

/*
 * Insert one node into BBST, return true if the node is inserted without error,
 * the BST will be rebalanced if necessary to ensure high searching performance.
 * Once the BBST is updated ( at least one node is deleted/inserted), the itera-
 * ter is expired.
 */
int insertBBSTNode(BBST tree, BBSTNode node)
{
	int      comp = 0;
    BBSTNode p    = NULL;
    
	Assert( tree != NULL );
	Assert( node != NULL );
	Assert( tree->DataCompFunc != NULL );
	Assert( tree->NodeIndex != NULL );

	PAIR pair = getHASHTABLENode(tree->NodeIndex, node->Data);
	if ( pair != NULL )
		return UTIL_BBST_DUPLICATE_VALUE;	/* Can't insert the duplicate val.*/

	/* Save node into the hash table. */
    setHASHTABLENode(tree->NodeIndex,
    				 node->Data,
    				 node,
    				 false);			/* Should no old value to free.   */

	if ( tree->Root == NULL ) {
        tree->Root = node;
        node->Parent = NULL;
	}
	else {
		p = tree->Root;
		while( p != NULL ) {
			comp = (tree->DataCompFunc)( tree->DataCompArg,
										 p->Data,
										 node->Data);

			if ( comp < 0 ) {				/* The node to insert is larger   */
				if ( p->Right != NULL ) {
                    p = p->Right;
				}
				else {
					connectAsRightChild(p, node);
					break;
				}
			}
			else {   /* Equal or greater case  */

				if ( p->Left != NULL ) {
					p = p->Left;
				}
				else {
					connectAsLeftChild(p, node);
					break;
				}
            }
        }
    
        Assert( p != NULL );

        /* Adjust the children node counter of all ancesters */
        shiftupCalculateChildCountAndDepth(p);
    
        /* Bottom-up check BST balance property and rebalance if necessary */
        rebalanceBBST(tree, node);
    }

    return FUNC_RETURN_OK;
}

/* 
 * Remove one node from BBST specified by node. The BST is also rotated if after
 * removal the balance is broken. Once the BBST is updated (at least one node is 
 * deleted/inserted), the iterater is expired.
 */
int removeBBSTNode(BBST tree, BBSTNode *pnode)
{
    BBSTNode node     = NULL;
	BBSTNode parent   = NULL;
	BBSTNode toremove = NULL;
	BBSTNode p		  = NULL;
    
	Assert( tree != NULL );
	Assert( tree->Root != NULL );
	Assert( (*pnode) != NULL );
    
    node   = *pnode;
    parent = node->Parent;
    
	/* Check if the node is in the tree。      */
    if( getHASHTABLENode(tree->NodeIndex, node->Data) == NULL ) {
    	return UTIL_BBST_NOT_IN_THIS_TREE;
    }
    
	if ( tree->Root == node ) {
		/* If we have only one node in the tree, it must be the one to remove.*/
		if ( tree->Root->NodeCount == 1 ) {
			tree->Root = NULL;
			clearHASHTABLE(tree->NodeIndex);
			return FUNC_RETURN_OK;
		}
		else if ( tree->Root->Right == NULL && tree->Root->Left != NULL) {
			tree->Root = node->Left;
			tree->Root->Parent = NULL;
			removeHASHTABLENode(tree->NodeIndex, node->Data);
			node->Left = NULL;
			return FUNC_RETURN_OK;
		}
		else if ( tree->Root->Right != NULL && tree->Root->Left == NULL) {
			tree->Root = node->Right;
			tree->Root->Parent = NULL;
			removeHASHTABLENode(tree->NodeIndex, node->Data);
			node->Right = NULL;
			return FUNC_RETURN_OK;
		}
	}
    
	/* Remove the node */
    
	/* Case 1: The leaf node to delete. */
	if ( node->Left == NULL && node->Right == NULL ) {
		removeHASHTABLENode(tree->NodeIndex, node->Data);
		disconnectChild(parent, node);

		p = parent;
		shiftupCalculateChildCountAndDepth(p);
		rebalanceBBST(tree, parent);
		return FUNC_RETURN_OK;
	}
	/* Case 2.1: One branch node with only one left leaf node. */
	else if ( node->Left != NULL && node->Right == NULL) {
		removeHASHTABLENode(tree->NodeIndex, node->Data);
		bool connleft = parent->Left == node ? true : false;
		disconnectChild(parent, node);
		if ( connleft ) {
			connectAsLeftChild(parent, node->Left);
		}
		else {
			connectAsRightChild(parent, node->Left);
		}
		p = parent;
		shiftupCalculateChildCountAndDepth(p);
		rebalanceBBST(tree, parent);
		node->Left = NULL;
		return FUNC_RETURN_OK;
	}
	/* Case 2.2: one branch node with only one right leaf node. */
	else if ( node->Right != NULL && node->Left == NULL) {
		removeHASHTABLENode(tree->NodeIndex, node->Data);
		bool connleft = parent->Left == node ? true : false;
		disconnectChild(parent, node);
		if ( connleft ) {
			connectAsLeftChild(parent, node->Right);
		}
		else {
			connectAsRightChild(parent, node->Right);
		}
		p = parent;
		shiftupCalculateChildCountAndDepth(p);
		rebalanceBBST(tree, parent);
		node->Right = NULL;
		return FUNC_RETURN_OK;
	}
    
	/*
	 * Case 3: one branch node with two leaf nodes.We choose the right most leaf
	 * node in the left child tree to replace the node to delete. toremove saves
     * the node to actually be removed in BBST. This is done by recursively call
     * this function again. In this case,the node toremove has at most one child 
     * tree. The recursion depth is only 2.Thus,No need to worry about the stack
     * overflow.
     */
	toremove = node->Left;
	while( toremove->Right != NULL ) {
		toremove = toremove->Right;
	}
	
	void *tmpval = node->Data;
	node->Data = toremove->Data;
	toremove->Data = tmpval;

	setHASHTABLENode(tree->NodeIndex, toremove->Data, toremove, false);
	setHASHTABLENode(tree->NodeIndex, node->Data, node, false);

    *pnode = toremove;
	return removeBBSTNode(tree, pnode);
}

int	reorderBBSTNodeData(BBST tree, void *data)
{
	int		 res	= FUNC_RETURN_OK;
	BBSTNode node 	= NULL;

	node = getBBSTNode(tree, data);
	if ( node == NULL )
	{
		return UTIL_BBST_NOT_IN_THIS_TREE;
	}

	res = removeBBSTNode(tree, &node);
	if ( res != FUNC_RETURN_OK )
	{
		Assert(false);
	}

	res = insertBBSTNode(tree, node);
	if ( res != FUNC_RETURN_OK )
	{
		Assert(false);
	}

	return res;
}

int getBBSTNodeCount(BBST tree)
{
	return tree->Root == NULL ? 0 : tree->Root->NodeCount;
}

/*
void freeBBST(BBST tree)
{
	clearBBST(tree);
	freeBBSTFreeNodes(tree);
	freeHASHTABLE(tree->NodeIndex);

	rm_pfree(tree->Context, tree);
}

void clearBBST(BBST tree)
{
	Assert( tree != NULL );
	while( tree->Root != NULL ) {
		BBSTNode node = tree->Root;
		removeBBSTNode(tree, &node);
		freeBBSTNode(tree, node);
	}
}
*/

int traverseBBSTMidOrder(BBST tree, DQueue lines)
{
	BBSTNode 	p 		= tree->Root;
	DQueueData 	stack;

	initializeDQueue(&stack, tree->Context);

	while( stack.NodeCount > 0 || p != NULL ) {
		if ( p != NULL ) {
			insertDQueueHeadNode(&stack, p);
			p = p->Left;
		}
		else {
				p = (BBSTNode)removeDQueueHeadNode(&stack);
				insertDQueueTailNode(lines, p);
				p = p->Right;
		}
	}

	Assert(stack.NodeCount == 0);
    removeAllDQueueNodes(&stack);
	cleanDQueue(&stack);

	return FUNC_RETURN_OK;
}

BBSTNode getLeftMostNode(BBST tree)
{
	BBSTNode res = tree->Root;
	if ( res == NULL )
		return res;

	while( res->Left != NULL ) {
		res = res->Left;
	}

	return res;
}
