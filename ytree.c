/*
 * -----------------------------  ytree.c  ------------------------------
 *
 * Copyright (c) 2016, Yorick de Wid <yorick17 at outlook dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * ytree implementation.
 *
 * This implementation demonstrates the B+ tree data structure
 * for educational purposes, including insertion, deletion, search, and display
 * of the search path, the leaves, or the whole tree.
 *  
 * Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 * Usage:  ytree [order]
 * where order is an optional argument
 * (integer MIN_ORDER <= order <= MAX_ORDER)
 * defined as the maximal number of pointers in any node.
 *
 * TODO
 * - Error handling
 * - Public API
 * - Supply config
 * - Persistent extension
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

/* Algorithm version */
#define VERSION "1.0"

/* Default order is 4 */
#define DEFAULT_ORDER 4

/* Enable for debug compilation */
//#define DEBUG 1

/* Enable for utilities and output compilation */
#define UTILITIES 1

/* 
 * Minimum order is necessarily 3. We set the maximum
 * order arbitrarily. You may change the maximum order.
 */
#define MIN_ORDER 3
#define MAX_ORDER 100

/* ********************************
 * TYPES
 * ********************************/

/*
 * Datatypes are used to
 * identify record values.
 */
enum datatype {
	DT_CHAR,
	DT_INT,
	DT_FLOAT,
	DT_DATA,
};

/*
 * Type representing the record
 * to which a given key refers.
 * The record can hold native types
 * or user defined structures. These
 * structures must be serializable.
 */
typedef struct {
	union {
		char _char;
		int _int;
		float _float;
		void *_data;
	} value;
	enum datatype value_type;
	size_t value_size;
} record_t;

/*
 * Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers holding the values. The relation between
 * keys and pointers differs between leaves and
 * internal nodes. In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs. The last pointer points to the
 * sequencial leaf to the right (or NULL in the
 * case of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array. Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next sequential 
 * leaf.
 */
typedef struct node {
	void **pointers;		// Array of pointers to records
	int *keys;				// Array of keys with size: order 
	struct node *parent;	// Parent node or NULL for root
	bool is_leaf;			// Internal node or leaf
	int num_keys;			// Number of keys in node
	struct node *next;		// Used for queue
} node_t;

/* ********************************
 * GLOBALS
 * ********************************/

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
node_t *queue = NULL;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
void (*release_callback)(void *) = NULL;

/* ********************************
 * FUNCTION PROTOTYPES
 * ********************************/

/* Helpers */
static void enqueue(node_t * new_node);
static node_t *dequeue();
int height(node_t *root);
int path_to_root(node_t *root, node_t *child);

/* Output */
void ytree_print_leaves(node_t *root);
void ytree_print_tree(node_t *root);
void find_and_print(node_t *root, int key, bool verbose); 
void find_and_print_range(node_t *root, int range1, int range2, bool verbose); 
int find_range(node_t *root, int key_start, int key_end, bool verbose, int returned_keys[], void *returned_pointers[]); 
node_t *find_leaf(node_t *root, int key, bool verbose);
record_t *find(node_t *root, int key, bool verbose);

/* Insertion */
record_t *make_record(enum datatype type, char c_value, int i_value, float f_value, void *p_value, size_t vsize);
static node_t *make_node();
static node_t *make_leaf();
static int get_left_index(node_t *parent, node_t *left);
static node_t *insert_into_leaf(node_t *leaf, int key, record_t *pointer );
static node_t *insert_into_leaf_after_splitting(node_t *root, node_t *leaf, int key, record_t *pointer);
static node_t *insert_into_node(node_t *root, node_t *parent, int left_index, int key, node_t * right);
static node_t *insert_into_node_after_splitting(node_t * root, node_t * parent, int left_index, int key, node_t * right);
static node_t *insert_into_parent(node_t * root, node_t * left, int key, node_t * right);
static node_t *insert_into_new_root(node_t * left, int key, node_t * right);
static node_t *start_new_tree(int key, record_t * pointer);
node_t *ytree_insert(node_t *root, int key, record_t *pointer);

/* Deletion */
int get_neighbor_index( node_t * n );
node_t *adjust_root(node_t * root);
node_t *coalesce_nodes(node_t * root, node_t * n, node_t * neighbor,
                      int neighbor_index, int k_prime);
node_t *redistribute_nodes(node_t * root, node_t * n, node_t * neighbor,
                          int neighbor_index,
		int k_prime_index, int k_prime);
node_t *delete_entry( node_t * root, node_t * n, int key, void * pointer );
node_t *delete( node_t * root, int key );

/* ********************************
 * HELPERS
 * ********************************/

/*
 * Helper function for printing the
 * tree. See print_tree.
 */
static void enqueue(node_t *new_node) {
	node_t *c;
	if (queue == NULL) {
		queue = new_node;
		queue->next = NULL;
	} else {
		c = queue;
		while (c->next != NULL) {
			c = c->next;
		}
		c->next = new_node;
		new_node->next = NULL;
	}
}

/*
 * Helper function for printing the
 * tree. See print_tree.
 */
static node_t *dequeue() {
	node_t *n = queue;
	queue = queue->next;
	n->next = NULL;
	return n;
}

/* TODO: make define
 * Finds the appropriate place to
 * split an even or uneven number
 * into two.
 */
static int cut(int length) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}

/* ********************************
 * OUTPUT
 * ********************************/

/*
 * Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void ytree_print_leaves(node_t *root) {
	int i;
	node_t *c = root;
	if (!root) {
		printf("Empty tree.\n");
		return;
	}

	/* Find leftmost leave */
	while (!c->is_leaf)
		c = c->pointers[0];

	while (true) {
		for (i = 0; i < c->num_keys; ++i) {
			if (verbose_output)
				printf("%x ", (unsigned int)(uintptr_t)c->pointers[i]);
			printf("%d ", c->keys[i]);
		}
		if (verbose_output)
			printf("%x ", (unsigned int)(uintptr_t)c->pointers[order - 1]);
		if (c->pointers[order - 1] != NULL) {
			printf(" | ");
			c = c->pointers[order - 1];
		} else
			break;
	}

	printf("\n");
}

/*
 * Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(node_t *root) {
	int h = 0;
	node_t *c = root;
	while (!c->is_leaf) {
		c = c->pointers[0];
		h++;
	}
	return h;
}

/*
 * Utility function to give the length in edges
 * of the path from any node to the root.
 */
int path_to_root(node_t *root, node_t *child) {
	int length = 0;
	node_t *c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}

/*
 * Prints the B+ tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void ytree_print_tree(node_t *root) {
	int i = 0;
	int rank = 0;
	int new_rank = 0;

	if (!root) {
		printf("Empty tree\n");
		return;
	}

	queue = NULL;
	enqueue(root);
	while (queue) {
		node_t *n = dequeue();
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_rank = path_to_root( root, n );
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		if (verbose_output) 
			printf("(%x)", (unsigned int)(uintptr_t)n);
		for (i = 0; i < n->num_keys; i++) {
			if (verbose_output)
				printf("%x ", (unsigned int)(uintptr_t)n->pointers[i]);
			printf("%d ", n->keys[i]);
		}
		if (!n->is_leaf)
			for (i = 0; i <= n->num_keys; i++)
				enqueue(n->pointers[i]);
		if (verbose_output) {
			if (n->is_leaf) 
				printf("%x ", (unsigned int)(uintptr_t)n->pointers[order - 1]);
			else
				printf("%x ", (unsigned int)(uintptr_t)n->pointers[n->num_keys]);
		}
		printf("| ");
	}
	printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(node_t * root, int key, bool verbose) {
	record_t * r = find(root, key, verbose);
	if (!r)
		printf("Key: %d  Record: NULL\n", key);
	else 
		printf("Key: %d  Record: %d\n", key, r->value._int);
}


/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
void find_and_print_range(node_t * root, int key_start, int key_end, bool verbose) {
	int i;
	int array_size = key_end - key_start + 1;
	int *returned_keys = (int *)malloc(array_size);
	void **returned_pointers = malloc(array_size);
	int num_found = find_range( root, key_start, key_end, verbose,
			returned_keys, returned_pointers);
	if (!num_found)
		printf("None found\n");
	else {
		for (i = 0; i < num_found; i++)
			printf("Key: %d  Record: %d\n", returned_keys[i], ((record_t *)returned_pointers[i])->value._int);
	}

	free(returned_keys);
}


/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(node_t * root, int key_start, int key_end, bool verbose,
		int *returned_keys, void **returned_pointers) {
	int i, num_found;
	num_found = 0;
	node_t * n = find_leaf( root, key_start, verbose );
	if (n == NULL) return 0;
	for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++) ;
	if (i == n->num_keys) return 0;
	while (n != NULL) {
		for (; i < n->num_keys && n->keys[i] <= key_end; ++i) {
			returned_keys[num_found] = n->keys[i];
			returned_pointers[num_found] = n->pointers[i];
			num_found++;
		}
		n = n->pointers[order - 1];
		i = 0;
	}
	return num_found;
}

/*
 * Traces the path from the root to a leaf, searching
 * by key. Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
node_t *find_leaf(node_t *root, int key, bool verbose) {
	int i = 0;
	node_t *c = root;
	if (!c) {
		if (verbose) 
			printf("Empty tree\n");

		return NULL;
	}

	while (!c->is_leaf) {
		if (verbose) {
			printf("[");
			for (i = 0; i < c->num_keys - 1; ++i)
				printf("%d ", c->keys[i]);
			printf("%d] ", c->keys[i]);
		}

		i = 0;
		while (i < c->num_keys) {
			if (key >= c->keys[i])
				++i;
			else
				break;
		}
		
		if (verbose)
			printf("%d ->\n", i);

		c = (node_t *)c->pointers[i];
	}

	if (verbose) {
		printf("Leaf [");
		for (i = 0; i < c->num_keys - 1; i++)
			printf("%d ", c->keys[i]);
		printf("%d] ->\n", c->keys[i]);
	}

	return c;
}

/*
 * Finds and returns the record to which
 * a key refers.
 */
record_t *find(node_t *root, int key, bool verbose) {
	int i = 0;
	node_t *c = find_leaf(root, key, verbose);
	if (!c)
		return NULL;

	for (i = 0; i < c->num_keys; ++i)
		if (c->keys[i] == key)
			break;

	if (i == c->num_keys) 
		return NULL;
	
	return (record_t *)c->pointers[i];
}

/* ********************************
 * INSERTION
 * ********************************/

/* 
 * Creates a new record to hold the value
 * to which a key refers.
 */
record_t *make_record(enum datatype type, char c_value, int i_value, float f_value, void *p_value, size_t vsize) {
	record_t *new_record = (record_t *)malloc(sizeof(record_t));
	if (!new_record) {
		perror("Record creation");
		exit(EXIT_FAILURE);
	} else {
		switch (type) {
			case DT_CHAR:
				new_record->value._char = c_value;
				break;
			case DT_INT:
				new_record->value._int = i_value;
				break;
			case DT_FLOAT:
				new_record->value._float = f_value;
				break;
			case DT_DATA:
				new_record->value._data = p_value;
				break;
		}
		new_record->value_type = type;
		new_record->value_size = vsize;
	}

	return new_record;
}

/* 
 * Helper macros for easy record
 * creation.
 */
#define make_record_char(c) make_record(DT_CHAR, c, 0, 0, NULL, 0)
#define make_record_int(i) make_record(DT_INT, 0, i, 0, NULL, 0)
#define make_record_float(f) make_record(DT_FLOAT, 0, 0, f, NULL, 0)
#define make_record_data(d,n) make_record(DT_FLOAT, 0, 0, 0, d, n)

/*
 * Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
static node_t *make_node() {
	node_t *new_node = (node_t *)malloc(sizeof(node_t));
	if (!new_node) {
		perror("Node creation.");
		exit(EXIT_FAILURE);
	}

	new_node->keys = malloc((order - 1) * sizeof(int));
	if (!new_node->keys) {
		perror("New node keys array.");
		exit(EXIT_FAILURE);
	}

	new_node->pointers = malloc(order * sizeof(void *));
	if (new_node->pointers == NULL) {
		perror("New node pointers array.");
		exit(EXIT_FAILURE);
	}

	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
	return new_node;
}

/* 
 * Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
static node_t *make_leaf() {
	node_t *leaf = make_node();
	leaf->is_leaf = true;
	return leaf;
}

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
static int get_left_index(node_t *parent, node_t *left) {
	int left_index = 0;
	while (left_index <= parent->num_keys && 
			parent->pointers[left_index] != left)
		left_index++;

	return left_index;
}

/*
 * Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
static node_t *insert_into_leaf(node_t *leaf, int key, record_t *pointer) {
	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}

	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = pointer;
	leaf->num_keys++;
	return leaf;
}

/* 
 * Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
static node_t *insert_into_leaf_after_splitting(node_t *root, node_t *leaf, int key, record_t *pointer) {
	node_t *new_leaf = make_leaf();

	int *temp_keys = (int *)malloc(order * sizeof(int));
	if (!temp_keys) {
		perror("Temporary keys array.");
		exit(EXIT_FAILURE);
	}

	void **temp_pointers = malloc(order * sizeof(void *));
	if (!temp_pointers) {
		perror("Temporary pointers array.");
		exit(EXIT_FAILURE);
	}

	int insertion_index = 0;
	while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	int i, j;
	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index)
			j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = pointer;

	leaf->num_keys = 0;

	int split = cut(order - 1);

	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < order; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	free(temp_pointers);
	free(temp_keys);

	/* Cheate the sequence chain */
	new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
	leaf->pointers[order - 1] = new_leaf;

	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pointers[i] = NULL;

	new_leaf->parent = leaf->parent;

	return insert_into_parent(root, leaf, new_leaf->keys[0], new_leaf);
}

/*
 * Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
static node_t *insert_into_node(node_t *root, node_t *n, 
		int left_index, int key, node_t *right) {
	int i;
	for (i = n->num_keys; i > left_index; i--) {
		n->pointers[i + 1] = n->pointers[i];
		n->keys[i] = n->keys[i - 1];
	}

	n->pointers[left_index + 1] = right;
	n->keys[left_index] = key;
	n->num_keys++;
	return root;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
static node_t *insert_into_node_after_splitting(node_t *root, node_t *old_node, int left_index, 
		int key, node_t *right) {

	node_t *child;

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */
	node_t **temp_pointers = malloc((order + 1) * sizeof(node_t *));
	if (!temp_pointers) {
		perror("Temporary pointers array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	int *temp_keys = malloc(order * sizeof(int));
	if (!temp_keys) {
		perror("Temporary keys array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	int i, j;
	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	/*
	 * Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */  
	int split = cut(order);
	node_t *new_node = make_node();
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	int k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}

	/*
	 * Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */
	return insert_into_parent(root, old_node, k_prime, new_node);
}

/* 
 * Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
static node_t *insert_into_parent(node_t *root, node_t *left, int key, node_t *right) {
	node_t *parent = left->parent;

	/* Case: new root. */
	if (!parent)
		return insert_into_new_root(left, key, right);

	/*
	 * Case: leaf or node.
	 * (Remainder of function body.)  
	 */

	/*
	 * Find the parent's pointer to the left 
	 * node.
	 */
	int left_index = get_left_index(parent, left);

	/*
	 * Simple case: the new key fits into the node. 
	 */
	if (parent->num_keys < order - 1)
		return insert_into_node(root, parent, left_index, key, right);

	/* 
	 * Harder case: split a node in order 
	 * to preserve the B+ tree properties.
	 */
	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}

/* 
 * Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
static node_t *insert_into_new_root(node_t *left, int key, node_t *right) {
	node_t * root = make_node();
	root->keys[0] = key;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->num_keys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}

/*
 * First insertion:
 * start a new tree.
 */
static node_t *start_new_tree(int key, record_t *pointer) {
	node_t *root = make_leaf();
	root->keys[0] = key;
	root->pointers[0] = pointer;
	root->pointers[order - 1] = NULL;
	root->parent = NULL;
	root->num_keys++;
	return root;
}

/*
 * Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
node_t *ytree_insert(node_t *root, int key, record_t *pointer) {
	/*
	 * The current implementation ignores
	 * duplicates.
	 */
	if (find(root, key, false) != NULL)
		return root;

	/*
	 * Sanity check
	 */
	if (!pointer)
		return NULL;

	/*
	 * Case: the tree does not exist yet.
	 * Start a new tree.
	 */
	if (!root) 
		return start_new_tree(key, pointer);

	/* 
	 * Case: the tree already exists.
	 * (Rest of function body.)
	 */
	node_t *leaf = find_leaf(root, key, false);

	/* 
	 *Case: leaf has room for key and pointer.
	 */
	if (leaf->num_keys < order - 1) {
		leaf = insert_into_leaf(leaf, key, pointer);
		return root;
	}

	/*
	 * Case: leaf must be split.
	 */
	return insert_into_leaf_after_splitting(root, leaf, key, pointer);
}

/* ********************************
 * DELETION
 * ********************************/

/* 
 * Utility function for deletion. Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(node_t *n) {
	int i;

	/* Return the index of the key to the left
	 * of the pointer in the parent pointing
	 * to n.  
	 * If n is the leftmost child, this means
	 * return -1.
	 */
	for (i = 0; i <= n->parent->num_keys; ++i)
		if (n->parent->pointers[i] == n)
			return i - 1;

	// Error state.
	printf("Search for nonexistent pointer to node in parent.\n");
	printf("Node:  %#x\n", (unsigned int)(uintptr_t)n);
	exit(EXIT_FAILURE);
}

node_t *remove_entry_from_node(node_t *n, int key, node_t *pointer) {
	int i, num_pointers;

	// Remove the key and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != key)
		i++;
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	// Remove the pointer and shift other pointers accordingly.
	// First determine number of pointers.
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];


	// One key fewer.
	n->num_keys--;

	// Set the other pointers to NULL for tidiness.
	// A leaf uses the last pointer to point to the next leaf.
	if (n->is_leaf)
		for (i = n->num_keys; i < order - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->num_keys + 1; i < order; i++)
			n->pointers[i] = NULL;

	return n;
}

node_t *adjust_root(node_t *root) {
	node_t *new_root;

	/* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */
	if (root->num_keys > 0)
		return root;

	/* Case: empty root. 
	 */

	// If it has a child, promote 
	// the first (only) child
	// as the new root.

	if (!root->is_leaf) {
		new_root = root->pointers[0];
		new_root->parent = NULL;
	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else
		new_root = NULL;

	free(root->keys);
	free(root->pointers);
	free(root);

	return new_root;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
node_t *coalesce_nodes(node_t * root, node_t * n, node_t * neighbor, int neighbor_index, int k_prime) {

	int i, j, neighbor_insertion_index, n_end;
	node_t * tmp;

	/* Swap neighbor with node if node is on the
	 * extreme left and neighbor is to its right.
	 */

	if (neighbor_index == -1) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	/* Starting point in the neighbor for copying
	 * keys and pointers from n.
	 * Recall that n and neighbor have swapped places
	 * in the special case of n being a leftmost child.
	 */

	neighbor_insertion_index = neighbor->num_keys;

	/* Case:  nonleaf node.
	 * Append k_prime and the following pointer.
	 * Append all pointers and keys from the neighbor.
	 */

	if (!n->is_leaf) {

		/* Append k_prime.
		 */

		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->num_keys++;


		n_end = n->num_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
		}

		/* The number of pointers is always
		 * one more than the number of keys.
		 */

		neighbor->pointers[i] = n->pointers[j];

		/* All children must now point up to the same parent.
		 */

		for (i = 0; i < neighbor->num_keys + 1; i++) {
			tmp = (node_t *)neighbor->pointers[i];
			tmp->parent = neighbor;
		}
	}

	/* In a leaf, append the keys and pointers of
	 * n to the neighbor.
	 * Set the neighbor's last pointer to point to
	 * what had been n's right neighbor.
	 */

	else {
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		neighbor->pointers[order - 1] = n->pointers[order - 1];
	}

	root = delete_entry(root, n->parent, k_prime, n);
	free(n->keys);
	free(n->pointers);
	free(n); 
	return root;
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
node_t *redistribute_nodes(node_t *root, node_t *n, node_t *neighbor, int neighbor_index, 
		int k_prime_index, int k_prime) {  
	int i;
	node_t *tmp;

	/* Case: n has a neighbor to the left. 
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's left end.
	 */
	if (neighbor_index != -1) {
		if (!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];

		for (i = n->num_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}

		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			tmp = (node_t *)n->pointers[0];
			tmp->parent = n;
			neighbor->pointers[neighbor->num_keys] = NULL;
			n->keys[0] = k_prime;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		} else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			n->parent->keys[k_prime_index] = n->keys[0];
		}
	} else {  
		/*
		 * Case: n is the leftmost child.
		 * Take a key-pointer pair from the neighbor to the right.
		 * Move the neighbor's leftmost key-pointer pair
		 * to n's rightmost position.
		 */
		if (n->is_leaf) {
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		} else {
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			tmp = (node_t *)n->pointers[n->num_keys + 1];
			tmp->parent = n;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->num_keys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}

	/* n now has one more key and one more pointer;
	 * the neighbor has one fewer of each.
	 */

	n->num_keys++;
	neighbor->num_keys--;

	return root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node_t *delete_entry( node_t * root, node_t * n, int key, void * pointer ) {
	int min_keys;
	node_t * neighbor;
	int neighbor_index;
	int k_prime_index, k_prime;
	int capacity;

	// Remove key and pointer from node.

	n = remove_entry_from_node(n, key, pointer);

	/* Case:  deletion from the root. 
	 */

	if (n == root) 
		return adjust_root(root);


	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

	min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

	/* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

	if (n->num_keys >= min_keys)
		return root;

	/* Case:  node falls below minimum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor node with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to node n and the pointer
	 * to the neighbor.
	 */

	neighbor_index = get_neighbor_index( n );
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor = neighbor_index == -1 ? n->parent->pointers[1] : 
		n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? order : order - 1;

	/* Coalescence. */

	if (neighbor->num_keys + n->num_keys < capacity)
		return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

	/* Redistribution. */
	return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}

/* Master deletion function */
node_t *delete(node_t *root, int key) {
	record_t *key_record = find(root, key, false);
	node_t *key_leaf = find_leaf(root, key, false);
	if (key_record && key_leaf) {
		root = delete_entry(root, key_leaf, key, key_record);
		if (release_callback)	
			release_callback(key_record->value._data);
		free(key_record);
	}

	return root;
}

void destroy_tree_nodes(node_t * root) {
	int i;
	if (root->is_leaf)
		for (i = 0; i < root->num_keys; i++)
			free(root->pointers[i]);
	else
		for (i = 0; i < root->num_keys + 1; i++)
			destroy_tree_nodes(root->pointers[i]);
	free(root->pointers);
	free(root->keys);
	free(root);
}

node_t *destroy_tree(node_t *root) {
	destroy_tree_nodes(root);
	return NULL;
}

/* ********************************
 * STANDALONE
 * ********************************/

#ifdef STANDALONE

#define PROGNAME "ytree"

/* Copyright and license notice to user at startup. */
void print_license_notice() {
	printf("Copyright (C) 2016 " PROGNAME ", Quenza Inc.\n"
			"All Rights Reserved\n"
			PROGNAME " version " VERSION "\n\n");
}

/* First message to the user. */
void print_status() {
	printf("Current config:\n");
	printf("  Min order %d\n", MIN_ORDER);
	printf("  Max order %d\n", MAX_ORDER);
	printf("  Current order %d\n", order);
	printf("  Record type INT\n");
	printf("  Verbose output %s\n", verbose_output ? "on" : "off");
	puts("");
}

/* Second message to the user */
void print_console_help() {
	printf("Enter any of the following commands after the prompt >>:\n"
		"  i <k>\t\tInsert <k> as both key and value)\n"
		"  f <k>\t\tFind the value under key <k>\n"
		"  p <k>\t\tPrint the path from the root to key k and its associated value\n"
		"  r <k1> <k2>\tPrint the keys and values found in the range [<k1>, <k2>\n"
		"  d <k>\t\tDelete key <k> and its associated value\n"
		"  x\t\tDestroy the whole tree. Start again with an empty tree of the same order\n"
		"  t\t\tPrint the ytree\n"
		"  l\t\tPrint the keys of the leaves (bottom row of the tree)\n"
		"  v\t\tToggle output of pointer addresses (\"verbose\") in tree and leaves\n"
		"  s\t\tSave to persistent storage\n"
		"  o\t\tRestore from persistent storage\n"
		"  a\t\tPrint status\n"
		"  q\t\tQuit (Or use Ctl-D)\n"
		"  ?\t\tPrint this help message\n");
}

void release_pointer(void *p) {
	printf("HIT %p\n", p);
}

/*
 * Open file depending on 
 * the environment.
 */
FILE *file_open(FILE *fp, const char *filename, const char *mode) {
#ifdef _WIN32
	fopen_s(&fp, filename, mode);
#else
	fp = fopen(filename, mode);
#endif
	return fp;
}

/*
 * Read input from file 
 */
void file_read_input(FILE *fp, const char *fmt, int *res) {
#ifdef _WIN32
	fscanf_s(fp, fmt, res);
#else
	fscanf(fp, fmt, res);
#endif
}

/*
 * Ask the user for input 
 */
void require_input(int *input) {
#ifdef _WIN32
	scanf_s("%d", input);
#else
	scanf("%d", input);
#endif
}

#ifdef _WIN32
#define get_instruction(i) scanf_s("%c", i, 1)
#else
#define get_instruction(i) scanf("%c", i)
#endif

/* Main */
int main(int argc, char *argv[]) {
	int input, range2;
	char instruction;

	node_t *root = NULL;
	verbose_output = false;

	if (argc > 1) {
		order = atoi(argv[1]);
		if (order < MIN_ORDER || order > MAX_ORDER) {
			fprintf(stderr, "Invalid order: %d\n", order);
			fprintf(stderr, "Value must be between %d and %d\n", MIN_ORDER, MAX_ORDER);
			exit(EXIT_FAILURE);
		}
	}

	print_license_notice();
	print_status();  
	print_console_help();

	release_callback = &release_pointer;

	if (argc > 2) {
		FILE *fp = NULL;
		fp = file_open(fp, argv[2], "r");
		if (!fp) {
			perror("Failure to open input file.");
			exit(EXIT_FAILURE);
		}

		while (!feof(fp)) {
			file_read_input(fp, "%d\n", &input);
			root = ytree_insert(root, input, make_record_int(input));
		}
		fclose(fp);
		ytree_print_tree(root);
	}

	printf(">> ");
	while (get_instruction(&instruction) != EOF) {
		switch (instruction) {
		case 'd': /* Delete */
			require_input(&input);
			root = delete(root, input);
			ytree_print_tree(root);
			break;
		case 'i': /* Insert */
			require_input(&input);
			root = ytree_insert(root, input, make_record_int(input));
			ytree_print_tree(root);
			break;
		case 'f':
		case 'p':
			require_input(&input);
			find_and_print(root, input, instruction == 'p');
			break;
		case 'r':
			require_input(&input);
			require_input(&range2);
			if (input > range2) {
				int tmp = range2;
				range2 = input;
				input = tmp;
			}
			find_and_print_range(root, input, range2, instruction == 'p');
			break;
		case 'l':
			ytree_print_leaves(root);
			break;
		case 'q':
			while (getchar() != (int)'\n');
			goto interactive_done;
		case 't':
			ytree_print_tree(root);
			break;
		case 'v':
			verbose_output = !verbose_output;
			printf("Verbose output: %d\n", verbose_output);
			break;
		case 'a':
			print_status();
			break;
		case 'x':
			if (root)
				root = destroy_tree(root);
			ytree_print_tree(root);
			break;
		default:
			print_console_help();
			break;
		}
		while (getchar() != (int)'\n');
		printf(">> ");
	}
	printf("\n");

interactive_done:
	return EXIT_SUCCESS;
}

#endif
