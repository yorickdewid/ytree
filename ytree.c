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
 * Usage: ytree [order]
 * where order is an optional argument
 * (integer MIN_ORDER <= order <= MAX_ORDER)
 * defined as the maximal number of pointers in any node.
 *
 * TODO
 * - Error handling
 * - Debug compile
 * - Public API
 * - Supply config / flags
 * - Persistent extension
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/stat.h>
#include "ytree.h"

/* Algorithm version */
#define VERSION "0.1"

/* Default order is 4 */
#define DEFAULT_ORDER 4

/* Default page size */
#define DEFAULT_PAGE_SIZE 1024

/* Database header */
#define DBHEADER "YTREE01"

/* 
 * Minimum order is necessarily 3. We set the maximum
 * order arbitrarily. You may change the maximum order.
 */
#define MIN_ORDER 3
#define MAX_ORDER 100

/* 
 * Database index algorithm.
 */
#define INDEX_TREE		0x01	// B+Tree index
#define INDEX_HASH		0x01	// Hash index

/* ********************************
 * TYPES
 * ********************************/

/*
 * Datatype helpers to determine the
 * type of data stored in the record
 */
#define is_char(r) (r->value_type == DT_CHAR)
#define is_int(r) (r->value_type == DT_INT)
#define is_float(r) (r->value_type == DT_FLOAT)
#define is_data(r) (r->value_type == DT_DATA)

/*
 * Database schema.
 * Storage only.
 */
struct schema {
	uint16_t id;							// Database id
	uint8_t type;							// Type of index
	uint32_t root;							// Offset to database
	uint16_t order;							// Tree order (B+Tree only)
};

/*
 * Database environment.
 * Storage only.
 */
struct env {
	char header[8];							// Database header
	uint32_t schema;						// Pointer to the database schema
	uint16_t page_size;						// Page size
	uint8_t flags;							// Bitmap defining tree options
};

/* ********************************
 * GLOBALS
 * ********************************/

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;

void (*release_callback)(void *) = NULL;

/* ********************************
 * FUNCTION PROTOTYPES
 * ********************************/

/* Helpers */
static int path_to_root(node_t *root, node_t *child);
static node_t *find_leaf(node_t *root, int key);

/* Search */
static int find_range(db_t **db, int key_start, int key_end, int *returned_keys, void *returned_pointers[]); 

/* Insertion */
static node_t *make_node_raw(db_t **db, bool is_leaf);
static int get_left_index(node_t *parent, node_t *left);
static node_t *insert_into_leaf(node_t *leaf, int key, record_t *pointer );
static node_t *insert_into_leaf_after_splitting(db_t **db, node_t *leaf, int key, record_t *pointer);
static node_t *insert_into_node(db_t **db, node_t *parent, int left_index, int key, node_t * right);
static node_t *insert_into_node_after_splitting(db_t **db, node_t * parent, int left_index, int key, node_t * right);
static node_t *insert_into_parent(db_t **db, node_t * left, int key, node_t * right);
static node_t *insert_into_new_root(db_t **db, node_t * left, int key, node_t * right);
static node_t *start_new_tree(db_t **db, int key, record_t * pointer);

/* Deletion */
static node_t *adjust_root(db_t **db);
static node_t *coalesce_nodes(db_t **db, node_t *n, node_t *neighbor, int neighbor_index, int k_prime);
static node_t *redistribute_nodes(db_t **db, node_t *n, node_t *neighbor, int neighbor_index, int k_prime_index, int k_prime);
static node_t *delete_entry(db_t **db, node_t *n, int key, void *pointer);

/* ********************************
 * HELPERS
 * ********************************/

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

static bool file_exist(const char *filename) {
    struct stat st;
    return stat(filename, &st) == 0;
}

/* ********************************
 * OUTPUT [DEBUG ONLY]
 * ********************************/

#ifdef DEBUG

/*
 * Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void ytree_print_leaves(db_t **db) {
	int i;
	node_t *c = (*db)->root;
	if (!(*db)->root) {
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
			printf("%x ", (unsigned int)(uintptr_t)c->pointers[(*db)->order - 1]);
		if (c->pointers[(*db)->order - 1] != NULL) {
			printf(" | ");
			c = c->pointers[(*db)->order - 1];
		} else
			break;
	}

	printf("\n");
}

/*
 * Find the datatype in the record
 * and print the corresponding
 * value to screen
 */
void ytree_print_value(record_t *record) {
	switch (record->value_type) {
		case DT_CHAR:
			printf("%c\n", record->value._char);
			break;
		case DT_INT:
			printf("%d\n", record->value._int);
			break;
		case DT_FLOAT:
			printf("%f\n", record->value._float);
			break;
		case DT_DATA:
			printf("%p\n", record->value._data);
			break;
	}
}

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
static void enqueue(node_t **queue, node_t *new_node) {
	node_t *c;
	if (!*queue) {
		*queue = new_node;
		(*queue)->next = NULL;
	} else {
		c = *queue;
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
static node_t *dequeue(node_t **queue) {
	node_t *n = *queue;
	*queue = (*queue)->next;
	n->next = NULL;
	return n;
}

/*
 * Prints the B+Tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void ytree_print_tree(db_t **db) {
	int i = 0;
	int rank = 0;
	int new_rank = 0;

	if (!(*db)->root) {
		printf("Empty tree\n");
		return;
	}

	node_t *queue = NULL;
	enqueue(&queue, (*db)->root);
	while (queue) {
		node_t *n = dequeue(&queue);
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_rank = path_to_root((*db)->root, n);
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		// if (verbose_output) 
		// 	printf("(%x)", (unsigned int)(uintptr_t)n);
		for (i = 0; i < n->num_keys; i++) {
			// if (verbose_output)
			// 	printf("%x ", (unsigned int)(uintptr_t)n->pointers[i]);
			printf("%d ", n->keys[i]);
		}

		if (!n->is_leaf)
			for (i = 0; i <= n->num_keys; i++)
				enqueue(&queue, n->pointers[i]);
		// if (verbose_output) {
		// 	if (n->is_leaf) 
		// 		printf("%x ", (unsigned int)(uintptr_t)n->pointers[4 - 1]);
		// 	else
		// 		printf("%x ", (unsigned int)(uintptr_t)n->pointers[n->num_keys]);
		// }
		printf("| ");
	}
	printf("\n");
}

/* public?
 * Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(db_t **db, int key, bool verbose) {
	record_t *record = ytree_find(db, key);
	if (!record) {
		printf("Key: %d  Record: NULL\n", key);
		return;
	}

	printf("Key: %d  Record: ", key);
	ytree_print_value(record);
}

/* public?
 * Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
void find_and_print_range(db_t **db, int key_start, int key_end, bool verbose) {
	int i;
	int array_size = key_end - key_start + 1;
	int *returned_keys = (int *)malloc(array_size);
	void **returned_pointers = malloc(array_size);
	int num_found = find_range(db, key_start, key_end, returned_keys, returned_pointers);
	if (num_found) {
		for (i = 0; i < num_found; i++) {
			printf("Key: %d  Record: ", returned_keys[i]);
			ytree_print_value((record_t *)returned_pointers[i]);
		}
	} else {
		printf("None found\n");
	}

	free(returned_keys);
}

#endif // DEBUG

/*
 * Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int ytree_height(db_t **db) {
	int h = 0;

	if (!(*db)->root) {
		return 0;
	}

	node_t *c = (*db)->root;
	while (!c->is_leaf) {
		c = c->pointers[0];
		h++;
	}

	return h;
}

/* Return number of keys */
int ytree_count(db_t **db) {
	int count = 0;
	node_t *c = (*db)->root;
	if (!(*db)->root)
		return count;

	/* Find leftmost leave */
	while (!c->is_leaf)
		c = c->pointers[0];

	while (true) {
		count += c->num_keys;
		if (c->pointers[(*db)->order - 1] != NULL)
			c = c->pointers[(*db)->order - 1];
		else
			break;
	}

	return count;
}

/*
 * Database version number
 * returned as string.
 */
const char *ytree_version() {
	return VERSION;
}

/* TODO: check index type == btree
 * Set B+Tree order, this is only valid for
 * a B+Tree structure, and is otherwise ignored.
 * Order can only be set if the tree is empty.
 * The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
void ytree_order(db_t **db, unsigned int order) {
	if (!(*db)->root) {
		(*db)->order = order;
	}
}

/*
 * Utility function to give the length in edges
 * of the path from any node to the root.
 */
static int path_to_root(node_t *root, node_t *child) {
	int length = 0;
	node_t *c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}

/*
 * Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive. Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
static int find_range(db_t **db, int key_start, int key_end, int *returned_keys, void **returned_pointers) {
	int i, num_found = 0;
	node_t *n = find_leaf((*db)->root, key_start);
	if (!n)
		return 0;

	assert(returned_keys);
	assert(returned_pointers);

	for (i = 0; i < n->num_keys && n->keys[i] < key_start; ++i);
	if (i == n->num_keys)
		return 0;

	while (n != NULL) {
		for (; i < n->num_keys && n->keys[i] <= key_end; ++i) {
			returned_keys[num_found] = n->keys[i];
			returned_pointers[num_found] = n->pointers[i];
			num_found++;
		}
		n = n->pointers[(*db)->order - 1];
		i = 0;
	}

	return num_found;
}

/* TODO: remove prints
 * Traces the path from the root to a leaf, searching
 * by key. Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
static node_t *find_leaf(node_t *root, int key) {
	int i = 0;
	node_t *c = root;
	if (!c)
		return NULL;

	while (!c->is_leaf) {
		// if (verbose) {
		// 	printf("[");
		// 	for (i = 0; i < c->num_keys - 1; ++i)
		// 		printf("%d ", c->keys[i]);
		// 	printf("%d] ", c->keys[i]);
		// }

		i = 0;
		while (i < c->num_keys) {
			if (key >= c->keys[i])
				++i;
			else
				break;
		}
		
		// if (verbose)
		// 	printf("%d ->\n", i);

		c = (node_t *)c->pointers[i];
	}

	// if (verbose) {
	// 	printf("Leaf [");
	// 	for (i = 0; i < c->num_keys - 1; i++)
	// 		printf("%d ", c->keys[i]);
	// 	printf("%d] ->\n", c->keys[i]);
	// }

	return c;
}

/*
 * Finds and returns the record to which
 * a key refers.
 */
record_t *ytree_find(db_t **db, int key) {
	int i = 0;
	node_t *c = find_leaf((*db)->root, key);
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
	record_t *new_record = (record_t *)calloc(1, sizeof(record_t));
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
 * Create a new record using valuepair. This 
 * is compatible with other databases.
 */
record_t *ytree_new_record(valuepair_t *pair) {
	return make_record(DT_DATA, 0, 0, 0, pair->data, pair->size);
}

/*
 * Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
static node_t *make_node_raw(db_t **db, bool is_leaf) {
	node_t *new_node = (node_t *)calloc(1, sizeof(node_t));
	if (!new_node) {
		perror("Node creation.");
		exit(EXIT_FAILURE);
	}

	new_node->keys = calloc(((*db)->order - 1), sizeof(int));
	if (!new_node->keys) {
		perror("New node keys array.");
		exit(EXIT_FAILURE);
	}

	new_node->pointers = calloc((*db)->order, sizeof(void *));
	if (new_node->pointers == NULL) {
		perror("New node pointers array.");
		exit(EXIT_FAILURE);
	}

	new_node->is_leaf = is_leaf;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
	return new_node;
}

/* 
 * Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
#define make_leaf(d) make_node_raw(d,true)
#define make_node(d) make_node_raw(d,false)

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
static node_t *insert_into_leaf_after_splitting(db_t **db, node_t *leaf, int key, record_t *pointer) {
	node_t *new_leaf = make_leaf(db);

	int *temp_keys = (int *)calloc((*db)->order, sizeof(int));
	if (!temp_keys) {
		perror("Temporary keys array.");
		exit(EXIT_FAILURE);
	}

	void **temp_pointers = calloc((*db)->order, sizeof(void *));
	if (!temp_pointers) {
		perror("Temporary pointers array.");
		exit(EXIT_FAILURE);
	}

	int insertion_index = 0;
	while (insertion_index < (*db)->order - 1 && leaf->keys[insertion_index] < key)
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

	int split = cut((*db)->order - 1);

	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < (*db)->order; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	free(temp_pointers);
	free(temp_keys);

	/* Cheate the sequence chain */
	new_leaf->pointers[(*db)->order - 1] = leaf->pointers[(*db)->order - 1];
	leaf->pointers[(*db)->order - 1] = new_leaf;

	for (i = leaf->num_keys; i < (*db)->order - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < (*db)->order - 1; i++)
		new_leaf->pointers[i] = NULL;

	new_leaf->parent = leaf->parent;

	return insert_into_parent(db, leaf, new_leaf->keys[0], new_leaf);
}

/*
 * Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+Tree properties.
 */
static node_t *insert_into_node(db_t **db, node_t *n, int left_index, int key, node_t *right) {
	int i;
	for (i = n->num_keys; i > left_index; i--) {
		n->pointers[i + 1] = n->pointers[i];
		n->keys[i] = n->keys[i - 1];
	}

	n->pointers[left_index + 1] = right;
	n->keys[left_index] = key;
	n->num_keys++;
	return (*db)->root;
}


/*
 * Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
static node_t *insert_into_node_after_splitting(db_t **db, node_t *old_node, int left_index, int key, node_t *right) {
	node_t *child;

	/*
	 * First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */
	node_t **temp_pointers = calloc(((*db)->order + 1), sizeof(node_t *));
	if (!temp_pointers) {
		perror("Temporary pointers array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	int *temp_keys = calloc((*db)->order, sizeof(int));
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
	int split = cut((*db)->order);
	node_t *new_node = make_node(db);
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	int k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < (*db)->order; i++, j++) {
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
	return insert_into_parent(db, old_node, k_prime, new_node);
}

/* 
 * Inserts a new node (leaf or internal node) into the B+Tree.
 * Returns the root of the tree after insertion.
 */
static node_t *insert_into_parent(db_t **db, node_t *left, int key, node_t *right) {
	node_t *parent = left->parent;

	/* Case: new root. */
	if (!parent)
		return insert_into_new_root(db, left, key, right);

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
	if (parent->num_keys < (*db)->order - 1)
		return insert_into_node(db, parent, left_index, key, right);

	/* 
	 * Harder case: split a node in order 
	 * to preserve the B+ tree properties.
	 */
	return insert_into_node_after_splitting(db, parent, left_index, key, right);
}

/* 
 * Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
static node_t *insert_into_new_root(db_t **db, node_t *left, int key, node_t *right) {
	node_t *root = make_node(db);
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
static node_t *start_new_tree(db_t **db, int key, record_t *pointer) {
	node_t *root = make_leaf(db);
	root->keys[0] = key;
	root->pointers[0] = pointer;
	root->pointers[(*db)->order - 1] = NULL;
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
void ytree_insert(db_t **db, int key, record_t *pointer) {
	assert(pointer);

	/*
	 * The current implementation ignores
	 * duplicates.
	 */
	if (ytree_find(db, key) != NULL) {
		return;
	}

	/*
	 * Case: the tree does not exist yet.
	 * Start a new tree.
	 */
	if (!(*db)->root) {
		(*db)->root = start_new_tree(db, key, pointer);
		return;
	}

	/* 
	 * Case: the tree already exists.
	 * (Rest of function body.)
	 */
	node_t *leaf = find_leaf((*db)->root, key);

	/* 
	 *Case: leaf has room for key and pointer.
	 */
	if (leaf->num_keys < (*db)->order - 1) {
		leaf = insert_into_leaf(leaf, key, pointer);
		return;
	}

	/*
	 * Case: leaf must be split.
	 */
	(*db)->root = insert_into_leaf_after_splitting(db, leaf, key, pointer);
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
static int get_neighbor_index(node_t *n) {
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

static node_t *remove_entry_from_node(db_t **db, node_t *n, int key, node_t *pointer) {
	int i, num_pointers;

	/* Remove the key and shift other keys accordingly. */
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

	/* One key fewer. */
	n->num_keys--;

	// Set the other pointers to NULL for tidiness.
	// A leaf uses the last pointer to point to the next leaf.
	if (n->is_leaf)
		for (i = n->num_keys; i < (*db)->order - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->num_keys + 1; i < (*db)->order; i++)
			n->pointers[i] = NULL;

	return n;
}

static node_t *adjust_root(db_t **db) {
	node_t *new_root = NULL;

	/* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */
	if ((*db)->root->num_keys > 0)
		return (*db)->root;

	/* Case: empty root. 
	 */

	/*
	 * If it has a child, promote 
	 * the first (only) child
	 * as the new root. If node is leaf
	 * root becomes empty.
	 */
	if (!(*db)->root->is_leaf) {
		new_root = (*db)->root->pointers[0];
		new_root->parent = NULL;
	}

	free((*db)->root->keys);
	free((*db)->root->pointers);
	free((*db)->root);

	return new_root;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
static node_t *coalesce_nodes(db_t **db, node_t *n, node_t *neighbor, int neighbor_index, int k_prime) {
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
		neighbor->pointers[(*db)->order - 1] = n->pointers[(*db)->order - 1];
	}

	(*db)->root = delete_entry(db, n->parent, k_prime, n);
	free(n->keys);
	free(n->pointers);
	free(n); 
	return (*db)->root;
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
static node_t *redistribute_nodes(db_t **db, node_t *n, node_t *neighbor, int neighbor_index, int k_prime_index, int k_prime) {  
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

	return (*db)->root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node_t *delete_entry(db_t **db, node_t *n, int key, void *pointer) {
	int min_keys;
	node_t *neighbor;
	int neighbor_index;
	int k_prime_index, k_prime;
	int capacity;

	/*
	 * Remove key and pointer from node.
	 */
	n = remove_entry_from_node(db, n, key, pointer);

	/*
	 * Case:  deletion from the root. 
	 */
	if (n == (*db)->root) 
		return adjust_root(db);

	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */
	min_keys = n->is_leaf ? cut((*db)->order - 1) : cut((*db)->order) - 1;

	/* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

	if (n->num_keys >= min_keys)
		return (*db)->root;

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

	neighbor_index = get_neighbor_index(n);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor = neighbor_index == -1 ? n->parent->pointers[1] : n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? (*db)->order : (*db)->order - 1;

	/* Coalescence. */
	if (neighbor->num_keys + n->num_keys < capacity)
		return coalesce_nodes(db, n, neighbor, neighbor_index, k_prime);

	/* Redistribution. */
	return redistribute_nodes(db, n, neighbor, neighbor_index, k_prime_index, k_prime);
}

/* 
 * Master deletion function
 */
void ytree_delete(db_t **db, int key) {
	record_t *key_record = ytree_find(db, key);
	node_t *key_leaf = find_leaf((*db)->root, key);
	if (key_record && key_leaf) {
		(*db)->root = delete_entry(db, key_leaf, key, key_record);

		/* Call pointer release hook if type is data */
		if (is_data(key_record) && release_callback)
			release_callback(key_record->value._data);

		free(key_record);
	}
}

/* TODO: call release hook
 * Traverse tree and delete nodes
 */
static void destroy_tree_nodes(node_t *root) {
	int i;
	if (root->is_leaf)
		for (i = 0; i < root->num_keys; ++i)
			free(root->pointers[i]);
	else
		for (i = 0; i < root->num_keys + 1; i++)
			destroy_tree_nodes(root->pointers[i]);

	free(root->pointers);
	free(root->keys);
	free(root);
}

/* 
 * Delete tree object
 */
void ytree_purge(db_t **db) {
	if (!(*db)->root)
		return;

	destroy_tree_nodes((*db)->root);

	(*db)->root = NULL;
}

/* ********************************
 * DATABASE OPERATIONS
 * ********************************/

/* Return schema size depending on page size */
#define get_schema_size(n) (n)->page_size/128

/* 
 * Write header to disk
 */
static void env_write_header(env_t *env) {
	struct env buffer;
	strncpy(buffer.header, DBHEADER, 8);

	buffer.schema = env->schema;
	// buffer.order = env->order;
	buffer.page_size = env->page_size;
	buffer.flags = env->flags;

	fwrite(&buffer, sizeof(struct env), 1, env->pdb);
	fflush(env->pdb);
}

/* 
 * Write schema to offset
 */
static void env_write_schema(env_t *env, uint32_t offset) {
	struct schema *schema = calloc(get_schema_size(env), sizeof(struct schema));

	fseek(env->pdb, offset, SEEK_SET);
	fwrite(schema, sizeof(struct schema), get_schema_size(env), env->pdb);

	free(schema);
}

static void env_alloc_page(env_t *env, unsigned short n) {
	fseek(env->pdb, (n * env->page_size) - 1, SEEK_SET);
	fwrite(&n, 1, 1, env->pdb);

	env->free_back = ftell(env->pdb);
}

/* 
 * Create new database environment
 */
void ytree_env_init(const char *dbname, env_t **env, uint8_t flags) {
	*env = (env_t *)calloc(1, sizeof(env_t));

	if (file_exist(dbname)) {
		puts("open");
		assert(0);

		// env_read_header(*env);
		// env_read_schema(*env);
	} else {
		(*env)->pdb = fopen(dbname, "w+b");
		if (!(*env)->pdb) {
			perror("ytree_env_init");
			exit(1);
		}

		/* Default settings */
		(*env)->schema = sizeof(env_t);
		(*env)->page_size = DEFAULT_PAGE_SIZE;
		(*env)->flags = flags;

		env_write_header(*env);
		env_write_schema(*env, (*env)->schema);

		(*env)->free_front = ftell((*env)->pdb);

		env_alloc_page(*env, 1);
	}
}

void ytree_db_init(short index, db_t **db, env_t **env) {
	assert(index < get_schema_size(*env));

	*db = (db_t *)calloc(1, sizeof(db_t));

	(*db)->schema_id = index;
	(*db)->env = *env;
	(*db)->order = DEFAULT_ORDER;
}

void ytree_db_close(db_t **db) {
	free(*db);
}

/* 
 * Close the database environment
 */
void ytree_env_close(env_t **env) {
	fclose((*env)->pdb);
	free(*env);
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
void print_status(db_t **db) {
	printf("Database status:\n");
	printf("  Schema index %d\n", (*db)->schema_id);
	printf("  Index type B+Tree\n");
	printf("  Current order %d\n", (*db)->order);
	printf("  Record type INT\n");
	printf("  Verbose output %s\n", verbose_output ? "on" : "off");
	printf("  Tree height %d\n", ytree_height(db));
	printf("  Tree empty %s\n",  ytree_db_empty(db) ? "yes" : "no");
	printf("  Count %d\n", ytree_count(db));
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

/* Hook test */
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
	env_t *env;
	db_t *db;

	verbose_output = false;

	/* Create new tree object */
	ytree_env_init("test.ydb", &env, DB_FLAG_VERBOSE);
	ytree_db_init(0, &db, &env);

	if (argc > 1) {
		int order = atoi(argv[1]);
		if (order < MIN_ORDER || order > MAX_ORDER) {
			fprintf(stderr, "Invalid order: %d\n", order);
			fprintf(stderr, "Value must be between %d and %d\n", MIN_ORDER, MAX_ORDER);
			exit(EXIT_FAILURE);
		}

		ytree_order(&db, order);
	}

	/* Default info to screen */
	print_license_notice();
	print_status(&db);  
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
			ytree_insert(&db, input, ytree_new_int(input));
		}
		fclose(fp);
		ytree_print_tree(&db);
	}

	printf(">> ");
	while (get_instruction(&instruction) != EOF) {
		switch (instruction) {
		case 'd': /* Delete */
			require_input(&input);
			ytree_delete(&db, input);
			ytree_print_tree(&db);
			break;
		case 'i': /* Insert */
			require_input(&input);
			ytree_insert(&db, input, ytree_new_int(input));
			ytree_print_tree(&db);
			break;
		case 'f': /* Find */
		case 'p':
			require_input(&input);
			find_and_print(&db, input, instruction == 'p');
			break;
		case 'r': /* Range */
			require_input(&input);
			require_input(&range2);
			if (input > range2) {
				int tmp = range2;
				range2 = input;
				input = tmp;
			}
			find_and_print_range(&db, input, range2, instruction == 'p');
			break;
		case 'l': /* List sequence */
			ytree_print_leaves(&db);
			break;
		case 'q': /* Quit */
			while (getchar() != (int)'\n');
			goto interactive_done;
		case 't': /* Print tree */
			ytree_print_tree(&db);
			break;
		case 'v': /* Toggle verbose */
			verbose_output = !verbose_output;
			printf("Verbose output: %d\n", verbose_output);
			break;
		case 'a': /* Status */
			print_status(&db);
			break;
		case 'x': /* Destroy tree */
			ytree_purge(&db);
			break;
		default: /* Help */
			print_console_help();
			break;
		}
		while (getchar() != (int)'\n');
		printf(">> ");
	}
	printf("\n");

interactive_done:
	ytree_db_close(&db);
	ytree_env_close(&env);

	return EXIT_SUCCESS;
}

#endif
