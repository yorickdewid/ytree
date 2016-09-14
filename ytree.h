/*
 * -----------------------------  ytree.h  ------------------------------
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
 * ytree definitions.
 */

#ifndef _YTREE_H_
#define _YTREE_H_

#include <stdbool.h>
#include <stdint.h>

/* Enable for debug compilation */
#ifdef STANDALONE
#define DEBUG 1
#endif

/* 
 * Tree options. These can be set when
 * a new tree is created.
 */
#define DB_FLAG_DUPLICATE	0x01	// Allow duplicated keys
#define DB_FLAG_HASH		0x02	// Use hash buckets where possible
#define DB_FLAG_VERBOSE		0x04	// Verbose output
#define DB_FLAG_PREF_SPEED	0x08	// Prefer speed
#define DB_FLAG_PREF_SIZE	0x10	// Prefer small database size

/* ********************************
 * TYPES
 * ********************************/

/*
 * Funtion pointer declarations for the
 * object hooks.
 */
typedef void (*hook_release)(void *object);
typedef void (*hook_serialize)(void *object, size_t *sz, void *out);

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
	void **pointers;						// Array of pointers to records
	uint32_t *_pointers;					// Array of pointers to offset
	int *keys;								// Array of keys with size: order 
	struct node *parent;					// Parent node or NULL for root
	bool is_leaf;							// Internal node or leaf
	int num_keys;							// Number of keys in node
	struct node *next;						// Used for queue
} node_t;

/* Database environment */
typedef struct {
	int schema;								// Offset to database schema
	int free_front;							// Offset to free block from front
	int free_back;							// Offset to free block from back
	size_t page_size;						// Page size
	char flags;								// Bitmap defining tree options
	FILE *pdb;								// Database file pointer
} env_t;

/* Single database */
typedef struct {
	int schema_id;							// Id in schema
	short order;							// Tree order (B+Tree only)
	int _root;								// Offset to root
	env_t *env;								// Pointer to current environment
	node_t *root;							// Pointer to root node
	struct {
		hook_release object_release;		// Called on record release
		hook_serialize object_serialize;	// Called on record serialization
	} hooks;
} db_t;

/* Key value pair */
typedef struct {
	void *data;
	size_t size;
} keypair_t, valuepair_t;

/* ********************************
 * FUNCTION PROTOTYPES
 * ********************************/

/* Output */
#ifdef DEBUG
void ytree_print_leaves(db_t **db);
void ytree_print_value(record_t *record);
void ytree_print_tree(db_t **db);

//TODO ytree?
void find_and_print(db_t **db, int key, bool verbose); 
void find_and_print_range(db_t **db, int range1, int range2, bool verbose); 
#endif // DEBUG

/* Miscellaneous */
int ytree_height(db_t **db);
int ytree_count(db_t **db);
void ytree_purge(db_t **db);
void ytree_order(db_t **db, unsigned int order);
const char *ytree_version();

void ytree_insert(db_t **db, int key, record_t *pointer);
record_t *ytree_find(db_t **db, int key);
void ytree_delete(db_t **db, int key);

/* Tree operations */
void ytree_env_init(const char *dbname, env_t **tree, uint8_t flags);
void ytree_env_close(env_t **tree);
void ytree_db_init(short index, db_t **db, env_t **env);
void ytree_db_close(db_t **db);

/* Record */
record_t *make_record(enum datatype type, char c_value, int i_value, float f_value, void *p_value, size_t vsize);
record_t *ytree_new_record(valuepair_t *pair);
int ytree_record_size(record_t *record);

/* Helper macros */
#define ytree_new_char(c) make_record(DT_CHAR, c, 0, 0, NULL, 0)
#define ytree_new_int(i) make_record(DT_INT, 0, i, 0, NULL, 0)
#define ytree_new_float(f) make_record(DT_FLOAT, 0, 0, f, NULL, 0)
#define ytree_new_data(d,n) make_record(DT_DATA, 0, 0, 0, d, n)

#define ytree_db_empty(d) ((*d)->root == NULL)

#endif // _YTREE_H_
