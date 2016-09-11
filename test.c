#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>

enum datatype {
	DT_CHAR,
	DT_INT,
	DT_FLOAT,
	DT_DATA,
};

/* Record */
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

/* Database environment */
typedef struct {
	int schema;								// Offset to database schema
	int free_front;							// Offset to free block from front
	int free_back;							// Offset to free block from back
	size_t page_size;						// Page size
	char flags;								// Bitmap defining tree options
	FILE *pdb;								// Database file pointer
} env_t;

typedef struct node {
	void **pointers;						// Array of pointers to records
	int *keys;								// Array of keys with size: order 
	struct node *parent;					// Parent node or NULL for root
	bool is_leaf;							// Internal node or leaf
	int num_keys;							// Number of keys in node
	struct node *next;						// Used for queue
} node_t;

/* Single database */
typedef struct {
	int schema_id;							// Id in schema
	short order;							// Tree order (B+Tree only)
	int _root;								// Offset to root
	env_t *env;								// Pointer to current environment
	node_t *root;							// Pointer to root node
	struct {
		void (*data_release)(void *);		// Called on record release
	} hooks;
} db_t;

void ytree_env_init(const char *dbname, env_t **env, uint8_t flags);
void ytree_db_init(short index, db_t **db, env_t **env);
void ytree_db_close(db_t **db);
void ytree_env_close(env_t **env);
void ytree_insert(db_t **db, int key, record_t *pointer);
void ytree_print_tree(db_t **db);
int ytree_count(db_t **db);
record_t *make_record(enum datatype type, char c_value, int i_value, float f_value, void *p_value, size_t vsize);

#define ytree_new_char(c) make_record(DT_CHAR, c, 0, 0, NULL, 0)
#define ytree_new_int(i) make_record(DT_INT, 0, i, 0, NULL, 0)
#define ytree_new_float(f) make_record(DT_FLOAT, 0, 0, f, NULL, 0)
#define ytree_new_data(d,n) make_record(DT_FLOAT, 0, 0, 0, d, n)

#define DATABASENAME "__test.ydb"

static unsigned int assertions = 0;
static unsigned int cases = 0;

#define asz(a) sizeof(a)/sizeof(a[0])

#define test_assert(c) { \
	assert(c); ++assertions; \
}

#define TESTCASE(n) void test_##n()
#define CALLTEST(n) { \
	++cases; \
	setup(#n); \
	test_##n(); \
	teardown(); \
}

void setup(const char *name) {
	printf("[*] Testcase %s\n", name);
}

void teardown() {
	unlink(DATABASENAME);
}

TESTCASE(create) {
	env_t *env = NULL;
	db_t *db = NULL;

	ytree_env_init(DATABASENAME, &env, 0);
	ytree_db_init(0, &db, &env);

	test_assert(env != NULL)
	test_assert(db != NULL)

	ytree_db_close(&db);
	ytree_env_close(&env);
}

TESTCASE(insert) {
	env_t *env = NULL;
	db_t *db = NULL;
	int input[] = {10,20,30,40,50,60,70,80,90,100};

	ytree_env_init(DATABASENAME, &env, 0);
	ytree_db_init(0, &db, &env);

	int i;
	for (i=0; i<asz(input); ++i)
		ytree_insert(&db, input[i], ytree_new_int(input[i]));

	test_assert(ytree_count(&db) == 10)

	ytree_db_close(&db);
	ytree_env_close(&env);
}

int main(int agrc, char *argv[]) {

	/* Run testcases */
	CALLTEST(create);
	CALLTEST(insert);

	printf("All tests OK\nReport:\n  Asserts:\t%d\n  Cases:\t%d\n", assertions, cases);

	return 0;
}