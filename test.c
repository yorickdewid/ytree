#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include "ytree.h"

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

	test_assert(ytree_count(&db) == asz(input));

	valuepair_t value;
	value.data = "somval";
	value.size = sizeof("somval");

	ytree_insert(&db, -10, ytree_new_record(&value));

	test_assert(ytree_count(&db) == asz(input) + 1);
	test_assert(!ytree_db_empty(&db));

	ytree_db_close(&db);
	ytree_env_close(&env);
}

TESTCASE(find) {
	env_t *env = NULL;
	db_t *db = NULL;
	int input[] = {768,-34,214,-456,712,546,-214};

	ytree_env_init(DATABASENAME, &env, 0);
	ytree_db_init(0, &db, &env);

	int i;
	for (i=0; i<asz(input); ++i)
		ytree_insert(&db, input[i], ytree_new_int(input[i]));

	test_assert(ytree_count(&db) == asz(input));

	for (i=0; i<asz(input); ++i) {
		record_t *record = ytree_find(&db, input[i]);
		test_assert(record);
		test_assert(record->value._int == input[i]);
	}

	ytree_db_close(&db);
	ytree_env_close(&env);
}

TESTCASE(delete) {
	env_t *env = NULL;
	db_t *db = NULL;
	int input[] = {-34,-546,235,13,-421,234,91,-6,35,9232,-164,905};

	ytree_env_init(DATABASENAME, &env, 0);
	ytree_db_init(0, &db, &env);

	int i;
	for (i=0; i<asz(input); ++i)
		ytree_insert(&db, input[i], ytree_new_int(input[i]));

	test_assert(ytree_count(&db) == asz(input));

	ytree_delete(&db, input[0]);

	test_assert(ytree_count(&db) == asz(input) - 1);

	for (i=1; i<asz(input); ++i)
		ytree_delete(&db, input[i]);

	test_assert(ytree_count(&db) == 0);
	test_assert(ytree_db_empty(&db));

	ytree_db_close(&db);
	ytree_env_close(&env);
}

TESTCASE(purge) {
	env_t *env = NULL;
	db_t *db = NULL;
	int input[] = {
		6152,-8573,-6162,-5755,-6495,5973,-3874
		-5867,-5692,-4740,9484,7054,3273,3331,
		9642,-8831,1444,716,453,7280,7971
	};

	ytree_env_init(DATABASENAME, &env, 0);
	ytree_db_init(0, &db, &env);

	int i;
	for (i=0; i<asz(input); ++i)
		ytree_insert(&db, input[i], ytree_new_int(input[i]));

	test_assert(ytree_count(&db) == asz(input));

	ytree_purge(&db);

	test_assert(ytree_count(&db) == 0);
	test_assert(ytree_db_empty(&db));

	ytree_db_close(&db);
	ytree_env_close(&env);
}


int main(int agrc, char *argv[]) {

	/* Run testcases */
	CALLTEST(create);
	CALLTEST(insert);
	CALLTEST(find);
	CALLTEST(delete);
	CALLTEST(purge);

	printf("All tests OK\nReport:\n  Cases:\t%d\n  Assertions:\t%d\n", cases, assertions);

	return 0;
}