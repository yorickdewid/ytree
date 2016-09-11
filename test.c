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

	test_assert(ytree_count(&db) == 10);

	valuepair_t value;
	value.data = "somval";
	value.size = sizeof("somval");

	ytree_insert(&db, -10, ytree_new_record(&value));

	test_assert(ytree_count(&db) == 11);

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