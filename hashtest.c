#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const int counter = 4096 * 60;

unsigned long hash(unsigned char *str) {
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

struct kv {
	unsigned long key;
	char *value;
};

int main(int argc, char *agrv[]) {
	struct kv **arr = malloc(counter * sizeof(struct kv *));

	FILE *fp = fopen("words.txt", "r");
	if (!fp)
		return 1;

	char search[32];

	int i;
	for (i=0; i<counter; ++i) {
		char *line = NULL;
		size_t len = 0;
		ssize_t read = getline(&line, &len, fp);

#ifdef HASH
		int idx = hash(line) % counter;
#else
		int idx = i;
#endif
		arr[idx] = (struct kv *)malloc(sizeof(struct kv));
		arr[idx]->key = (unsigned long)i;
		arr[idx]->value = malloc(32);
		strncpy(arr[idx]->value, line, 32);

		if (i == counter - 1) {
			strncpy(search, line, 32);
			search[31] = '\0';
		}
	}

	printf("Looking for %s\n", search);

	int j;
	for (j=0; j<25; ++j) {

#ifdef HASH
		int idx = hash(search) % counter;
		printf("%u => %s", arr[idx]->key, arr[idx]->value);
#else
		for (i=0; i<counter; ++i) {
			if (!strcmp(arr[i]->value, search))
				printf("%u => %s", arr[i]->key, arr[i]->value);
		}
#endif

	}

	fclose(fp);

	free(arr);
	return 0;
}
