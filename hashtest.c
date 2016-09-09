/*
 * -----------------------------  hashtest.c  ------------------------------
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
 * Test hash vs full scan benchmark.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WORDFILE "words.txt"

static const int counter = 4096 * 55;

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

int memtest() {
	FILE *fp = fopen(WORDFILE, "r");
	if (!fp)
		return 1;

	struct kv **arr = calloc(sizeof(struct kv *), counter);

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
		printf("%u ==> %s", arr[idx]->key, arr[idx]->value);
#else
		for (i=0; i<counter; ++i) {
			if (!strcmp(arr[i]->value, search))
				printf("%u => %s", arr[i]->key, arr[i]->value);
		}
#endif

	}

	free(arr);

	fclose(fp);
}

#define PAGE_SIZE	1024

int disktest() {
	FILE *fp = fopen(WORDFILE, "r");
	if (!fp)
		return 1;

	FILE *fpo = fopen("outbin", "w+b");
	if (!fpo)
		return 1;

	char search[32];

	int i;
	for (i=0; i<counter; ++i) {
		char *line = NULL;
		size_t len = 0;
		ssize_t read = getline(&line, &len, fp);

#ifdef HASH
		int offset = hash(line) % counter;
#else
		int offset = i;
#endif

		fseek(fpo, i * (sizeof(struct kv) + (sizeof(char) * 32)), SEEK_SET);

		struct kv page;
		page.key = (unsigned long)i;
		page.value = malloc(32);
		strncpy(page.value, line, 32);

		if (i == counter - 1) {
			strncpy(search, line, 32);
			search[31] = '\0';
		}

		fwrite(&page, sizeof(struct kv), 1, fpo);
		fwrite(page.value, sizeof(char), 32, fpo);

		free(page.value);
	}

	printf("Looking for %s\n", search);

	fflush(fpo);

	int j;
	for (j=0; j<45; ++j) {
#ifdef HASH
		int offset = hash(search) % counter;

		char buffer[32];
		struct kv page;

		fseek(fpo, offset * (sizeof(struct kv) + (sizeof(char) * 32)), SEEK_SET);
		fread(&page, sizeof(struct kv), 1, fpo);
		fread(buffer, sizeof(char), 32, fpo);

		printf("%u ==> %s", page.key, buffer);
#else
		fseek(fpo, 0, SEEK_SET);

		for (i=0; i<counter; ++i) {
			char buffer[32];
			struct kv page;

			fread(&page, sizeof(struct kv), 1, fpo);
			fread(buffer, sizeof(char), 32, fpo);

			buffer[31] = '\0';

			if (!strcmp(buffer, search))
				printf("%u => %s", page.key, buffer);
		}
#endif
	}

	fclose(fpo);

	fclose(fp);
}

int main(int argc, char *agrv[]) {

	// memtest();
	disktest();

	return 0;
}
