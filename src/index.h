#ifndef INDEX_H
#define INDEX_H


#include <stdint.h>


void index_init();
void index_shutdown();
int index_create(const char *tablename);
int index_put(const char *tablename, int64_t key, long offset);
int index_get(const char *tablename, int64_t key, long *offset_out);