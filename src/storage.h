#ifndef STORAGE_H
#define STORAGE_H


#include <stdint.h>


#define MAX_TEXT 256


typedef enum { T_INT=1, T_TEXT=2 } ColType;


typedef struct {
char name[64];
int ncols;
ColType types[16];
char colnames[16][64];
} TableSchema;


// storage API
int create_table(const char *tablename, TableSchema *schema);
int insert_row(const char *tablename, int64_t pkey, const char *textval);
int select_all(const char *tablename);