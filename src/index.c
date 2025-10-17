#include "index.h"
// On put, append to file and store in memory (simple linked list). For production you'd want a proper hash or B-Tree.


typedef struct Entry {
int64_t key;
long offset;
struct Entry *next;
} Entry;


typedef struct TableIndex {
char name[64];
Entry *head;
struct TableIndex *next;
} TableIndex;


static TableIndex *tables = NULL;


static TableIndex *find_table(const char *name) {
TableIndex *t = tables;
while (t) { if (strcmp(t->name,name)==0) return t; t=t->next; }
return NULL;
}


void index_init() {
// lazily load indexes when create or put happens. For simplicity do nothing here.
}


void index_shutdown() {
TableIndex *t = tables;
while (t) {
TableIndex *nx = t->next;
Entry *e = t->head;
while (e) { Entry *en = e->next; free(e); e=en; }
free(t);
t=nx;
}
tables = NULL;
}


int index_create(const char *tablename) {
if (find_table(tablename)) return 1; // exists
TableIndex *t = malloc(sizeof(TableIndex));
strncpy(t->name, tablename, sizeof(t->name)); t->head = NULL; t->next = tables; tables = t;
// write empty file
char path[256]; snprintf(path,sizeof(path),"data/%s.idx", tablename);
FILE *fp = fopen(path,"w"); if (fp) fclose(fp);
return 1;
}


int index_put(const char *tablename, int64_t key, long offset) {
TableIndex *t = find_table(tablename);
if (!t) {
index_create(tablename);
t = find_table(tablename);
}
Entry *e = malloc(sizeof(Entry)); e->key = key; e->offset = offset; e->next = t->head; t->head = e;
// append to file
char path[256]; snprintf(path,sizeof(path),"data/%s.idx", tablename);
FILE *fp = fopen(path,"a"); if (!fp) return 0;
fprintf(fp, "%lld %ld\n", (long long)key, offset);
fclose(fp);
return 1;
}


int index_get(const char *tablename, int64_t key, long *offset_out) {
TableIndex *t = find_table(tablename);
if (!t) {
// try loading file
char path[256]; snprintf(path,sizeof(path),"data/%s.idx", tablename);
FILE *fp = fopen(path,"r"); if (!fp) return 0;
index_create(tablename);
t = find_table(tablename);
long k; long off; while (fscanf(fp, "%lld %ld", &k, &off)==2) {
index_put(tablename, k, off);
}
fclose(fp);
}
Entry *e = t->head;
while (e) { if (e->key==key) { *offset_out = e->offset; return 1; } e=e->next; }
return 0;
}