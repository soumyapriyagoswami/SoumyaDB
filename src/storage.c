#include "storage.h"
// format: name|ncols|colname:type|colname:type...
char *tok = strtok(buf, "|");
if (!tok) { free(buf); fclose(fp); return 0; }
strncpy(schema->name, tok, sizeof(schema->name));
tok = strtok(NULL, "|");
if (!tok) { free(buf); fclose(fp); return 0; }
schema->ncols = atoi(tok);
for (int i=0;i<schema->ncols;i++) {
char *c = strtok(NULL, "|");
if (!c) break;
char coln[64]; int t;
sscanf(c, "%63[^:]:%d", coln, &t);
strncpy(schema->colnames[i], coln, sizeof(schema->colnames[i]));
schema->types[i] = (ColType)t;
}
free(buf);
fclose(fp);
return 1;
}


int insert_row(const char *tablename, int64_t pkey, const char *textval) {
TableSchema s;
if (!read_schema(tablename, &s)) { printf("Table not found.\n"); return 0; }
// open file and append
char path[256];
snprintf(path, sizeof(path), "data/%s.tbl", tablename);
FILE *fp = fopen(path, "ab+");
if (!fp) return 0;
// record offset
if (fseek(fp, 0, SEEK_END)!=0) { fclose(fp); return 0; }
long offset = ftell(fp);
// write record
uint16_t tlen = (uint16_t)strlen(textval);
if (tlen > MAX_TEXT) tlen = MAX_TEXT;
fwrite(&pkey, sizeof(int64_t), 1, fp);
fwrite(&tlen, sizeof(uint16_t), 1, fp);
fwrite(textval, 1, tlen, fp);
// flush
fflush(fp);
fclose(fp);
// update index
index_put(tablename, pkey, offset);
printf("Inserted key=%lld at offset=%ld\n", (long long)pkey, offset);
return 1;
}


int select_all(const char *tablename) {
TableSchema s;
if (!read_schema(tablename, &s)) { printf("Table not found.\n"); return 0; }
char path[256];
snprintf(path, sizeof(path), "data/%s.tbl", tablename);
FILE *fp = fopen(path, "rb");
if (!fp) return 0;
uint32_t hlen;
if (fread(&hlen, sizeof(hlen), 1, fp)!=1) { fclose(fp); return 0; }
if (fseek(fp, hlen + sizeof(hlen), SEEK_SET)!=0) { fclose(fp); return 0; }
printf("-- Contents of %s --\n", tablename);
while (1) {
int64_t pkey;
uint16_t tlen;
if (fread(&pkey, sizeof(int64_t),1,fp)!=1) break;
if (fread(&tlen, sizeof(uint16_t),1,fp)!=1) break;
char buf[MAX_TEXT+1];
if (tlen>MAX_TEXT) tlen=MAX_TEXT;
if (fread(buf,1,tlen,fp)!=tlen) break;
buf[tlen]='\0';
printf("%lld | %s\n", (long long)pkey, buf);
}
fclose(fp);
return 1;
}