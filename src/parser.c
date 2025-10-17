#include "storage.h"
char *p = strstr(buf, "CREATE TABLE");
p += 12;
while (*p==' ') p++;
char tname[64]; int i=0;
while (*p && *p!=' ' && *p!='(') { tname[i++]=*p++; }
tname[i]='\0';
char *lparen = strchr(buf, '(');
char *rparen = strrchr(buf, ')');
if (!lparen || !rparen) { printf("Bad CREATE syntax.\n"); return 0; }
char inside[512]; int inlen = rparen - lparen -1;
strncpy(inside, lparen+1, inlen); inside[inlen]='\0';
// parse cols
TableSchema s; memset(&s,0,sizeof(s)); strncpy(s.name, tname, sizeof(s.name));
char *tok = strtok(inside, ",");
int colidx=0;
while (tok && colidx<16) {
char coln[64]; int t;
trim(tok);
if (sscanf(tok, "%63[^:]:%d", coln, &t)!=2) {
// try textual types parsing like name:TEXT or name:INT
char c1[64], c2[64]; if (sscanf(tok, "%63[^:]:%63s", c1, c2)==2) {
strncpy(coln,c1,sizeof(coln));
if (strcasecmp(c2,"INT")==0) t=T_INT; else t=T_TEXT;
} else { printf("Bad column format '%s'\n", tok); return 0; }
}
strncpy(s.colnames[colidx], coln, sizeof(s.colnames[colidx]));
s.types[colidx] = (ColType)t;
colidx++;
tok = strtok(NULL, ",");
}
s.ncols = colidx;
if (s.ncols<1) { printf("Need at least one column\n"); return 0; }
if (s.types[0] != T_INT) { printf("First column must be INT primary key in this prototype.\n"); return 0; }
return create_table(tname, &s);
}
else if (strncasecmp(buf, "INSERT", 6)==0) {
// format: INSERT table 123 "text"
char tname[64]; long long key; char txt[512];
// naive parse
char *p = (char*)buf + 6; while (*p==' ') p++;
int n = sscanf(p, "%63s %lld \"%511[^"]\"", tname, &key, txt);
if (n<3) { printf("Bad INSERT syntax. Use: INSERT table 1 \"text\"\n"); return 0; }
return insert_row(tname, (int64_t)key, txt);
}
else if (strncasecmp(buf, "SELECT", 6)==0) {
// format: SELECT table
char tname[64]; char *p = (char*)buf + 6; while (*p==' ') p++;
if (sscanf(p, "%63s", tname)!=1) { printf("Bad SELECT syntax.\n"); return 0; }
return select_all(tname);
}
else {
printf("Unknown command.\n"); return 0;
}
}

