#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifdef _WIN32
    #include <io.h>
    #include <fcntl.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <windows.h>
    #define open _open
    #define close _close
    #define read _read
    #define write _write
    #define lseek _lseek
    #define ssize_t int
#else
    #include <fcntl.h>
    #include <unistd.h>
    #include <sys/file.h>
#endif

// Constants
#define MAX_NAME 50
#define MAX_FIELD 50
#define MAX_RECORDS 10000
#define ORDER 4
#define MAX_QUERY 512
#define MAX_TABLES 50
#define MAX_COLUMNS 10

// Column definition
typedef struct Column {
    char name[MAX_FIELD];
    char type[20]; // INT, FLOAT, VARCHAR
    int size;
} Column;

// Table schema
typedef struct TableSchema {
    char name[MAX_FIELD];
    Column columns[MAX_COLUMNS];
    int num_columns;
    int primary_key_index;
} TableSchema;

// Generic record (flexible storage)
typedef struct Record {
    int id;
    char data[MAX_COLUMNS][MAX_FIELD];
} Record;

// B+-tree node
typedef struct BPTNode {
    int keys[ORDER];
    struct BPTNode* children[ORDER + 1];
    long offsets[ORDER];
    int num_keys;
    int is_leaf;
    struct BPTNode* next;
} BPTNode;

// Table structure
typedef struct Table {
    TableSchema schema;
    BPTNode* root;
    int record_count;
    int fd;
} Table;

// Database structure
typedef struct Database {
    Table tables[MAX_TABLES];
    int num_tables;
    char* db_dir;
} Database;

// Function prototypes
Database* createDatabase(const char* db_dir);
void createTable(Database* db, const char* table_name, Column* columns, int num_columns, int pk_index);
Table* findTable(Database* db, const char* table_name);
void listTables(Database* db);
void describeTable(Database* db, const char* table_name);
void insertRecord(Database* db, const char* table_name, Record* rec);
void updateRecord(Database* db, const char* table_name, int id, Record* rec);
void deleteRecord(Database* db, const char* table_name, int id);
Record* findRecord(Table* table, int id);
void selectRecords(Table* table, int min_id, int max_id);
void selectAllRecords(Table* table);
BPTNode* createBPTNode(int is_leaf);
void insertIntoBPTree(Table* table, int key, long offset);
void insertIntoBPTreeRecursive(BPTNode* node, int key, long offset);
BPTNode* findLeaf(BPTNode* node, int key);
void splitChild(BPTNode* parent, int index);
void displayRecord(Table* table, Record* rec);
void freeBPTree(BPTNode* node);
void freeDatabase(Database* db);
char* trim(char* str);
void processQuery(Database* db, char* query);
long getNextOffset(int fd);
char* stristr(const char* haystack, const char* needle);
void saveTableSchema(Database* db, Table* table);
void loadTableSchemas(Database* db);
void loadRecords(Table* table);

// Platform-specific file locking
#ifdef _WIN32
void lockFile(int fd, int exclusive) {
    HANDLE hFile = (HANDLE)_get_osfhandle(fd);
    OVERLAPPED overlapped = {0};
    DWORD flags = exclusive ? LOCKFILE_EXCLUSIVE_LOCK : 0;
    LockFileEx(hFile, flags, 0, MAXDWORD, MAXDWORD, &overlapped);
}

void unlockFile(int fd) {
    HANDLE hFile = (HANDLE)_get_osfhandle(fd);
    OVERLAPPED overlapped = {0};
    UnlockFileEx(hFile, 0, MAXDWORD, MAXDWORD, &overlapped);
}
#else
void lockFile(int fd, int exclusive) {
    flock(fd, exclusive ? LOCK_EX : LOCK_SH);
}

void unlockFile(int fd) {
    flock(fd, LOCK_UN);
}
#endif

// Create database
Database* createDatabase(const char* db_dir) {
    Database* db = (Database*)malloc(sizeof(Database));
    if (!db) return NULL;
    
    db->num_tables = 0;
    db->db_dir = strdup(db_dir);
    
    // Create directory if it doesn't exist
#ifdef _WIN32
    CreateDirectoryA(db_dir, NULL);
#else
    mkdir(db_dir, 0755);
#endif
    
    loadTableSchemas(db);
    return db;
}

// Trim whitespace
char* trim(char* str) {
    char* end;
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return str;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';
    return str;
}

// Case-insensitive string search
char* stristr(const char* haystack, const char* needle) {
    if (!*needle) return (char*)haystack;
    for (; *haystack; haystack++) {
        if (toupper(*haystack) == toupper(*needle)) {
            const char* h = haystack;
            const char* n = needle;
            while (*h && *n && toupper(*h) == toupper(*n)) {
                h++;
                n++;
            }
            if (!*n) return (char*)haystack;
        }
    }
    return NULL;
}

// Get next offset
long getNextOffset(int fd) {
    return lseek(fd, 0, SEEK_END);
}

// Create B+-tree node
BPTNode* createBPTNode(int is_leaf) {
    BPTNode* node = (BPTNode*)malloc(sizeof(BPTNode));
    if (node) {
        node->num_keys = 0;
        node->is_leaf = is_leaf;
        node->next = NULL;
        for (int i = 0; i < ORDER + 1; i++) {
            node->children[i] = NULL;
        }
        for (int i = 0; i < ORDER; i++) {
            node->offsets[i] = -1;
        }
    }
    return node;
}

// Save table schema
void saveTableSchema(Database* db, Table* table) {
    char schema_file[256];
    snprintf(schema_file, sizeof(schema_file), "%s/schemas.dat", db->db_dir);
    
    FILE* fp = fopen(schema_file, "ab");
    if (fp) {
        fwrite(&table->schema, sizeof(TableSchema), 1, fp);
        fclose(fp);
    }
}

// Load table schemas
void loadTableSchemas(Database* db) {
    char schema_file[256];
    snprintf(schema_file, sizeof(schema_file), "%s/schemas.dat", db->db_dir);
    
    FILE* fp = fopen(schema_file, "rb");
    if (!fp) return;
    
    TableSchema schema;
    while (fread(&schema, sizeof(TableSchema), 1, fp) == 1) {
        if (db->num_tables >= MAX_TABLES) break;
        
        Table* table = &db->tables[db->num_tables];
        table->schema = schema;
        table->root = createBPTNode(1);
        table->record_count = 0;
        
        // Open data file
        char data_file[256];
        snprintf(data_file, sizeof(data_file), "%s/%s.dat", db->db_dir, schema.name);
#ifdef _WIN32
        table->fd = open(data_file, _O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
        table->fd = open(data_file, O_CREAT | O_RDWR, 0644);
#endif
        
        if (table->fd >= 0) {
            loadRecords(table);
            db->num_tables++;
        }
    }
    
    fclose(fp);
}

// Load records from table file
void loadRecords(Table* table) {
    lseek(table->fd, 0, SEEK_SET);
    Record rec;
    long offset = 0;
    
    while (read(table->fd, &rec, sizeof(Record)) == sizeof(Record)) {
        if (rec.id != 0) {
            insertIntoBPTree(table, rec.id, offset);
            table->record_count++;
        }
        offset += sizeof(Record);
    }
}

// Create table
void createTable(Database* db, const char* table_name, Column* columns, int num_columns, int pk_index) {
    if (db->num_tables >= MAX_TABLES) {
        printf("Error: Maximum number of tables reached!\n");
        return;
    }
    
    if (findTable(db, table_name)) {
        printf("Error: Table '%s' already exists!\n", table_name);
        return;
    }
    
    Table* table = &db->tables[db->num_tables];
    strncpy(table->schema.name, table_name, MAX_FIELD - 1);
    table->schema.num_columns = num_columns;
    table->schema.primary_key_index = pk_index;
    
    for (int i = 0; i < num_columns; i++) {
        table->schema.columns[i] = columns[i];
    }
    
    table->root = createBPTNode(1);
    table->record_count = 0;
    
    // Create data file
    char data_file[256];
    snprintf(data_file, sizeof(data_file), "%s/%s.dat", db->db_dir, table_name);
#ifdef _WIN32
    table->fd = open(data_file, _O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    table->fd = open(data_file, O_CREAT | O_RDWR, 0644);
#endif
    
    if (table->fd < 0) {
        printf("Error: Could not create table file!\n");
        return;
    }
    
    saveTableSchema(db, table);
    db->num_tables++;
    printf("Table '%s' created successfully.\n", table_name);
}

// Find table by name
Table* findTable(Database* db, const char* table_name) {
    for (int i = 0; i < db->num_tables; i++) {
        if (strcasecmp(db->tables[i].schema.name, table_name) == 0) {
            return &db->tables[i];
        }
    }
    return NULL;
}

// List all tables
void listTables(Database* db) {
    if (db->num_tables == 0) {
        printf("No tables in database.\n");
        return;
    }
    
    printf("\n--- Tables ---\n");
    for (int i = 0; i < db->num_tables; i++) {
        printf("%s (%d records)\n", db->tables[i].schema.name, db->tables[i].record_count);
    }
    printf("--- End ---\n");
}

// Describe table structure
void describeTable(Database* db, const char* table_name) {
    Table* table = findTable(db, table_name);
    if (!table) {
        printf("Error: Table '%s' not found!\n", table_name);
        return;
    }
    
    printf("\n--- Table: %s ---\n", table->schema.name);
    printf("Column Name          Type          Primary Key\n");
    printf("------------------------------------------------\n");
    for (int i = 0; i < table->schema.num_columns; i++) {
        printf("%-20s %-13s %s\n", 
               table->schema.columns[i].name,
               table->schema.columns[i].type,
               (i == table->schema.primary_key_index) ? "YES" : "NO");
    }
    printf("--- End ---\n");
}

// Split child node
void splitChild(BPTNode* parent, int index) {
    BPTNode* full_child = parent->children[index];
    BPTNode* new_child = createBPTNode(full_child->is_leaf);
    
    int mid = ORDER / 2;
    
    if (full_child->is_leaf) {
        new_child->num_keys = ORDER - mid;
        for (int i = 0; i < new_child->num_keys; i++) {
            new_child->keys[i] = full_child->keys[mid + i];
            new_child->offsets[i] = full_child->offsets[mid + i];
        }
        new_child->next = full_child->next;
        full_child->next = new_child;
        full_child->num_keys = mid;
    } else {
        new_child->num_keys = ORDER - mid - 1;
        for (int i = 0; i < new_child->num_keys; i++) {
            new_child->keys[i] = full_child->keys[mid + 1 + i];
            new_child->children[i] = full_child->children[mid + 1 + i];
        }
        new_child->children[new_child->num_keys] = full_child->children[ORDER];
        full_child->num_keys = mid;
    }
    
    for (int i = parent->num_keys; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->children[i + 1] = parent->children[i];
    }
    parent->keys[index] = full_child->keys[mid];
    parent->children[index + 1] = new_child;
    parent->num_keys++;
}

// Insert into non-full node
void insertIntoBPTreeRecursive(BPTNode* node, int key, long offset) {
    int i = node->num_keys - 1;
    
    if (node->is_leaf) {
        while (i >= 0 && node->keys[i] > key) {
            node->keys[i + 1] = node->keys[i];
            node->offsets[i + 1] = node->offsets[i];
            i--;
        }
        node->keys[i + 1] = key;
        node->offsets[i + 1] = offset;
        node->num_keys++;
    } else {
        while (i >= 0 && node->keys[i] > key) i--;
        i++;
        
        if (node->children[i]->num_keys == ORDER) {
            splitChild(node, i);
            if (key > node->keys[i]) i++;
        }
        insertIntoBPTreeRecursive(node->children[i], key, offset);
    }
}

// Insert into B+-tree
void insertIntoBPTree(Table* table, int key, long offset) {
    if (!table->root) {
        table->root = createBPTNode(1);
    }
    
    if (table->root->num_keys == ORDER) {
        BPTNode* new_root = createBPTNode(0);
        new_root->children[0] = table->root;
        splitChild(new_root, 0);
        table->root = new_root;
    }
    
    insertIntoBPTreeRecursive(table->root, key, offset);
}

// Find leaf node
BPTNode* findLeaf(BPTNode* node, int key) {
    if (!node) return NULL;
    if (node->is_leaf) return node;
    
    int i = 0;
    while (i < node->num_keys && key > node->keys[i]) i++;
    return findLeaf(node->children[i], key);
}

// Find record by ID
Record* findRecord(Table* table, int id) {
    static Record rec;
    BPTNode* leaf = findLeaf(table->root, id);
    
    for (int i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == id) {
            lockFile(table->fd, 0);
            lseek(table->fd, leaf->offsets[i], SEEK_SET);
            ssize_t bytes = read(table->fd, &rec, sizeof(Record));
            unlockFile(table->fd);
            if (bytes == sizeof(Record) && rec.id == id) return &rec;
        }
    }
    return NULL;
}

// Display record
void displayRecord(Table* table, Record* rec) {
    printf("ID: %d", rec->id);
    for (int i = 1; i < table->schema.num_columns; i++) {
        printf(", %s: %s", table->schema.columns[i].name, rec->data[i]);
    }
    printf("\n");
}

// Insert record
void insertRecord(Database* db, const char* table_name, Record* rec) {
    Table* table = findTable(db, table_name);
    if (!table) {
        printf("Error: Table '%s' not found!\n", table_name);
        return;
    }
    
    if (findRecord(table, rec->id)) {
        printf("Error: Record with ID %d already exists!\n", rec->id);
        return;
    }
    
    lockFile(table->fd, 1);
    long offset = getNextOffset(table->fd);
    lseek(table->fd, offset, SEEK_SET);
    write(table->fd, rec, sizeof(Record));
    insertIntoBPTree(table, rec->id, offset);
    table->record_count++;
    unlockFile(table->fd);
    printf("Record inserted successfully.\n");
}

// Update record
void updateRecord(Database* db, const char* table_name, int id, Record* rec) {
    Table* table = findTable(db, table_name);
    if (!table) {
        printf("Error: Table '%s' not found!\n", table_name);
        return;
    }
    
    BPTNode* leaf = findLeaf(table->root, id);
    long offset = -1;
    for (int i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == id) {
            offset = leaf->offsets[i];
            break;
        }
    }
    
    if (offset == -1) {
        printf("Error: Record not found!\n");
        return;
    }
    
    rec->id = id;
    lockFile(table->fd, 1);
    lseek(table->fd, offset, SEEK_SET);
    write(table->fd, rec, sizeof(Record));
    unlockFile(table->fd);
    printf("Record updated successfully.\n");
}

// Delete record
void deleteRecord(Database* db, const char* table_name, int id) {
    Table* table = findTable(db, table_name);
    if (!table) {
        printf("Error: Table '%s' not found!\n", table_name);
        return;
    }
    
    BPTNode* leaf = findLeaf(table->root, id);
    long offset = -1;
    int key_index = -1;
    
    for (int i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == id) {
            offset = leaf->offsets[i];
            key_index = i;
            break;
        }
    }
    
    if (offset == -1) {
        printf("Error: Record not found!\n");
        return;
    }
    
    lockFile(table->fd, 1);
    Record empty = {0};
    lseek(table->fd, offset, SEEK_SET);
    write(table->fd, &empty, sizeof(Record));
    
    for (int i = key_index; i < leaf->num_keys - 1; i++) {
        leaf->keys[i] = leaf->keys[i + 1];
        leaf->offsets[i] = leaf->offsets[i + 1];
    }
    leaf->num_keys--;
    
    table->record_count--;
    unlockFile(table->fd);
    printf("Record deleted successfully.\n");
}

// Select all records
void selectAllRecords(Table* table) {
    printf("\n--- All Records from %s ---\n", table->schema.name);
    BPTNode* leaf = table->root;
    while (leaf && !leaf->is_leaf) leaf = leaf->children[0];
    
    int found = 0;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            Record rec;
            lockFile(table->fd, 0);
            lseek(table->fd, leaf->offsets[i], SEEK_SET);
            read(table->fd, &rec, sizeof(Record));
            unlockFile(table->fd);
            if (rec.id != 0) {
                displayRecord(table, &rec);
                found++;
            }
        }
        leaf = leaf->next;
    }
    if (!found) printf("No records found.\n");
    printf("--- End ---\n");
}

// Select records in range
void selectRecords(Table* table, int min_id, int max_id) {
    if (min_id > max_id) {
        printf("Error: Invalid range!\n");
        return;
    }
    printf("\n--- Records in Range %d to %d ---\n", min_id, max_id);
    BPTNode* leaf = table->root;
    while (leaf && !leaf->is_leaf) leaf = leaf->children[0];
    
    int found = 0;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            if (leaf->keys[i] >= min_id && leaf->keys[i] <= max_id) {
                Record rec;
                lockFile(table->fd, 0);
                lseek(table->fd, leaf->offsets[i], SEEK_SET);
                read(table->fd, &rec, sizeof(Record));
                unlockFile(table->fd);
                if (rec.id != 0) {
                    displayRecord(table, &rec);
                    found++;
                }
            }
        }
        leaf = leaf->next;
    }
    if (!found) printf("No records found.\n");
    printf("--- End ---\n");
}

// Free B+-tree
void freeBPTree(BPTNode* node) {
    if (!node) return;
    if (!node->is_leaf) {
        for (int i = 0; i <= node->num_keys; i++) {
            freeBPTree(node->children[i]);
        }
    }
    free(node);
}

// Free database
void freeDatabase(Database* db) {
    if (!db) return;
    for (int i = 0; i < db->num_tables; i++) {
        freeBPTree(db->tables[i].root);
        close(db->tables[i].fd);
    }
    free(db->db_dir);
    free(db);
}

// Process query
void processQuery(Database* db, char* query) {
    char query_copy[MAX_QUERY];
    strncpy(query_copy, query, MAX_QUERY - 1);
    query_copy[MAX_QUERY - 1] = '\0';
    
    char* token = strtok(query_copy, " \n;");
    if (!token) {
        printf("Error: Empty query!\n");
        return;
    }

    char command[20];
    strncpy(command, token, sizeof(command) - 1);
    command[sizeof(command) - 1] = '\0';
    for (int i = 0; command[i]; i++) command[i] = toupper(command[i]);

    if (strcmp(command, "CREATE") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "TABLE") != 0) {
            printf("Error: Expected 'TABLE' after CREATE!\n");
            return;
        }
        
        token = strtok(NULL, " (\n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        char table_name[MAX_FIELD];
        strncpy(table_name, trim(token), MAX_FIELD - 1);
        
        // Parse columns
        Column columns[MAX_COLUMNS];
        int num_columns = 0;
        int pk_index = 0;
        
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected column definitions!\n");
            return;
        }
        
        // Simple parsing: column_name type, column_name type, ...
        char* col_start = token;
        while (*col_start && num_columns < MAX_COLUMNS) {
            while (*col_start && (isspace(*col_start) || *col_start == '(' || *col_start == ',')) col_start++;
            if (!*col_start || *col_start == ')') break;
            
            // Get column name
            char col_name[MAX_FIELD] = {0};
            int j = 0;
            while (*col_start && !isspace(*col_start) && *col_start != ',' && *col_start != ')' && j < MAX_FIELD - 1) {
                col_name[j++] = *col_start++;
            }
            col_name[j] = '\0';
            
            // Skip whitespace
            while (*col_start && isspace(*col_start)) col_start++;
            
            // Get type
            char col_type[20] = {0};
            j = 0;
            while (*col_start && !isspace(*col_start) && *col_start != ',' && *col_start != ')' && j < 19) {
                col_type[j++] = toupper(*col_start++);
            }
            col_type[j] = '\0';
            
            strncpy(columns[num_columns].name, col_name, MAX_FIELD - 1);
            strncpy(columns[num_columns].type, col_type, 19);
            columns[num_columns].size = MAX_FIELD;
            
            if (num_columns == 0) pk_index = 0; // First column is PK
            
            num_columns++;
            
            // Skip to next column
            while (*col_start && *col_start != ',' && *col_start != ')') col_start++;
        }
        
        if (num_columns > 0) {
            createTable(db, table_name, columns, num_columns, pk_index);
        } else {
            printf("Error: No columns defined!\n");
        }
    }
    else if (strcmp(command, "SHOW") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "TABLES") != 0) {
            printf("Error: Expected 'TABLES' after SHOW!\n");
            return;
        }
        listTables(db);
    }
    else if (strcmp(command, "DESCRIBE") == 0 || strcmp(command, "DESC") == 0) {
        token = strtok(NULL, " \n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        describeTable(db, token);
    }
    else if (strcmp(command, "INSERT") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "INTO") != 0) {
            printf("Error: Expected 'INTO' after INSERT!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        char table_name[MAX_FIELD];
        strncpy(table_name, token, MAX_FIELD - 1);
        
        Table* table = findTable(db, table_name);
        if (!table) {
            printf("Error: Table '%s' not found!\n", table_name);
            return;
        }
        
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "VALUES") != 0) {
            printf("Error: Expected 'VALUES'!\n");
            return;
        }
        
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected values!\n");
            return;
        }
        
        Record rec = {0};
        
        // Parse ID
        char* val_start = token;
        while (*val_start && (*val_start == ' ' || *val_start == '(')) val_start++;
        rec.id = atoi(val_start);
        
        // Parse other values
        int col_idx = 1;
        while (*val_start && *val_start != ',') val_start++;
        if (*val_start == ',') val_start++;
        
        while (*val_start && col_idx < table->schema.num_columns) {
            while (*val_start && (isspace(*val_start) || *val_start == ',')) val_start++;
            
            if (*val_start == '\'' || *val_start == '\"') {
                char quote = *val_start++;
                int j = 0;
                while (*val_start && *val_start != quote && j < MAX_FIELD - 1) {
                    rec.data[col_idx][j++] = *val_start++;
                }
                rec.data[col_idx][j] = '\0';
                if (*val_start == quote) val_start++;
            } else {
                int j = 0;
                while (*val_start && *val_start != ',' && *val_start != ')' && j < MAX_FIELD - 1) {
                    if (!isspace(*val_start)) {
                        rec.data[col_idx][j++] = *val_start;
                    }
                    val_start++;
                }
                rec.data[col_idx][j] = '\0';
            }
            col_idx++;
        }
        
        insertRecord(db, table_name, &rec);
    }
    else if (strcmp(command, "SELECT") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcmp(token, "*") != 0) {
            printf("Error: Expected '*'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "FROM") != 0) {
            printf("Error: Expected 'FROM'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        
        char table_name[MAX_FIELD];
        strncpy(table_name, token, MAX_FIELD - 1);
        
        Table* table = findTable(db, table_name);
        if (!table) {
            printf("Error: Table '%s' not found!\n", table_name);
            return;
        }
        
        token = strtok(NULL, " \n");
        if (!token) {
            selectAllRecords(table);
        } else if (strcasecmp(token, "WHERE") == 0) {
            token = strtok(NULL, " \n");
            if (!token || strcasecmp(token, "id") != 0) {
                printf("Error: Expected 'id'!\n");
                return;
            }
            token = strtok(NULL, " \n");
            if (!token) {
                printf("Error: Expected condition!\n");
                return;
            }
            if (strcasecmp(token, "=") == 0) {
                token = strtok(NULL, " ;\n");
                if (!token) {
                    printf("Error: Expected ID value!\n");
                    return;
                }
                int id = atoi(token);
                Record* rec = findRecord(table, id);
                if (rec) {
                    printf("\n--- Result ---\n");
                    displayRecord(table, rec);
                    printf("--- End ---\n");
                } else {
                    printf("No records found.\n");
                }
            } else if (strcasecmp(token, "BETWEEN") == 0) {
                token = strtok(NULL, " \n");
                if (!token) {
                    printf("Error: Expected min ID!\n");
                    return;
                }
                int min_id = atoi(token);
                token = strtok(NULL, " \n");
                if (!token || strcasecmp(token, "AND") != 0) {
                    printf("Error: Expected 'AND'!\n");
                    return;
                }
                token = strtok(NULL, " ;\n");
                if (!token) {
                    printf("Error: Expected max ID!\n");
                    return;
                }
                int max_id = atoi(token);
                selectRecords(table, min_id, max_id);
            } else {
                printf("Error: Unsupported condition!\n");
            }
        }
    }
    else if (strcmp(command, "UPDATE") == 0) {
        token = strtok(NULL, " \n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        char table_name[MAX_FIELD];
        strncpy(table_name, token, MAX_FIELD - 1);
        
        Table* table = findTable(db, table_name);
        if (!table) {
            printf("Error: Table '%s' not found!\n", table_name);
            return;
        }
        
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "SET") != 0) {
            printf("Error: Expected 'SET'!\n");
            return;
        }
        
        Record rec = {0};
        int id = -1;
        
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected SET values!\n");
            return;
        }
        
        // Parse SET clause
        for (int col = 1; col < table->schema.num_columns; col++) {
            char* col_pos = stristr(token, table->schema.columns[col].name);
            if (col_pos) {
                char* eq = strchr(col_pos, '=');
                if (eq) {
                    eq++;
                    while (*eq && isspace(*eq)) eq++;
                    if (*eq == '\'' || *eq == '\"') {
                        char quote = *eq++;
                        int j = 0;
                        while (*eq && *eq != quote && j < MAX_FIELD - 1) {
                            rec.data[col][j++] = *eq++;
                        }
                        rec.data[col][j] = '\0';
                    } else {
                        int j = 0;
                        while (*eq && *eq != ',' && !isspace(*eq) && j < MAX_FIELD - 1) {
                            rec.data[col][j++] = *eq++;
                        }
                        rec.data[col][j] = '\0';
                    }
                }
            }
        }
        
        // Parse WHERE clause
        char* where_pos = stristr(token, "WHERE");
        if (where_pos) {
            char* id_pos = stristr(where_pos, "id");
            if (id_pos) {
                char* eq = strchr(id_pos, '=');
                if (eq) {
                    eq++;
                    id = atoi(eq);
                }
            }
        }
        
        if (id == -1) {
            printf("Error: Invalid UPDATE syntax!\n");
            return;
        }
        
        updateRecord(db, table_name, id, &rec);
    }
    else if (strcmp(command, "DELETE") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "FROM") != 0) {
            printf("Error: Expected 'FROM'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token) {
            printf("Error: Expected table name!\n");
            return;
        }
        char table_name[MAX_FIELD];
        strncpy(table_name, token, MAX_FIELD - 1);
        
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected WHERE clause!\n");
            return;
        }
        
        char* where_pos = stristr(token, "WHERE");
        if (!where_pos) {
            printf("Error: Expected 'WHERE'!\n");
            return;
        }
        
        char* id_pos = stristr(where_pos, "id");
        if (!id_pos) {
            printf("Error: Expected 'id'!\n");
            return;
        }
        
        char* eq = strchr(id_pos, '=');
        if (!eq) {
            printf("Error: Expected '='!\n");
            return;
        }
        
        eq++;
        while (*eq && isspace(*eq)) eq++;
        
        int id = atoi(eq);
        if (id == 0 && *eq != '0') {
            printf("Error: Invalid ID value!\n");
            return;
        }
        
        deleteRecord(db, table_name, id);
    }
    else {
        printf("Error: Unknown command '%s'!\n", command);
    }
}

int main() {
    Database* db = createDatabase("dbms_data");
    if (!db) {
        printf("Failed to initialize database!\n");
        return 1;
    }

    char query[MAX_QUERY];
    /*printf("Multi-Table DBMS (Type 'EXIT' to quit)\n");
    printf("Loaded %d tables.\n", db->num_tables);
    printf("\nSupported commands:\n");
    printf("  CREATE TABLE table_name (col1 type, col2 type, ...)\n");
    printf("  SHOW TABLES\n");
    printf("  DESCRIBE table_name\n");
    printf("  INSERT INTO table_name VALUES (val1, 'val2', ...)\n");
    printf("  SELECT * FROM table_name [WHERE id = value]\n");
    printf("  SELECT * FROM table_name WHERE id BETWEEN min AND max\n");
    printf("  UPDATE table_name SET col='val' WHERE id = value\n");
    printf("  DELETE FROM table_name WHERE id = value\n");*/
    
    while (1) {
        //printf("\nQuery> ");
        if (!fgets(query, MAX_QUERY, stdin)) break;
        query[strcspn(query, "\n")] = 0;
        if (strcasecmp(query, "EXIT") == 0) break;
        if (strlen(trim(query)) == 0) continue;
        processQuery(db, query);
    }

    freeDatabase(db);
    printf("Database closed. Goodbye!\n");
    return 0;
}