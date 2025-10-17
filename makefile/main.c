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
#define MAX_DEPT 50
#define MAX_RECORDS 1000
#define ORDER 4 // B+-tree order
#define MAX_QUERY 256

// Record structure
typedef struct Record {
    int id;
    char name[MAX_NAME];
    float grade;
    char dept[MAX_DEPT];
} Record;

// B+-tree node structure
typedef struct BPTNode {
    int keys[ORDER];
    struct BPTNode* children[ORDER + 1];
    long offsets[ORDER]; // File offsets
    int num_keys;
    int is_leaf;
    struct BPTNode* next;
} BPTNode;

// Database structure
typedef struct Database {
    BPTNode* root;
    int record_count;
    char* data_file;
    int fd;
} Database;

// Function prototypes
Database* createDatabase(const char* filename);
BPTNode* createBPTNode(int is_leaf);
void insertRecord(Database* db, int id, const char* name, float grade, const char* dept);
void updateRecord(Database* db, int id, const char* name, float grade, const char* dept);
void deleteRecord(Database* db, int id);
Record* findRecord(Database* db, int id);
void selectRecords(Database* db, int min_id, int max_id);
void saveRecordToFile(Database* db, Record* rec, long offset);
void loadRecords(Database* db);
void insertIntoBPTree(Database* db, int key, long offset);
void insertIntoBPTreeRecursive(BPTNode* node, int key, long offset);
BPTNode* findLeaf(BPTNode* node, int key);
void splitChild(BPTNode* parent, int index);
void displayRecord(Record* rec);
void freeBPTree(BPTNode* node);
void freeDatabase(Database* db);
char* trim(char* str);
int validateInput(int id, const char* name, float grade, const char* dept);
void processQuery(Database* db, char* query);
long getNextOffset(Database* db);
char* stristr(const char* haystack, const char* needle);

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
Database* createDatabase(const char* filename) {
    Database* db = (Database*)malloc(sizeof(Database));
    if (!db) return NULL;
    db->root = createBPTNode(1);
    db->record_count = 0;
    db->data_file = strdup(filename);
#ifdef _WIN32
    db->fd = open(filename, _O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    db->fd = open(filename, O_CREAT | O_RDWR, 0644);
#endif
    if (db->fd < 0) {
        free(db->data_file);
        free(db->root);
        free(db);
        return NULL;
    }
    loadRecords(db);
    return db;
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

// Validate input
int validateInput(int id, const char* name, float grade, const char* dept) {
    if (id <= 0 || id > MAX_RECORDS) return 0;
    if (strlen(name) == 0 || strlen(name) >= MAX_NAME) return 0;
    if (grade < 0.0 || grade > 100.0) return 0;
    if (strlen(dept) == 0 || strlen(dept) >= MAX_DEPT) return 0;
    return 1;
}

// Get next available file offset
long getNextOffset(Database* db) {
    return lseek(db->fd, 0, SEEK_END);
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

// Insert record
void insertRecord(Database* db, int id, const char* name, float grade, const char* dept) {
    if (!validateInput(id, name, grade, dept)) {
        printf("Error: Invalid input!\n");
        return;
    }
    if (findRecord(db, id)) {
        printf("Error: Record with ID %d already exists!\n", id);
        return;
    }
    
    lockFile(db->fd, 1);
    Record rec = {id, {0}, grade, {0}};
    strncpy(rec.name, name, MAX_NAME - 1);
    strncpy(rec.dept, dept, MAX_DEPT - 1);
    
    long offset = getNextOffset(db);
    saveRecordToFile(db, &rec, offset);
    insertIntoBPTree(db, id, offset);
    db->record_count++;
    unlockFile(db->fd);
    printf("Record inserted successfully.\n");
}

// Update record
void updateRecord(Database* db, int id, const char* name, float grade, const char* dept) {
    if (!validateInput(id, name, grade, dept)) {
        printf("Error: Invalid input!\n");
        return;
    }
    
    // Find the record and its offset
    BPTNode* leaf = findLeaf(db->root, id);
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
    
    lockFile(db->fd, 1);
    Record rec = {id, {0}, grade, {0}};
    strncpy(rec.name, name, MAX_NAME - 1);
    strncpy(rec.dept, dept, MAX_DEPT - 1);
    
    lseek(db->fd, offset, SEEK_SET);
    write(db->fd, &rec, sizeof(Record));
    unlockFile(db->fd);
    printf("Record updated successfully.\n");
}

// Delete record
void deleteRecord(Database* db, int id) {
    BPTNode* leaf = findLeaf(db->root, id);
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
    
    lockFile(db->fd, 1);
    Record empty = {0};
    lseek(db->fd, offset, SEEK_SET);
    write(db->fd, &empty, sizeof(Record));
    
    // Remove from B+-tree (simple removal for leaf)
    for (int i = key_index; i < leaf->num_keys - 1; i++) {
        leaf->keys[i] = leaf->keys[i + 1];
        leaf->offsets[i] = leaf->offsets[i + 1];
    }
    leaf->num_keys--;
    
    db->record_count--;
    unlockFile(db->fd);
    printf("Record deleted successfully.\n");
}

// Find record by ID
Record* findRecord(Database* db, int id) {
    static Record rec;
    BPTNode* leaf = findLeaf(db->root, id);
    
    for (int i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == id) {
            lockFile(db->fd, 0);
            lseek(db->fd, leaf->offsets[i], SEEK_SET);
            ssize_t bytes = read(db->fd, &rec, sizeof(Record));
            unlockFile(db->fd);
            if (bytes == sizeof(Record) && rec.id == id) return &rec;
        }
    }
    return NULL;
}

// Select records in ID range
void selectRecords(Database* db, int min_id, int max_id) {
    if (min_id > max_id) {
        printf("Error: Invalid range!\n");
        return;
    }
    printf("\n--- Records in Range %d to %d ---\n", min_id, max_id);
    BPTNode* leaf = db->root;
    while (leaf && !leaf->is_leaf) leaf = leaf->children[0];
    
    int found = 0;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            if (leaf->keys[i] >= min_id && leaf->keys[i] <= max_id) {
                Record rec;
                lockFile(db->fd, 0);
                lseek(db->fd, leaf->offsets[i], SEEK_SET);
                read(db->fd, &rec, sizeof(Record));
                unlockFile(db->fd);
                if (rec.id != 0) {
                    displayRecord(&rec);
                    found++;
                }
            }
        }
        leaf = leaf->next;
    }
    if (!found) printf("No records found.\n");
    printf("--- End ---\n");
}

// Display a single record
void displayRecord(Record* rec) {
    printf("ID: %d, Name: %s, Grade: %.2f, Dept: %s\n", 
           rec->id, rec->name, rec->grade, rec->dept);
}

// Save record to file at specific offset
void saveRecordToFile(Database* db, Record* rec, long offset) {
    lseek(db->fd, offset, SEEK_SET);
    write(db->fd, rec, sizeof(Record));
}

// Load records from file
void loadRecords(Database* db) {
    lseek(db->fd, 0, SEEK_SET);
    Record rec;
    long offset = 0;
    
    while (read(db->fd, &rec, sizeof(Record)) == sizeof(Record)) {
        if (rec.id != 0) {
            insertIntoBPTree(db, rec.id, offset);
            db->record_count++;
        }
        offset += sizeof(Record);
    }
}

// Split child node at given index
void splitChild(BPTNode* parent, int index) {
    BPTNode* full_child = parent->children[index];
    BPTNode* new_child = createBPTNode(full_child->is_leaf);
    
    int mid = ORDER / 2;
    
    // Copy second half to new node
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
    
    // Insert middle key into parent
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
        // Insert into leaf
        while (i >= 0 && node->keys[i] > key) {
            node->keys[i + 1] = node->keys[i];
            node->offsets[i + 1] = node->offsets[i];
            i--;
        }
        node->keys[i + 1] = key;
        node->offsets[i + 1] = offset;
        node->num_keys++;
    } else {
        // Find child to insert into
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
void insertIntoBPTree(Database* db, int key, long offset) {
    if (!db->root) {
        db->root = createBPTNode(1);
    }
    
    if (db->root->num_keys == ORDER) {
        BPTNode* new_root = createBPTNode(0);
        new_root->children[0] = db->root;
        splitChild(new_root, 0);
        db->root = new_root;
    }
    
    insertIntoBPTreeRecursive(db->root, key, offset);
}

// Find leaf node for key
BPTNode* findLeaf(BPTNode* node, int key) {
    if (!node) return NULL;
    if (node->is_leaf) return node;
    
    int i = 0;
    while (i < node->num_keys && key > node->keys[i]) i++;
    return findLeaf(node->children[i], key);
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
    freeBPTree(db->root);
    close(db->fd);
    free(db->data_file);
    free(db);
}

// Process custom query
void processQuery(Database* db, char* query) {
    char query_copy[MAX_QUERY];
    strncpy(query_copy, query, MAX_QUERY - 1);
    query_copy[MAX_QUERY - 1] = '\0';
    
    char* token = strtok(query_copy, " \n;");
    if (!token) {
        printf("Error: Empty query!\n");
        return;
    }

    // Convert to uppercase for case-insensitive comparison
    char command[20];
    strncpy(command, token, sizeof(command) - 1);
    command[sizeof(command) - 1] = '\0';
    for (int i = 0; command[i]; i++) command[i] = toupper(command[i]);

    if (strcmp(command, "INSERT") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "INTO") != 0) {
            printf("Error: Expected 'INTO' after INSERT!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "students") != 0) {
            printf("Error: Expected table name 'students'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "VALUES") != 0) {
            printf("Error: Expected 'VALUES'!\n");
            return;
        }
        
        // Skip opening parenthesis and get ID
        token = strtok(NULL, " (,\n");
        if (!token) {
            printf("Error: Expected ID!\n");
            return;
        }
        int id = atoi(token);
        
        // Get name (handle quoted strings)
        token = strtok(NULL, "");  // Get rest of string
        if (!token) {
            printf("Error: Expected name!\n");
            return;
        }
        
        // Find the first quote or non-whitespace for name
        char* name_start = token;
        while (*name_start && (isspace(*name_start) || *name_start == ',')) name_start++;
        
        char name[MAX_NAME] = {0};
        if (*name_start == '\'' || *name_start == '\"') {
            char quote = *name_start;
            name_start++;
            char* name_end = strchr(name_start, quote);
            if (name_end) {
                int len = name_end - name_start;
                if (len >= MAX_NAME) len = MAX_NAME - 1;
                strncpy(name, name_start, len);
                name[len] = '\0';
                token = name_end + 1;
            }
        } else {
            sscanf(name_start, "%[^,]", name);
            token = strchr(name_start, ',');
        }
        
        // Get grade
        if (!token) {
            printf("Error: Expected grade!\n");
            return;
        }
        while (*token && (*token == ',' || isspace(*token))) token++;
        float grade = atof(token);
        
        // Move to department
        while (*token && *token != ',' && *token != ')') token++;
        if (*token == ',') token++;
        while (*token && isspace(*token)) token++;
        
        char dept[MAX_DEPT] = {0};
        if (*token == '\'' || *token == '\"') {
            char quote = *token;
            token++;
            char* dept_end = strchr(token, quote);
            if (dept_end) {
                int len = dept_end - token;
                if (len >= MAX_DEPT) len = MAX_DEPT - 1;
                strncpy(dept, token, len);
                dept[len] = '\0';
            }
        } else {
            sscanf(token, "%[^);]", dept);
        }
        
        insertRecord(db, id, trim(name), grade, trim(dept));
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
        if (!token || strcasecmp(token, "students") != 0) {
            printf("Error: Expected table name 'students'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "WHERE") != 0) {
            printf("Error: Expected 'WHERE'!\n");
            return;
        }
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
            Record* rec = findRecord(db, id);
            if (rec) {
                printf("\n--- Result ---\n");
                displayRecord(rec);
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
            selectRecords(db, min_id, max_id);
        } else {
            printf("Error: Unsupported condition!\n");
        }
    }
    else if (strcmp(command, "UPDATE") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "students") != 0) {
            printf("Error: Expected table name 'students'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "SET") != 0) {
            printf("Error: Expected 'SET'!\n");
            return;
        }
        
        char name[MAX_NAME] = {0};
        float grade = -1;
        char dept[MAX_DEPT] = {0};
        int id = -1;
        
        // Parse SET clause - get rest of query
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected SET values!\n");
            return;
        }
        
        // Parse name = 'value'
        char* name_pos = stristr(token, "name");
        if (name_pos) {
            char* eq = strchr(name_pos, '=');
            if (eq) {
                eq++;
                while (*eq && isspace(*eq)) eq++;
                if (*eq == '\'' || *eq == '\"') {
                    char quote = *eq;
                    eq++;
                    char* end = strchr(eq, quote);
                    if (end) {
                        int len = end - eq;
                        if (len >= MAX_NAME) len = MAX_NAME - 1;
                        strncpy(name, eq, len);
                        name[len] = '\0';
                    }
                }
            }
        }
        
        // Parse grade = value
        char* grade_pos = stristr(token, "grade");
        if (grade_pos) {
            char* eq = strchr(grade_pos, '=');
            if (eq) {
                eq++;
                grade = atof(eq);
            }
        }
        
        // Parse dept = 'value'
        char* dept_pos = stristr(token, "dept");
        if (dept_pos) {
            char* eq = strchr(dept_pos, '=');
            if (eq) {
                eq++;
                while (*eq && isspace(*eq)) eq++;
                if (*eq == '\'' || *eq == '\"') {
                    char quote = *eq;
                    eq++;
                    char* end = strchr(eq, quote);
                    if (end) {
                        int len = end - eq;
                        if (len >= MAX_DEPT) len = MAX_DEPT - 1;
                        strncpy(dept, eq, len);
                        dept[len] = '\0';
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
        
        if (id == -1 || strlen(name) == 0 || grade < 0 || strlen(dept) == 0) {
            printf("Error: Invalid UPDATE syntax!\n");
            return;
        }
        
        updateRecord(db, id, name, grade, dept);
    }
    else if (strcmp(command, "DELETE") == 0) {
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "FROM") != 0) {
            printf("Error: Expected 'FROM'!\n");
            return;
        }
        token = strtok(NULL, " \n");
        if (!token || strcasecmp(token, "students") != 0) {
            printf("Error: Expected table name 'students'!\n");
            return;
        }
        
        // Get rest of query
        token = strtok(NULL, "");
        if (!token) {
            printf("Error: Expected WHERE clause!\n");
            return;
        }
        
        // Find WHERE clause
        char* where_pos = stristr(token, "WHERE");
        if (!where_pos) {
            printf("Error: Expected 'WHERE'!\n");
            return;
        }
        
        // Find id = value
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
        
        deleteRecord(db, id);
    }
    else {
        printf("Error: Unknown command '%s'!\n", command);
    }
}

int main() {
    Database* db = createDatabase("database.bin");
    if (!db) {
        printf("Failed to initialize database!\n");
        return 1;
    }

    char query[MAX_QUERY];
    printf("Soumyapriya Database Management System (Type 'EXIT' to quit)\n");
    printf("Loaded %d existing records.\n", db->record_count);
    
    while (1) {
        printf("\nQuery> ");
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