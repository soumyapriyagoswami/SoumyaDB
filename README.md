<h1 align="center">🌈 <b>🌐 SoumyaDB — Your Lightweight, Fast & Persistent C-based DBMS</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Made%20with-C99-orange.svg">
  <img src="https://img.shields.io/badge/License-BSD%202--Clause-blue.svg">
  <img src="https://img.shields.io/badge/Platform-Linux%20|%20macOS%20|%20Windows-green.svg">
</p>

**SoumyaDB** is a lightweight, file-based, multi-table **Database Management System (DBMS)** built in **C**, designed to combine **speed, simplicity, and persistence**. Featuring **B+ tree indexing** for fast record access and SQL-like query support, SoumyaDB is perfect for **learning, research prototypes, and embedded systems**.

---

## 🌟 Features

- **Multi-table Support**  
  Store and manage multiple tables with independent schemas.  

- **Persistent Storage**  
  Table schemas and records are saved to disk, allowing data to persist between sessions.

- **B+ Tree Indexing**  
  Fast record lookups by primary key (ID) using a B+ tree structure.

- **SQL-like Query Support**  

```sql
CREATE TABLE table_name (col1 type, col2 type, ...);
INSERT INTO table_name VALUES (val1, 'val2', ...);
SELECT * FROM table_name [WHERE id = value | BETWEEN min AND max];
UPDATE table_name SET col='val' WHERE id=value;
DELETE FROM table_name WHERE id=value;
SHOW TABLES;
DESCRIBE table_name;
```
### 🔒 Cross-platform File Locking
Ensures safe concurrent access on Windows and Linux.

### 📊 Flexible Column Types
Supports INT, FLOAT, and VARCHAR (as strings).

### 🏗️ Lightweight and Modular
Easily extendable for new features like joins, transactions, or indexing improvements.

## 🖥️ Installation & Usage

### Clone the repository:
```bash
git clone https://github.com/your-username/SoumyaDB.git
cd src
```

### Compile the DBMS:
```bash
gcc main.c -o soumyadb

```
### Run SoumyaDB:
```bash
./soumyadb
```
### Example Queries

```bash

CREATE TABLE members (id INT, name VARCHAR, grade FLOAT, dept VARCHAR)
CREATE TABLE employees (id INT, name VARCHAR, salary FLOAT, position VARCHAR)
SHOW TABLES
DESCRIBE students
DESCRIBE employees
INSERT INTO students VALUES (101, 'Alice', 95.5, 'CS')
INSERT INTO employees VALUES (1, 'Bob', 75000, 'Manager')
SELECT * FROM students
SELECT * FROM employees
INSERT INTO students VALUES (105, 'Soumyapriya Goswami', 8.5, 'Information Technology');
SELECT * FROM students WHERE id = 101;
SELECT * FROM students WHERE id BETWEEN 100 AND 200;
UPDATE students SET name = 'Alice Jones', grade = 90.0, dept = 'CS' WHERE id = 101;
SELECT * FROM students WHERE id = 101;
DELETE FROM students WHERE id = 101;
EXIT
```
## 🤝 Contributing

<p align="center">
  <img src="https://img.shields.io/badge/PRs-Welcome-brightgreen.svg?style=for-the-badge" alt="PRs Welcome">
  <img src="https://img.shields.io/badge/Open%20Source-%E2%9D%A4-red.svg?style=for-the-badge" alt="Open Source Love">
  <img src="https://img.shields.io/badge/Contributions-Encouraged-blue.svg?style=for-the-badge" alt="Contributions Encouraged">
  <img src="https://img.shields.io/badge/Made%20with-%F0%9F%92%9A%20Passion-yellow.svg?style=for-the-badge" alt="Made with Passion">
</p>

We welcome contributions from everyone! 🚀  

Here’s how you can help improve **EasyLang**:

1. **Fork** the repository  
2. **Create** a feature branch  
3. **Commit** your changes  
4. **Submit** a pull request  
### SoumyaDB is created for educational purposes, to learn about file-based storage, B+ trees, and SQL-like query parsing in C. It can also serve as a lightweight DBMS prototype for embedded systems or IoT projects.**
