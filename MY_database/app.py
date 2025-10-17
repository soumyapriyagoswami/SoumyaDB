from flask import Flask, render_template_string, request, jsonify
from flask_cors import CORS
import subprocess
import json
import os
import tempfile
import re

app = Flask(__name__)
CORS(app)

# Path to your compiled C DBMS executable
DBMS_EXECUTABLE = "./dbms"  # Adjust path as needed
DB_DIR = "dbms_data"

def run_dbms_command(query):
    """Execute DBMS query and capture output"""
    try:
        # Execute the DBMS with the query
        process = subprocess.Popen(
            [DBMS_EXECUTABLE],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=os.getcwd()
        )
        
        stdout, stderr = process.communicate(input=query, timeout=10)
        
        if process.returncode != 0 and stderr:
            return {"success": False, "error": stderr}
        
        return {"success": True, "data": stdout if stdout else "Query executed successfully"}
    
    except subprocess.TimeoutExpired:
        process.kill()
        return {"success": False, "error": "Query execution timeout"}
    except FileNotFoundError:
        return {"success": False, "error": f"DBMS executable not found at {DBMS_EXECUTABLE}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Advanced DBMS Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --secondary: #ec4899;
            --success: #10b981;
            --danger: #ef4444;
            --warning: #f59e0b;
            --bg: #0f172a;
            --bg-secondary: #1e293b;
            --border: #334155;
            --text: #f1f5f9;
            --text-secondary: #cbd5e1;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, var(--bg) 0%, #1a1f35 100%);
            color: var(--text);
            min-height: 100vh;
        }

        .navbar {
            background: rgba(15, 23, 42, 0.95);
            border-bottom: 2px solid var(--border);
            padding: 1rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            backdrop-filter: blur(10px);
            position: sticky;
            top: 0;
            z-index: 100;
        }

        .navbar h1 {
            font-size: 1.5rem;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .nav-stats {
            display: flex;
            gap: 2rem;
            font-size: 0.9rem;
        }

        .stat {
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }

        .stat-value {
            font-weight: bold;
            color: var(--primary);
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
            display: grid;
            grid-template-columns: 250px 1fr;
            gap: 2rem;
        }

        .sidebar {
            background: var(--bg-secondary);
            border-radius: 12px;
            border: 1px solid var(--border);
            padding: 1.5rem;
            height: fit-content;
            position: sticky;
            top: 80px;
        }

        .sidebar h3 {
            color: var(--primary);
            margin-bottom: 1rem;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .nav-menu {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .nav-item {
            padding: 0.75rem 1rem;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s ease;
            border: 1px solid transparent;
            font-size: 0.9rem;
        }

        .nav-item:hover {
            background: rgba(99, 102, 241, 0.1);
            border-color: var(--primary);
            color: var(--primary);
        }

        .nav-item.active {
            background: var(--primary);
            color: white;
        }

        .main {
            display: flex;
            flex-direction: column;
            gap: 1.5rem;
        }

        .card {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            transition: all 0.3s ease;
        }

        .card:hover {
            border-color: var(--primary);
            box-shadow: 0 0 20px rgba(99, 102, 241, 0.1);
        }

        .card h2 {
            color: var(--primary);
            margin-bottom: 1rem;
            font-size: 1.2rem;
        }

        .input-group {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }

        input, textarea, select {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border);
            color: var(--text);
            padding: 0.75rem;
            border-radius: 8px;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            flex: 1;
        }

        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: var(--primary);
            background: rgba(99, 102, 241, 0.05);
            box-shadow: 0 0 10px rgba(99, 102, 241, 0.2);
        }

        .btn {
            padding: 0.75rem 1.5rem;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 500;
            transition: all 0.3s ease;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .btn-primary {
            background: var(--primary);
            color: white;
        }

        .btn-primary:hover {
            background: var(--primary-dark);
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(99, 102, 241, 0.3);
        }

        .btn-success {
            background: var(--success);
            color: white;
        }

        .btn-danger {
            background: var(--danger);
            color: white;
        }

        .btn-small {
            padding: 0.5rem 1rem;
            font-size: 0.8rem;
        }

        .query-box {
            margin-bottom: 1rem;
        }

        textarea {
            width: 100%;
            min-height: 120px;
            font-family: 'Courier New', monospace;
            resize: vertical;
        }

        .output {
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1rem;
            max-height: 400px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 0.85rem;
            white-space: pre-wrap;
            word-break: break-word;
            margin-top: 1rem;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }

        table th {
            background: rgba(99, 102, 241, 0.1);
            padding: 1rem;
            text-align: left;
            border-bottom: 2px solid var(--primary);
            font-weight: 600;
        }

        table td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
        }

        table tr:hover {
            background: rgba(99, 102, 241, 0.05);
        }

        .action-btns {
            display: flex;
            gap: 0.5rem;
        }

        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
            backdrop-filter: blur(5px);
        }

        .modal.active {
            display: flex;
        }

        .modal-content {
            background: var(--bg-secondary);
            border: 2px solid var(--primary);
            border-radius: 12px;
            padding: 2rem;
            max-width: 500px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        }

        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
        }

        .modal-header h2 {
            color: var(--primary);
        }

        .close-btn {
            background: none;
            border: none;
            color: var(--text);
            font-size: 1.5rem;
            cursor: pointer;
        }

        .form-group {
            margin-bottom: 1rem;
        }

        .form-group label {
            display: block;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
            font-weight: 500;
            color: var(--text-secondary);
        }

        .alert {
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            display: none;
        }

        .alert.success {
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid var(--success);
            color: var(--success);
            display: block;
        }

        .alert.error {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid var(--danger);
            color: var(--danger);
            display: block;
        }

        .hidden {
            display: none !important;
        }

        .columns-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.5rem;
            margin-bottom: 0.5rem;
        }

        @media (max-width: 768px) {
            .container {
                grid-template-columns: 1fr;
            }

            .sidebar {
                position: static;
            }

            .columns-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="navbar">
        <h1>🗄️ Futuristic Database Management System -Soumyapriya Goswami</h1>
        <div class="nav-stats">
            <div class="stat">
                <span>Tables:</span>
                <span class="stat-value" id="tableCount">0</span>
            </div>
        </div>
    </div>

    <div class="container">
        <aside class="sidebar">
            <h3>Operations</h3>
            <div class="nav-menu">
                <div class="nav-item active" data-section="query">📝 Query Console</div>
                <div class="nav-item" data-section="create">➕ Create Table</div>
                <div class="nav-item" data-section="view">👁️ View Tables</div>
                <div class="nav-item" data-section="manage">✏️ Manage Data</div>
            </div>
        </aside>

        <main class="main">
            <!-- Query Console Section -->
            <div class="section" id="query-section">
                <div class="card">
                    <h2>📝 Query Console</h2>
                    <div class="query-box">
                        <textarea id="queryInput" placeholder="Enter your SQL query here...&#10;Example: SHOW TABLES;&#10;Example: SELECT * FROM users;"></textarea>
                    </div>
                    <button class="btn btn-primary" onclick="executeQuery()">▶ Execute Query</button>
                    <div class="alert" id="alert"></div>
                    <div class="output hidden" id="queryOutput"></div>
                </div>
            </div>

            <!-- Create Table Section -->
            <div class="section hidden" id="create-section">
                <div class="card">
                    <h2>➕ Create New Table</h2>
                    <div class="form-group">
                        <label>Table Name</label>
                        <input type="text" id="tableName" placeholder="Enter table name">
                    </div>
                    <div id="columnsContainer"></div>
                    <button class="btn btn-primary btn-small" onclick="addColumnField()">+ Add Column</button>
                    <button class="btn btn-success" onclick="createTable()" style="margin-top: 1rem; width: 100%;">✓ Create Table</button>
                    <div class="alert" id="createAlert"></div>
                </div>
            </div>

            <!-- View Tables Section -->
            <div class="section hidden" id="view-section">
                <div class="card">
                    <h2>👁️ View Tables</h2>
                    <button class="btn btn-primary btn-small" onclick="loadTables()">🔄 Refresh</button>
                    <div id="tablesContainer" style="margin-top: 1rem;"></div>
                </div>
            </div>

            <!-- Manage Data Section -->
            <div class="section hidden" id="manage-section">
                <div class="card">
                    <h2>✏️ Manage Data</h2>
                    <div class="form-group">
                        <label>Select Table</label>
                        <select id="manageTableSelect" onchange="loadTableData()">
                            <option value="">-- Select a table --</option>
                        </select>
                    </div>
                    <div id="dataContainer" style="margin-top: 1rem;"></div>
                </div>
            </div>
        </main>
    </div>

    <!-- Modal -->
    <div class="modal" id="dataModal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modalTitle">Insert Data</h2>
                <button class="close-btn" onclick="closeModal()">&times;</button>
            </div>
            <div class="alert" id="modalAlert"></div>
            <div id="modalForm"></div>
            <button class="btn btn-success" onclick="submitModalData()" style="width: 100%; margin-top: 1rem;">✓ Submit</button>
        </div>
    </div>

    <script>
        const API_URL = 'http://localhost:5000/api';

        // Navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', () => {
                document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                
                const section = item.dataset.section;
                document.querySelectorAll('.section').forEach(s => s.classList.add('hidden'));
                document.getElementById(`${section}-section`).classList.remove('hidden');
                
                if (section === 'view') loadTables();
                if (section === 'create') initializeCreateForm();
                if (section === 'manage') loadTablesList();
            });
        });

        // Alert function
        function showAlert(message, type = 'error', containerId = 'alert') {
            const alert = document.getElementById(containerId);
            alert.textContent = message;
            alert.className = `alert ${type}`;
            setTimeout(() => {
                if (type === 'success') {
                    alert.textContent = '';
                    alert.className = 'alert';
                }
            }, 4000);
        }

        // Execute Query
        async function executeQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                showAlert('Please enter a query');
                return;
            }

            try {
                const response = await fetch(`${API_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query })
                });

                const result = await response.json();
                const output = document.getElementById('queryOutput');
                
                if (result.success) {
                    output.textContent = result.data;
                    output.classList.remove('hidden');
                    showAlert('✓ Query executed successfully', 'success');
                } else {
                    output.textContent = '❌ Error:\\n' + result.error;
                    output.classList.remove('hidden');
                    showAlert(result.error);
                }
            } catch (error) {
                showAlert('Connection error: ' + error.message);
            }
        }

        // Initialize Create Form
        function initializeCreateForm() {
            const container = document.getElementById('columnsContainer');
            if (container.children.length === 0) {
                container.innerHTML = `
                    <div class="form-group">
                        <label>Column 1</label>
                        <div class="columns-grid">
                            <input type="text" class="colName" placeholder="Column name">
                            <select class="colType">
                                <option>INT</option>
                                <option>FLOAT</option>
                                <option>VARCHAR</option>
                            </select>
                        </div>
                    </div>
                `;
            }
        }

        // Add Column Field
        function addColumnField() {
            const container = document.getElementById('columnsContainer');
            const count = container.children.length + 1;
            const group = document.createElement('div');
            group.className = 'form-group';
            group.innerHTML = `
                <label>Column ${count}</label>
                <div class="columns-grid">
                    <input type="text" class="colName" placeholder="Column name">
                    <select class="colType">
                        <option>INT</option>
                        <option>FLOAT</option>
                        <option>VARCHAR</option>
                    </select>
                </div>
            `;
            container.appendChild(group);
        }

        // Create Table
        async function createTable() {
            const tableName = document.getElementById('tableName').value.trim();
            if (!tableName) {
                showAlert('Please enter a table name', 'error', 'createAlert');
                return;
            }

            const columns = [];
            document.querySelectorAll('#columnsContainer .form-group').forEach(group => {
                const name = group.querySelector('.colName').value.trim();
                const type = group.querySelector('.colType').value;
                if (name) columns.push({ name, type });
            });

            if (columns.length === 0) {
                showAlert('Please add at least one column', 'error', 'createAlert');
                return;
            }

            const colDefs = columns.map(c => `${c.name} ${c.type}`).join(', ');
            const query = `CREATE TABLE ${tableName} (${colDefs});`;

            try {
                const response = await fetch(`${API_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query })
                });

                const result = await response.json();
                if (result.success) {
                    showAlert('✓ Table created successfully', 'success', 'createAlert');
                    document.getElementById('tableName').value = '';
                    document.getElementById('columnsContainer').innerHTML = '';
                    initializeCreateForm();
                } else {
                    showAlert(result.error, 'error', 'createAlert');
                }
            } catch (error) {
                showAlert(error.message, 'error', 'createAlert');
            }
        }

        // Load Tables
        async function loadTables() {
            try {
                const response = await fetch(`${API_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: 'SHOW TABLES;' })
                });

                const result = await response.json();
                const container = document.getElementById('tablesContainer');
                
                if (result.success) {
                    const output = result.data;
                    container.innerHTML = `<div class="output" style="margin-top: 0;">${output}</div>`;
                } else {
                    container.innerHTML = `<div class="alert error" style="display: block;">Error: ${result.error}</div>`;
                }
            } catch (error) {
                document.getElementById('tablesContainer').innerHTML = `<div class="alert error" style="display: block;">Error: ${error.message}</div>`;
            }
        }

        // Load Tables List for Manage
        async function loadTablesList() {
            try {
                const response = await fetch(`${API_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: 'SHOW TABLES;' })
                });

                const result = await response.json();
                const select = document.getElementById('manageTableSelect');
                
                if (result.success) {
                    const lines = result.data.split('\\n').filter(line => 
                        line.trim() && !line.includes('---') && !line.includes('Tables')
                    );
                    select.innerHTML = '<option value="">-- Select a table --</option>';
                    lines.forEach(line => {
                        const tableName = line.split('(')[0].trim();
                        if (tableName) {
                            select.innerHTML += `<option value="${tableName}">${tableName}</option>`;
                        }
                    });
                }
            } catch (error) {
                console.error(error);
            }
        }

        // Load Table Data
        async function loadTableData() {
            const tableName = document.getElementById('manageTableSelect').value;
            if (!tableName) return;

            try {
                const response = await fetch(`${API_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: `SELECT * FROM ${tableName};` })
                });

                const result = await response.json();
                const container = document.getElementById('dataContainer');
                
                if (result.success) {
                    container.innerHTML = `
                        <button class="btn btn-primary btn-small" onclick="openInsertModal('${tableName}')">+ Insert Record</button>
                        <div class="output" style="margin-top: 1rem;">${result.data}</div>
                    `;
                } else {
                    container.innerHTML = `<div class="alert error" style="display: block;">Error: ${result.error}</div>`;
                }
            } catch (error) {
                document.getElementById('dataContainer').innerHTML = `<div class="alert error" style="display: block;">Error: ${error.message}</div>`;
            }
        }

        // Modal Functions
        function openInsertModal(tableName) {
            document.getElementById('modalTitle').textContent = `Insert into ${tableName}`;
            document.getElementById('dataModal').classList.add('active');
        }

        function closeModal() {
            document.getElementById('dataModal').classList.remove('active');
        }

        function submitModalData() {
            alert('Implement insert with proper form parsing');
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute SQL query"""
    data = request.json
    query = data.get('query', '').strip()
    
    if not query:
        return jsonify({"success": False, "error": "Query cannot be empty"}), 400
    
    result = run_dbms_command(query)
    return jsonify(result)

if __name__ == '__main__':
    print("Starting DBMS Dashboard...")
    print("Open http://localhost:5000 in your browser")
    print(f"Make sure your compiled DBMS executable is at: {DBMS_EXECUTABLE}")
    app.run(debug=True, port=5000)