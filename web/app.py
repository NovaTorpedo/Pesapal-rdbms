from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import sys
import os
import json
import datetime

# Add parent directory to path to import pesapal_rdbms modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from core import Database, ConstraintError
from parser import SQLParser

app = Flask(__name__, static_folder='static', template_folder='template')
CORS(app)

# Initialize DB
db = Database('users_db.json')
parser = SQLParser(db)

def init_db():
    """Initialize the users table schema."""
    try:
        # Schema matches the frontend expectations
        db.create_table("users", {
            "id": "int", 
            "full_name": "str", 
            "email": "str",
            "role": "str",
            "department": "str",
            "status": "str",
            "joined_date": "str"
        }, pk="id")
        
        # Indexes for performance
        t = db.get_table("users")
        t.create_index("email", unique=True)
        t.create_index("role", unique=False)
        
        print("Table 'users' initialized.")
    except Exception:
        pass # Table likely exists

init_db()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/users', methods=['GET'])
def get_users():
    try:
        res = parser.execute("SELECT * FROM users")
        users = json.loads(res)
        # Sort by ID descending (newest first)
        users.sort(key=lambda x: x['id'], reverse=True)
        return jsonify(users)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.json
    try:
        # 1. Generate ID (Max + 1)
        try:
            existing_res = parser.execute("SELECT * FROM users")
            existing = json.loads(existing_res)
            new_id = max([u['id'] for u in existing]) + 1 if existing else 1
        except:
            new_id = 1

        # 2. Set defaults
        joined = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # 3. Construct Query (Fixed: Must be single line for regex parser)
        sql = f"INSERT INTO users (id, full_name, email, role, department, status, joined_date) VALUES ({new_id}, '{data['full_name']}', '{data['email']}', '{data['role']}', '{data['department']}', '{data['status']}', '{joined}')"
        
        parser.execute(sql)
        db.save()
        return jsonify({"status": "success", "id": new_id}), 201
    except ConstraintError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    try:
        res = parser.execute(f"SELECT * FROM users WHERE id = {user_id}")
        data = json.loads(res)
        if not data:
            return jsonify({"error": "User not found"}), 404
        return jsonify(data[0])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/users/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.json
    try:
        # Check existence
        check = parser.execute(f"SELECT * FROM users WHERE id = {user_id}")
        if not json.loads(check):
            return jsonify({"error": "User not found"}), 404

        # Update fields one by one (Parser limitation)
        fields = ['full_name', 'email', 'role', 'department', 'status']
        for field in fields:
            if field in data:
                sql = f"UPDATE users SET {field} = '{data[field]}' WHERE id = {user_id}"
                parser.execute(sql)
        
        db.save()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    try:
        parser.execute(f"DELETE FROM users WHERE id = {user_id}")
        db.save()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        users = json.loads(parser.execute("SELECT * FROM users"))
        total = len(users)
        active = sum(1 for u in users if u.get('status') == 'Active')
        departments = len(set(u.get('department') for u in users if u.get('department')))
        
        return jsonify({
            "total_users": total,
            "active_users": active,
            "departments": departments
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)