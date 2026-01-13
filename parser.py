import re
import json
from .core import Database, DBError

class SQLParser:
    def __init__(self, db: Database):
        self.db = db

    def execute(self, query: str):
        query = query.strip()
        
        # Router for commands
        if re.match(r'^CREATE TABLE', query, re.IGNORECASE):
            return self._handle_create(query)
        elif re.match(r'^INSERT INTO', query, re.IGNORECASE):
            return self._handle_insert(query)
        elif re.match(r'^SELECT', query, re.IGNORECASE):
            return self._handle_select(query)
        elif re.match(r'^UPDATE', query, re.IGNORECASE):
            return self._handle_update(query)
        elif re.match(r'^DELETE FROM', query, re.IGNORECASE):
            return self._handle_delete(query)
        elif re.match(r'^HELP', query, re.IGNORECASE):
            return self._help()
        else:
            raise DBError("Syntax Error: Unknown command.")

    def _parse_where(self, where_clause: str):
        """Simple parser: WHERE col = val"""
        if not where_clause:
            return None
        # Only supporting simple equality 'col = val' for now
        match = re.search(r'(\w+)\s*=\s*(.+)', where_clause)
        if match:
            col, val = match.groups()
            return {col: self._convert_type(val.strip())}
        return {}

    def _convert_type(self, val: str):
        """Guess type from string value."""
        if val.startswith("'") and val.endswith("'"):
            return val[1:-1]
        if val.lower() == 'true': return True
        if val.lower() == 'false': return False
        
        # Try Integer
        try:
            return int(val)
        except ValueError:
            pass
            
        # Try Float
        try:
            return float(val)
        except ValueError:
            pass
            
        return val

    def _handle_create(self, query):
        # CREATE TABLE users (id INT, name TEXT) PRIMARY KEY id
        # Regex to capture table name and columns part
        pattern = r"CREATE TABLE\s+(\w+)\s*\((.+)\)(?:\s+PRIMARY KEY\s+(\w+))?"
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            raise DBError("Syntax Error in CREATE TABLE")
        
        table_name, cols_str, pk = match.groups()
        columns = {}
        for col_def in cols_str.split(','):
            parts = col_def.strip().split()
            c_name = parts[0]
            # Normalize types to internal representation
            c_type = parts[1].lower().replace('text', 'str').replace('integer', 'int')
            columns[c_name] = c_type
            
        return self.db.create_table(table_name, columns, pk)

    def _handle_insert(self, query):
        # INSERT INTO users (id, name) VALUES (1, 'John')
        pattern = r"INSERT INTO\s+(\w+)\s*\((.+)\)\s*VALUES\s*\((.+)\)"
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            raise DBError("Syntax Error in INSERT")
        
        table_name, cols_str, vals_str = match.groups()
        cols = [c.strip() for c in cols_str.split(',')]
        
        # Split values respecting quotes (simple split, imperfect for commas inside quotes)
        vals = [self._convert_type(v.strip()) for v in vals_str.split(',')]
        
        if len(cols) != len(vals):
            raise DBError("Column/Value count mismatch")
            
        data = dict(zip(cols, vals))
        t = self.db.get_table(table_name)
        rid = t.insert(data)
        return f"Inserted 1 row into {table_name}, ID: {rid}"

    def _handle_select(self, query):
        # SELECT * FROM users WHERE id = 1
        # OR JOIN: SELECT * FROM users JOIN orders ON users.id = orders.user_id
        
        # Check for JOIN
        join_pattern = r"SELECT\s+\*\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\s*=\s*(\w+)"
        join_match = re.match(join_pattern, query, re.IGNORECASE)
        if join_match:
            t1, t2, k1, k2 = join_match.groups()
            # Remove table prefixes if present (users.id -> id)
            k1 = k1.split('.')[-1]
            k2 = k2.split('.')[-1]
            results = self.db.join(t1, t2, k1, k2)
            return json.dumps(results, indent=2, default=str)

        # Standard SELECT
        pattern = r"SELECT\s+\*\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            raise DBError("Syntax Error in SELECT")
        
        table_name, where_clause = match.groups()
        where = self._parse_where(where_clause)
        
        t = self.db.get_table(table_name)
        results = t.select(where)
        return json.dumps(results, indent=2, default=str)

    def _handle_update(self, query):
        # UPDATE users SET name = 'Jane' WHERE id = 1
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)"
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            raise DBError("Syntax Error in UPDATE (WHERE clause required)")
        
        table_name, set_clause, where_clause = match.groups()
        
        # Parse SET (only one field supported for simplicity)
        if '=' not in set_clause:
             raise DBError("Syntax Error in SET clause")
        key, val = set_clause.split('=', 1)
        new_data = {key.strip(): self._convert_type(val.strip())}
        
        where = self._parse_where(where_clause)
        t = self.db.get_table(table_name)
        count = t.update(where, new_data)
        return f"Updated {count} rows."

    def _handle_delete(self, query):
        # DELETE FROM users WHERE id = 1
        pattern = r"DELETE FROM\s+(\w+)\s+WHERE\s+(.+)"
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            raise DBError("Syntax Error in DELETE (WHERE clause required)")
            
        table_name, where_clause = match.groups()
        where = self._parse_where(where_clause)
        t = self.db.get_table(table_name)
        count = t.delete(where)
        return f"Deleted {count} rows."

    def _help(self):
        return """
        PESAPAL RDBMS HELP
        ==================
        
        1. CREATE TABLE
           Syntax:  CREATE TABLE <name> (<col> <type>, ...) PRIMARY KEY <col>
           Example: CREATE TABLE users (id INT, name STR) PRIMARY KEY id
           
        2. INSERT DATA
           Syntax:  INSERT INTO <name> (<cols>) VALUES (<vals>)
           Example: INSERT INTO users (id, name) VALUES (1, 'Alice')
           
        3. READ DATA (SELECT)
           Syntax:  SELECT * FROM <name> [WHERE <col>=<val>]
           Example: SELECT * FROM users WHERE id = 1
           
        4. UPDATE DATA
           Syntax:  UPDATE <name> SET <col>=<val> WHERE <col>=<val>
           Example: UPDATE users SET name = 'Bob' WHERE id = 1
           
        5. DELETE DATA
           Syntax:  DELETE FROM <name> WHERE <col>=<val>
           Example: DELETE FROM users WHERE id = 1
           
        6. JOIN TABLES
           Syntax:  SELECT * FROM <t1> JOIN <t2> ON <t1_col>=<t2_col>
           Example: SELECT * FROM users JOIN orders ON id = user_id

        Supported Types: INT, STR, BOOL, FLOAT
        """