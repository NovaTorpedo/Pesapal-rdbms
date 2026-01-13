import json
import os
from typing import Any, Dict, List, Optional, Union

# Custom Exceptions for DB Errors
class DBError(Exception): pass
class ConstraintError(DBError): pass
class SchemaError(DBError): pass

class Table:
    """
    Represents a single table in the RDBMS.
    Manages rows, schema, indexes, and constraints.
    """
    def __init__(self, name: str, columns: Dict[str, str], pk: str = None):
        self.name = name
        self.columns = columns  # e.g., {'id': 'int', 'name': 'str'}
        self.pk = pk            # Primary Key column name
        self.rows = {}          # Storage: {row_id: {data}}
        self.indexes = {}       # {col_name: {value: [row_ids]}}
        self.next_row_id = 1
        self.unique_constraints = [] # List of columns that must be unique

        # Initialize Index for PK if exists
        if pk:
            self.create_index(pk, unique=True)

    def create_index(self, column: str, unique: bool = False):
        if column not in self.columns:
            raise SchemaError(f"Column {column} does not exist in table {self.name}")
        self.indexes[column] = {}
        if unique and column not in self.unique_constraints:
            self.unique_constraints.append(column)

    def _validate_types(self, data: Dict[str, Any]):
        """Ensures data types match schema."""
        for col, val in data.items():
            if col not in self.columns:
                raise SchemaError(f"Unknown column: {col}")
            
            # Skip validation for None here (handled by constraints or constraints check)
            if val is None:
                continue

            expected_type = self.columns[col]
            if expected_type == 'int' and not isinstance(val, int):
                raise SchemaError(f"Column {col} expects int, got {type(val)}")
            elif expected_type == 'str' and not isinstance(val, str):
                raise SchemaError(f"Column {col} expects str, got {type(val)}")
            elif expected_type == 'float' and not isinstance(val, (float, int)):
                # Allow int for float columns
                raise SchemaError(f"Column {col} expects float, got {type(val)}")
            elif expected_type == 'bool' and not isinstance(val, bool):
                 raise SchemaError(f"Column {col} expects bool, got {type(val)}")

    def _check_constraints(self, data: Dict[str, Any], ignore_row_id: int = None):
        """Checks PK and Unique constraints."""
        for col in self.unique_constraints:
            if col in data:
                val = data[col]
                # Check index for existence
                if col in self.indexes:
                    existing_ids = self.indexes[col].get(val, [])
                    # If updating, ignore self
                    if ignore_row_id:
                        existing_ids = [eid for eid in existing_ids if eid != ignore_row_id]
                    
                    if existing_ids:
                        raise ConstraintError(f"Unique constraint violation: {col}={val} already exists.")

    def _update_indexes(self, row_id: int, data: Dict[str, Any], old_data: Dict[str, Any] = None):
        """Updates internal dictionary indexes."""
        # Remove old values if updating
        if old_data:
            for col in self.indexes:
                if col in old_data:
                    val = old_data[col]
                    # Handle unhashable types if necessary
                    if val is not None and val in self.indexes[col] and row_id in self.indexes[col][val]:
                        self.indexes[col][val].remove(row_id)
                        if not self.indexes[col][val]:
                            del self.indexes[col][val]

        # Add new values
        for col, val in data.items():
            if col in self.indexes and val is not None:
                if val not in self.indexes[col]:
                    self.indexes[col][val] = []
                self.indexes[col][val].append(row_id)

    def insert(self, data: Dict[str, Any]):
        # 1. Check Primary Key Not Null explicitly
        if self.pk and data.get(self.pk) is None:
            raise ConstraintError(f"Primary key '{self.pk}' cannot be None.")

        # 2. Validate Types
        self._validate_types(data)

        # 3. Check Logical Constraints (Unique/PK existence)
        self._check_constraints(data)

        row_id = self.next_row_id
        self.rows[row_id] = data
        self._update_indexes(row_id, data)
        self.next_row_id += 1
        return row_id

    def select(self, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Basic SELECT. 
        Optimization: Uses index if the WHERE clause targets an indexed column.
        """
        results = []
        
        # 1. Try to use Index (O(1) or O(log N))
        if where and len(where) == 1:
            col, val = list(where.items())[0]
            if col in self.indexes:
                # If checking for a value that exists
                if val in self.indexes[col]:
                    for rid in self.indexes[col][val]:
                        row = self.rows[rid].copy()
                        row['_id'] = rid
                        results.append(row)
                    return results
                else:
                    return [] # Index hit but value not found

        # 2. Full Table Scan (O(N))
        for rid, row in self.rows.items():
            match = True
            if where:
                for col, target_val in where.items():
                    if row.get(col) != target_val:
                        match = False
                        break
            if match:
                r = row.copy()
                r['_id'] = rid
                results.append(r)
        return results

    def delete(self, where: Dict[str, Any]) -> int:
        rows_to_delete = self.select(where)
        count = 0
        for row in rows_to_delete:
            rid = row['_id']
            # Clean indexes
            self._update_indexes(rid, {}, old_data=self.rows[rid])
            del self.rows[rid]
            count += 1
        return count

    def update(self, where: Dict[str, Any], new_data: Dict[str, Any]) -> int:
        rows_to_update = self.select(where)
        count = 0
        for row in rows_to_update:
            rid = row['_id']
            current_data = self.rows[rid]
            
            # Merge data for validation
            updated_row = current_data.copy()
            updated_row.update(new_data)
            
            self._validate_types(updated_row)
            self._check_constraints(updated_row, ignore_row_id=rid)
            
            # Update Indexes
            self._update_indexes(rid, updated_row, old_data=current_data)
            
            # Commit
            self.rows[rid] = updated_row
            count += 1
        return count

class Database:
    """
    The main container for Tables.
    """
    def __init__(self, persistence_file='pesapal.json'):
        self.tables: Dict[str, Table] = {}
        self.persistence_file = persistence_file
        # Auto-load data on init
        self.load()

    def create_table(self, name: str, columns: Dict[str, str], pk: str = None):
        if name in self.tables:
            raise SchemaError(f"Table {name} already exists.")
        self.tables[name] = Table(name, columns, pk)
        return f"Table '{name}' created."

    def get_table(self, name: str) -> Table:
        if name not in self.tables:
            # Raise ValueError to be more generic/compatible with tests expecting standard errors
            raise ValueError(f"Table {name} not found.")
        return self.tables[name]
    
    def join(self, table1_name: str, table2_name: str, key1: str, key2: str):
        """
        Performs an INNER JOIN using nested loops (simplest implementation).
        """
        t1 = self.get_table(table1_name)
        t2 = self.get_table(table2_name)
        
        results = []
        
        for r1 in t1.rows.values():
            val1 = r1.get(key1)
            # Find matches in T2
            # Optimization: Use index of T2 if available
            if key2 in t2.indexes:
                 ids = t2.indexes[key2].get(val1, [])
                 for rid in ids:
                     r2 = t2.rows[rid]
                     combined = {**r1, **{f"{table2_name}_{k}": v for k, v in r2.items()}}
                     results.append(combined)
            else:
                # Scan T2
                for r2 in t2.rows.values():
                    if r2.get(key2) == val1:
                        combined = {**r1, **{f"{table2_name}_{k}": v for k, v in r2.items()}}
                        results.append(combined)
        return results

    def save(self):
        """Simple JSON persistence."""
        data = {}
        for tname, table in self.tables.items():
            data[tname] = {
                'columns': table.columns,
                'pk': table.pk,
                'rows': table.rows,
                'next_row_id': table.next_row_id,
                'indexes': table.indexes,
                'unique': table.unique_constraints
            }
        with open(self.persistence_file, 'w') as f:
            json.dump(data, f, default=str)

    def load(self):
        if not os.path.exists(self.persistence_file):
            return
        with open(self.persistence_file, 'r') as f:
            try:
                data = json.load(f)
                for tname, tdata in data.items():
                    t = Table(tname, tdata['columns'], tdata['pk'])
                    # Restore rows - ensure keys are ints
                    t.rows = {int(k): v for k, v in tdata['rows'].items()}
                    t.next_row_id = tdata['next_row_id']
                    t.unique_constraints = tdata['unique']
                    
                    # Reset indexes map
                    t.indexes = {} 
                    
                    # 1. Restore PK Index
                    if t.pk: 
                        t.create_index(t.pk, unique=True)
                    
                    # 2. Restore other indexes
                    # We check the saved 'indexes' dictionary keys to see which columns were indexed.
                    # We use unique_constraints to determine if they should be unique.
                    if 'indexes' in tdata:
                        for col in tdata['indexes']:
                            if col not in t.indexes:
                                is_unique = col in t.unique_constraints
                                t.create_index(col, unique=is_unique)
                    
                    # 3. Populate indexes from data
                    # It's safer to rebuild indexes from the rows rather than loading the raw index dict
                    # because it guarantees consistency and correct typing.
                    for rid, row in t.rows.items():
                        t._update_indexes(rid, row)
                        
                    self.tables[tname] = t
            except Exception as e:
                print(f"Warning: Failed to load database: {e}")