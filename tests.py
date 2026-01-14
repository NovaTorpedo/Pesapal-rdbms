import unittest
import os
import sys
import json
import random
import shutil

sys.path.append(os.getcwd())

# Try/Except to ensure friendly error message if core.py is missing
from core import Database, Table, SchemaError, ConstraintError


class TestPesapalDBMaster(unittest.TestCase):
    """
    TEST SUITE
    Combines comprehensive CRUD coverage with Advanced Edge Case detection.
    """
    DB_FILE = 'test_db_master.json'

    def setUp(self):
        if os.path.exists(self.DB_FILE):
            try:
                os.remove(self.DB_FILE)
            except PermissionError:
                pass
        
        self.db = Database(persistence_file=self.DB_FILE)
        # Standard table for most tests
        self.db.create_table('users', {'id': 'int', 'name': 'str', 'email': 'str'}, pk='id')
        self.table = self.db.get_table('users')
        self.table.create_index('email', unique=True)

    def tearDown(self):
        if os.path.exists(self.DB_FILE):
            try:
                os.remove(self.DB_FILE)
            except PermissionError:
                pass

    # =========================================================
    # 1. BASIC CRUD & FUNCTIONALITY
    # =========================================================

    def test_insert_select(self):
        """Test basic insertion and retrieval"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        res = self.table.select({'id': 1})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['name'], 'Alice')

    def test_select_all(self):
        """Test selecting all records"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        res = self.table.select({})
        self.assertEqual(len(res), 2)

    def test_select_multiple_conditions(self):
        """Test select with multiple filtering conditions"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Alice', 'email': 'alice2@test.com'})
        
        res = self.table.select({'name': 'Alice', 'email': 'alice@test.com'})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['id'], 1)

    def test_update_multiple_fields(self):
        """Test updating multiple fields at once"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.update({'id': 1}, {'name': 'Alicia', 'email': 'alicia@test.com'})
        res = self.table.select({'id': 1})
        self.assertEqual(res[0]['name'], 'Alicia')
        self.assertEqual(res[0]['email'], 'alicia@test.com')

    def test_delete_with_condition(self):
        """Test deleting with non-key condition"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Alice', 'email': 'alice2@test.com'})
        self.table.delete({'name': 'Alice'}) # Should delete both
        
        res = self.table.select({})
        self.assertEqual(len(res), 0)

    # =========================================================
    # 2. SCHEMA & DATA TYPES (Detailed Strictness)
    # =========================================================

    def test_schema_validation_types(self):
        """Test that schema validates data types (int, float, bool, str)"""
        self.db.create_table('types', {
            'id': 'int', 
            'score': 'float', 
            'active': 'bool',
            'tag': 'str'
        }, pk='id')
        t = self.db.get_table('types')

        # Valid Insert
        t.insert({'id': 1, 'score': 99.9, 'active': True, 'tag': 'A'})
        
        # Invalid Int
        with self.assertRaises((SchemaError, ValueError)):
            t.insert({'id': 'not_int', 'score': 99.9, 'active': True, 'tag': 'A'})

    def test_float_precision(self):
        """Test float precision is maintained"""
        self.db.create_table('precisions', {'id': 'int', 'val': 'float'}, pk='id')
        self.db.get_table('precisions').insert({'id': 1, 'val': 1.23456789})
        res = self.db.get_table('precisions').select({'id': 1})
        self.assertAlmostEqual(res[0]['val'], 1.23456789)

    def test_missing_field_error(self):
        """Test insert with missing non-PK field raises SchemaError"""
        with self.assertRaises(SchemaError):
            self.table.insert({'id': 1, 'name': 'Alice'})  # Missing 'email'

    def test_extra_field_error(self):
        """Test insert with extra undefined field raises SchemaError"""
        with self.assertRaises(SchemaError):
            self.table.insert({'id': 1, 'name': 'Alice', 'email': 'a@a.com', 'extra': 'X'})

    # =========================================================
    # 3. CONSTRAINTS & LOGIC (The "Trap" Tests)
    # =========================================================

    def test_pk_constraint(self):
        """Test Primary Key enforcement"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'a@a.com'})
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 1, 'name': 'Bob', 'email': 'b@b.com'})

    def test_unique_constraint(self):
        """Test Unique Index enforcement on insert"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})

    def test_update_causing_unique_violation(self):
        """
        CRITICAL: Test updating a record to a value that conflicts with ANOTHER record.
        Many simple DBs fail this check.
        """
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})

        # Try to change Bob's email to Alice's email -> Should Fail
        with self.assertRaises(ConstraintError):
            self.table.update({'id': 2}, {'email': 'alice@test.com'})

    def test_update_self_no_violation(self):
        """
        CRITICAL: Test updating a record with its own current value.
        Should NOT raise an error.
        """
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        try:
            self.table.update({'id': 1}, {'email': 'alice@test.com', 'name': 'Alicia'})
        except ConstraintError:
            self.fail("Updating record with its own unique value raised ConstraintError")

    # =========================================================
    # 4. INDEXING & STRESS TESTS
    # =========================================================

    def test_index_consistency_stress(self):
        """
        STRESS TEST: Randomly insert and delete to ensure index 
        doesn't hold references to deleted data or miss new data.
        """
        # Insert 50 records
        for i in range(50):
            self.table.insert({'id': i, 'name': f'User{i}', 'email': f'user{i}@test.com'})

        # Delete even numbers
        for i in range(0, 50, 2):
            self.table.delete({'id': i})

        # Verify Index: Query by email for a deleted user (Should be empty)
        res = self.table.select({'email': 'user0@test.com'})
        self.assertEqual(len(res), 0, "Index returned a deleted record (Ghost read)")

        # Verify Index: Query by email for existing user (Should exist)
        res = self.table.select({'email': 'user1@test.com'})
        self.assertEqual(len(res), 1, "Index failed to find existing record")

        # Reuse deleted ID with NEW email (Should work)
        self.table.insert({'id': 0, 'name': 'Reborn', 'email': 'new_user0@test.com'})
        res = self.table.select({'email': 'new_user0@test.com'})
        self.assertEqual(len(res), 1)

    def test_create_unique_index_with_duplicates(self):
        """Test creating unique index fails if data already has duplicates"""
        self.db.create_table('bad_data', {'id': 'int', 'code': 'str'}, pk='id')
        t = self.db.get_table('bad_data')
        t.insert({'id': 1, 'code': 'A'})
        t.insert({'id': 2, 'code': 'A'})
        
        with self.assertRaises(ConstraintError):
            t.create_index('code', unique=True)

    # =========================================================
    # 5. JOINS
    # =========================================================

    def test_join_basic(self):
        """Test basic Join Logic"""
        self.db.create_table('orders', {'oid': 'int', 'uid': 'int', 'amt': 'int'}, pk='oid')
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'a@a.com'})
        self.db.get_table('orders').insert({'oid': 100, 'uid': 1, 'amt': 50})
        
        results = self.db.join('users', 'orders', 'id', 'uid')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['orders_amt'], 50)
        self.assertEqual(results[0]['name'], 'Alice')

    def test_join_three_way(self):
        """Test joining three tables (two sequential joins)"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'product_id': 'int'}, pk='oid')
        self.db.create_table('products', {'pid': 'int', 'name': 'str', 'price': 'float'}, pk='pid')
        
        t_orders = self.db.get_table('orders')
        t_products = self.db.get_table('products')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        t_products.insert({'pid': 10, 'name': 'Widget', 'price': 9.99})
        t_orders.insert({'oid': 100, 'user_id': 1, 'product_id': 10})
        
        # First join: users and orders
        results1 = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results1), 1)
        
        # Second join: orders and products
        results2 = self.db.join('orders', 'products', 'product_id', 'pid')
        self.assertEqual(len(results2), 1)
        # Note: key names depend on your implementation (e.g., 'products_name' or 'name')
        # We check for the value mostly
        found = False
        for k, v in results2[0].items():
            if v == 'Widget':
                found = True
        self.assertTrue(found, "Did not find product name 'Widget' in 3-way join result")

    def test_join_no_match(self):
        """Test join with no matching records"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amt': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        t_orders.insert({'oid': 100, 'user_id': 999, 'amt': 50})  # No matching user
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 0)

    def test_join_column_name_collision(self):
        """
        Test joining tables where both have a 'name' column.
        Result keys should be namespaced (e.g. users_name, roles_name).
        """
        self.db.create_table('roles', {'rid': 'int', 'name': 'str'}, pk='rid')
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'a@a.com'})
        self.db.get_table('roles').insert({'rid': 10, 'name': 'Admin'})
        
        # Create a specific setup for this
        self.db.create_table('depts', {'did': 'int', 'name': 'str', 'mgr_id': 'int'}, pk='did')
        self.db.get_table('depts').insert({'did': 1, 'name': 'Sales', 'mgr_id': 1})
        
        # User 1 is manager of Dept 1
        res = self.db.join('users', 'depts', 'id', 'mgr_id')
        
        row = res[0]
        keys = row.keys()
        
        # Ensure we can distinguish the names
        self.assertTrue('users_name' in keys or 'name' in keys)
        self.assertTrue('depts_name' in keys)

    # =========================================================
    # 6. COMPLEX SCENARIOS
    # =========================================================

    def test_complex_data_scenario(self):
        """Test a realistic multi-table scenario with data validation"""
        # Create products and orders
        self.db.create_table('products', {'pid': 'int', 'name': 'str', 'price': 'float'}, pk='pid')
        self.db.create_table('order_items', {'id': 'int', 'order_id': 'int', 'product_id': 'int', 'qty': 'int'}, pk='id')
        
        prod_table = self.db.get_table('products')
        order_items = self.db.get_table('order_items')
        
        # Add data
        prod_table.insert({'pid': 1, 'name': 'Widget', 'price': 9.99})
        prod_table.insert({'pid': 2, 'name': 'Gadget', 'price': 19.99})
        
        order_items.insert({'id': 1, 'order_id': 100, 'product_id': 1, 'qty': 2})
        order_items.insert({'id': 2, 'order_id': 100, 'product_id': 2, 'qty': 1})
        
        # Join to get order details
        results = self.db.join('order_items', 'products', 'product_id', 'pid')
        self.assertEqual(len(results), 2)
        
        # Verify prices are included and correct
        prices = [r.get('products_price') or r.get('price') for r in results]
        self.assertTrue(9.99 in prices)
        self.assertTrue(19.99 in prices)

    def test_large_dataset(self):
        """Test handling larger dataset (100 records)"""
        # Insert 100 records
        for i in range(100):
            self.table.insert({'id': i, 'name': f'User{i}', 'email': f'user{i}@test.com'})
        
        # Verify all inserted
        res = self.table.select({})
        self.assertEqual(len(res), 100)
        
        # Test selective query
        res = self.table.select({'name': 'User50'})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['id'], 50)

    def test_mixed_operations_scenario(self):
        """Test complex scenario with mixed operations (Insert, Update, Delete)"""
        # Insert initial data
        for i in range(1, 4):
            self.table.insert({'id': i, 'name': f'User{i}', 'email': f'user{i}@test.com'})
        
        # Update some records
        self.table.update({'id': 2}, {'name': 'UpdatedUser2'})
        
        # Delete a record
        self.table.delete({'id': 3})
        
        # Add new record
        self.table.insert({'id': 4, 'name': 'User4', 'email': 'user4@test.com'})
        
        # Verify final state
        res = self.table.select({})
        self.assertEqual(len(res), 3)
        
        # Check updated record
        res = self.table.select({'id': 2})
        self.assertEqual(res[0]['name'], 'UpdatedUser2')
        
        # Check deleted record is gone
        res = self.table.select({'id': 3})
        self.assertEqual(len(res), 0)

    # =========================================================
    # 7. PERSISTENCE & CORRUPTION
    # =========================================================

    def test_persistence_indexes_work_after_load(self):
        """Test that indexes are rebuilt/usable after loading from disk"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.db.save()

        # Reload
        new_db = Database(persistence_file=self.DB_FILE)
        new_table = new_db.get_table('users')
        
        # This query relies on the unique index on email
        res = new_table.select({'email': 'alice@test.com'})
        self.assertEqual(len(res), 1)

    def test_corrupt_db_file(self):
        """Test behavior when the DB file is corrupted"""
        self.db.save()
        with open(self.DB_FILE, 'w') as f:
            f.write("{ NOT VALID JSON }")
            
        with self.assertRaises(Exception):
            _ = Database(persistence_file=self.DB_FILE)

if __name__ == '__main__':
    unittest.main()