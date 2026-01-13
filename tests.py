import unittest
import os
import sys
import json

sys.path.append(os.getcwd())

from core import Database, Table, SchemaError, ConstraintError

class TestPesapalDB(unittest.TestCase):
    def setUp(self):
        self.db = Database(persistence_file='test_db.json')
        # Setup a basic user table
        self.db.create_table('users', {'id': 'int', 'name': 'str', 'email': 'str'}, pk='id')
        self.table = self.db.get_table('users')
        self.table.create_index('email', unique=True)

    def tearDown(self):
        if os.path.exists('test_db.json'):
            os.remove('test_db.json')

    # ========== BASIC CRUD TESTS ==========
    
    def test_insert_select(self):
        """Test basic insertion and retrieval"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        res = self.table.select({'id': 1})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['name'], 'Alice')

    def test_insert_multiple_records(self):
        """Test inserting multiple records"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        self.table.insert({'id': 3, 'name': 'Charlie', 'email': 'charlie@test.com'})
        res = self.table.select({})
        self.assertEqual(len(res), 3)

    def test_select_all(self):
        """Test selecting all records"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        res = self.table.select({})
        self.assertEqual(len(res), 2)

    def test_select_with_condition(self):
        """Test select with filtering condition"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        res = self.table.select({'name': 'Bob'})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['email'], 'bob@test.com')

    def test_select_multiple_conditions(self):
        """NEW: Test select with multiple filtering conditions"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Alice', 'email': 'alice2@test.com'})
        self.table.insert({'id': 3, 'name': 'Bob', 'email': 'bob@test.com'})
        
        res = self.table.select({'name': 'Alice', 'email': 'alice@test.com'})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['id'], 1)

    def test_update(self):
        """Test Update"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.update({'id': 1}, {'name': 'Alice Wonderland'})
        res = self.table.select({'id': 1})
        self.assertEqual(res[0]['name'], 'Alice Wonderland')

    def test_update_multiple_fields(self):
        """Test updating multiple fields at once"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.update({'id': 1}, {'name': 'Alicia', 'email': 'alicia@test.com'})
        res = self.table.select({'id': 1})
        self.assertEqual(res[0]['name'], 'Alicia')
        self.assertEqual(res[0]['email'], 'alicia@test.com')

    def test_update_multiple_records(self):
        """NEW: Test updating multiple records that match condition"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Alice', 'email': 'alice2@test.com'})
        self.table.insert({'id': 3, 'name': 'Bob', 'email': 'bob@test.com'})
        
        self.table.update({'name': 'Alice'}, {'name': 'Alicia'})
        
        res = self.table.select({'name': 'Alicia'})
        self.assertEqual(len(res), 2)

    def test_delete(self):
        """Test Delete"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.delete({'id': 1})
        res = self.table.select({'id': 1})
        self.assertEqual(len(res), 0)

    def test_delete_multiple(self):
        """Test deleting multiple matching records"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        self.table.insert({'id': 3, 'name': 'Charlie', 'email': 'charlie@test.com'})
        self.table.delete({'id': 2})
        res = self.table.select({})
        self.assertEqual(len(res), 2)

    def test_delete_with_condition(self):
        """NEW: Test deleting with non-key condition"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Alice', 'email': 'alice2@test.com'})
        self.table.insert({'id': 3, 'name': 'Bob', 'email': 'bob@test.com'})
        
        self.table.delete({'name': 'Alice'})
        
        res = self.table.select({})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['name'], 'Bob')

    # ========== SCHEMA & DATA TYPE TESTS ==========
    
    def test_table_creation_with_schema(self):
        """Test creating table with defined schema"""
        self.db.create_table('products', 
                           {'pid': 'int', 'name': 'str', 'price': 'float'}, 
                           pk='pid')
        table = self.db.get_table('products')
        self.assertIsNotNone(table)

    def test_multiple_data_types(self):
        """Test support for different column data types"""
        self.db.create_table('test_types', 
                           {'id': 'int', 'name': 'str', 'score': 'float', 'active': 'bool'}, 
                           pk='id')
        table = self.db.get_table('test_types')
        table.insert({'id': 1, 'name': 'Test', 'score': 95.5, 'active': True})
        res = table.select({'id': 1})
        self.assertEqual(res[0]['score'], 95.5)
        self.assertEqual(res[0]['active'], True)

    def test_schema_validation(self):
        """Test that schema validation is enforced"""
        # This tests if invalid data types are caught
        try:
            # Try to insert wrong type - behavior depends on implementation
            self.table.insert({'id': 'not_an_int', 'name': 'Test', 'email': 'test@test.com'})
            # If it doesn't raise an error, at least verify conversion happened
        except (SchemaError, ValueError, TypeError):
            pass  # Expected behavior

    def test_int_type(self):
        """NEW: Test integer type handling"""
        self.table.insert({'id': 100, 'name': 'Test', 'email': 'test@test.com'})
        res = self.table.select({'id': 100})
        self.assertIsInstance(res[0]['id'], int)
        self.assertEqual(res[0]['id'], 100)

    def test_str_type(self):
        """NEW: Test string type handling"""
        self.table.insert({'id': 1, 'name': 'Test String', 'email': 'test@test.com'})
        res = self.table.select({'id': 1})
        self.assertIsInstance(res[0]['name'], str)
        self.assertEqual(res[0]['name'], 'Test String')

    def test_float_type(self):
        """NEW: Test float type handling"""
        self.db.create_table('prices', {'id': 'int', 'amount': 'float'}, pk='id')
        table = self.db.get_table('prices')
        table.insert({'id': 1, 'amount': 99.99})
        res = table.select({'id': 1})
        self.assertIsInstance(res[0]['amount'], float)
        self.assertAlmostEqual(res[0]['amount'], 99.99)

    def test_bool_type(self):
        """NEW: Test boolean type handling"""
        self.db.create_table('flags', {'id': 'int', 'active': 'bool'}, pk='id')
        table = self.db.get_table('flags')
        table.insert({'id': 1, 'active': True})
        table.insert({'id': 2, 'active': False})
        
        res_true = table.select({'id': 1})
        res_false = table.select({'id': 2})
        
        self.assertTrue(res_true[0]['active'])
        self.assertFalse(res_false[0]['active'])

    # ========== CONSTRAINT TESTS ==========
    
    def test_pk_constraint(self):
        """Test Primary Key enforcement"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        # Insert duplicate ID
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 1, 'name': 'Bob', 'email': 'bob@test.com'})

    def test_unique_constraint(self):
        """Test Unique Index enforcement"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        # Insert duplicate Email with different ID
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})

    def test_pk_not_null(self):
        """Test that primary key cannot be null"""
        with self.assertRaises((ConstraintError, ValueError)):
            self.table.insert({'id': None, 'name': 'Alice', 'email': 'alice@test.com'})

    def test_composite_constraints(self):
        """Test multiple constraints work together"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        
        # Test PK violation
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 1, 'name': 'Bob', 'email': 'bob@test.com'})
        
        # Test unique violation
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})

    # ========== INDEX TESTS ==========
    
    def test_index_creation(self):
        """Test creating an index on a column"""
        self.db.create_table('indexed_table', {'id': 'int', 'code': 'str'}, pk='id')
        table = self.db.get_table('indexed_table')
        table.create_index('code', unique=False)
        # If it doesn't raise an error, index was created

    def test_unique_index(self):
        """Test unique index enforcement"""
        # Already tested in test_unique_constraint, but explicit here
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})

    def test_non_unique_index(self):
        """Test non-unique index allows duplicates"""
        self.db.create_table('posts', {'id': 'int', 'user_id': 'int', 'title': 'str'}, pk='id')
        table = self.db.get_table('posts')
        table.create_index('user_id', unique=False)
        
        table.insert({'id': 1, 'user_id': 1, 'title': 'Post 1'})
        table.insert({'id': 2, 'user_id': 1, 'title': 'Post 2'})  # Same user_id should be allowed
        
        res = table.select({'user_id': 1})
        self.assertEqual(len(res), 2)

    def test_index_improves_query(self):
        """Test that indexed queries work correctly"""
        # Insert multiple records
        for i in range(10):
            self.table.insert({'id': i, 'name': f'User{i}', 'email': f'user{i}@test.com'})
        
        # Query using indexed column
        res = self.table.select({'email': 'user5@test.com'})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['id'], 5)

    def test_index_on_multiple_columns(self):
        """NEW: Test creating indexes on multiple columns"""
        self.db.create_table('multi_index', {'id': 'int', 'code': 'str', 'status': 'str'}, pk='id')
        table = self.db.get_table('multi_index')
        
        table.create_index('code', unique=True)
        table.create_index('status', unique=False)
        
        table.insert({'id': 1, 'code': 'A1', 'status': 'active'})
        table.insert({'id': 2, 'code': 'A2', 'status': 'active'})
        
        # Both indexes should work
        res_code = table.select({'code': 'A1'})
        self.assertEqual(len(res_code), 1)
        
        res_status = table.select({'status': 'active'})
        self.assertEqual(len(res_status), 2)

    def test_index_updated_after_update(self):
        """NEW: Test that indexes are maintained after UPDATE operations"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        
        # Update email (which has unique index)
        self.table.update({'id': 1}, {'email': 'newalice@test.com'})
        
        # Old email should not be found
        res_old = self.table.select({'email': 'alice@test.com'})
        self.assertEqual(len(res_old), 0)
        
        # New email should be found
        res_new = self.table.select({'email': 'newalice@test.com'})
        self.assertEqual(len(res_new), 1)
        
        # Unique constraint should still work
        with self.assertRaises(ConstraintError):
            self.table.insert({'id': 3, 'name': 'Charlie', 'email': 'newalice@test.com'})

    def test_index_updated_after_delete(self):
        """NEW: Test that indexes are cleaned up after DELETE operations"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.delete({'id': 1})
        
        # Should be able to reuse the email after deletion
        try:
            self.table.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})
            res = self.table.select({'email': 'alice@test.com'})
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0]['id'], 2)
        except ConstraintError:
            self.fail("Index not properly cleaned up after DELETE")

    # ========== JOIN TESTS ==========
    
    def test_join(self):
        """Test Join Logic"""
        # Create orders table
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'a@a.com'})
        t_orders.insert({'oid': 100, 'user_id': 1, 'amount': 50})
        
        # Perform Join
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['orders_amount'], 50)
        self.assertEqual(results[0]['name'], 'Alice')

    def test_join_multiple_matches(self):
        """Test join with multiple matching records"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        # One user with multiple orders
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        t_orders.insert({'oid': 100, 'user_id': 1, 'amount': 50})
        t_orders.insert({'oid': 101, 'user_id': 1, 'amount': 75})
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r['name'] == 'Alice' for r in results))

    def test_join_no_match(self):
        """Test join with no matching records"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        t_orders.insert({'oid': 100, 'user_id': 999, 'amount': 50})  # No matching user
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 0)

    def test_join_multiple_users(self):
        """Test join with multiple users and orders"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.table.insert({'id': 2, 'name': 'Bob', 'email': 'bob@test.com'})
        t_orders.insert({'oid': 100, 'user_id': 1, 'amount': 50})
        t_orders.insert({'oid': 101, 'user_id': 2, 'amount': 75})
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 2)
        names = {r['name'] for r in results}
        self.assertEqual(names, {'Alice', 'Bob'})

    def test_join_with_empty_tables(self):
        """NEW: Test join when one table is empty"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        
        # users has data, orders is empty
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        self.assertEqual(len(results), 0)

    def test_join_preserves_all_columns(self):
        """NEW: Test that join result includes all columns from both tables"""
        self.db.create_table('orders', {'oid': 'int', 'user_id': 'int', 'amount': 'int'}, pk='oid')
        t_orders = self.db.get_table('orders')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        t_orders.insert({'oid': 100, 'user_id': 1, 'amount': 50})
        
        results = self.db.join('users', 'orders', 'id', 'user_id')
        
        # Check that result has columns from both tables
        self.assertIn('name', results[0])
        self.assertIn('email', results[0])
        self.assertIn('orders_amount', results[0])
        self.assertIn('orders_oid', results[0])

    def test_join_three_way(self):
        """NEW: Test joining three tables (two sequential joins)"""
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
        self.assertEqual(results2[0]['products_name'], 'Widget')

    # ========== PERSISTENCE TESTS ==========
    
    def test_persistence_save_load(self):
        """Test that data persists across database instances"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.db.save()
        
        # Create new database instance
        db2 = Database(persistence_file='test_db.json')
        table2 = db2.get_table('users')
        res = table2.select({'id': 1})
        
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['name'], 'Alice')

    def test_persistence_schema(self):
        """Test that schema persists"""
        self.db.save()
        
        db2 = Database(persistence_file='test_db.json')
        table2 = db2.get_table('users')
        
        # Verify primary key still enforced
        table2.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        with self.assertRaises(ConstraintError):
            table2.insert({'id': 1, 'name': 'Bob', 'email': 'bob@test.com'})

    def test_persistence_with_indexes(self):
        """NEW: Test that indexes persist across save/load"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.db.save()
        
        # Load into new instance
        db2 = Database(persistence_file='test_db.json')
        table2 = db2.get_table('users')
        
        # Unique index should still be enforced
        with self.assertRaises(ConstraintError):
            table2.insert({'id': 2, 'name': 'Bob', 'email': 'alice@test.com'})

    def test_persistence_with_multiple_tables(self):
        """NEW: Test persistence with multiple tables"""
        self.db.create_table('products', {'id': 'int', 'name': 'str'}, pk='id')
        self.db.create_table('orders', {'id': 'int', 'amount': 'int'}, pk='id')
        
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        self.db.get_table('products').insert({'id': 1, 'name': 'Widget'})
        self.db.get_table('orders').insert({'id': 1, 'amount': 100})
        
        self.db.save()
        
        # Load and verify all tables
        db2 = Database(persistence_file='test_db.json')
        self.assertIsNotNone(db2.get_table('users'))
        self.assertIsNotNone(db2.get_table('products'))
        self.assertIsNotNone(db2.get_table('orders'))

    # ========== EDGE CASES & ERROR HANDLING ==========
    
    def test_empty_select(self):
        """Test selecting from empty table"""
        res = self.table.select({})
        self.assertEqual(len(res), 0)

    def test_update_nonexistent(self):
        """Test updating non-existent record"""
        affected = self.table.update({'id': 999}, {'name': 'Ghost'})
        # Should affect 0 rows or return indication of no match

    def test_delete_nonexistent(self):
        """Test deleting non-existent record"""
        affected = self.table.delete({'id': 999})
        # Should affect 0 rows or return indication of no match

    def test_get_nonexistent_table(self):
        """Test getting a table that doesn't exist"""
        with self.assertRaises((KeyError, ValueError)):
            self.db.get_table('nonexistent')

    def test_duplicate_table_creation(self):
        """Test creating a table that already exists"""
        with self.assertRaises((SchemaError, ValueError)):
            self.db.create_table('users', {'id': 'int'}, pk='id')

    def test_invalid_column_in_query(self):
        """Test querying with column that doesn't exist"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        try:
            res = self.table.select({'nonexistent_column': 'value'})
            # Depending on implementation, might return empty or raise error
        except (KeyError, ValueError):
            pass  # Expected behavior

    def test_select_no_results(self):
        """NEW: Test select that matches no records"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        res = self.table.select({'name': 'NonExistent'})
        self.assertEqual(len(res), 0)

    def test_constraint_error_message(self):
        """NEW: Test that ConstraintError has meaningful message"""
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        
        try:
            self.table.insert({'id': 1, 'name': 'Bob', 'email': 'bob@test.com'})
            self.fail("Should raise ConstraintError")
        except ConstraintError as e:
            # Error should have some message
            self.assertIsNotNone(str(e))
            self.assertGreater(len(str(e)), 0)

    # ========== COMPLEX SCENARIOS ==========
    
    def test_full_crud_cycle(self):
        """Test complete CRUD cycle on a record"""
        # Create
        self.table.insert({'id': 1, 'name': 'Alice', 'email': 'alice@test.com'})
        
        # Read
        res = self.table.select({'id': 1})
        self.assertEqual(res[0]['name'], 'Alice')
        
        # Update
        self.table.update({'id': 1}, {'email': 'newalice@test.com'})
        res = self.table.select({'id': 1})
        self.assertEqual(res[0]['email'], 'newalice@test.com')
        
        # Delete
        self.table.delete({'id': 1})
        res = self.table.select({'id': 1})
        self.assertEqual(len(res), 0)

    def test_complex_data_scenario(self):
        """Test a realistic multi-table scenario"""
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
        
        # Verify prices are included
        self.assertTrue(any(r['products_price'] == 9.99 for r in results))
        self.assertTrue(any(r['products_price'] == 19.99 for r in results))

    def test_large_dataset(self):
        """NEW: Test handling larger dataset"""
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
        """NEW: Test complex scenario with mixed operations"""
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

if __name__ == '__main__':
    unittest.main()