# Databricks notebook source
# MAGIC %run ./neudesic-framework-functions

# COMMAND ----------
import neudesic-framework-functions
import unittest

class TestNeudesicFrameworkFunctions(unittest.TestCase):

    def test_sql_connection(self):
        scopeType = 'Admin'
        conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
        conn.autocommit = True
        cursor = conn.cursor()
        rows = cursor.execute('SELECT 1')
#         rows = cursor.execute('SELECT 1 UNION SELECT 2')
        row = [row for row in rows]
        self.assertEqual("[(1, )]", str(row), "Value returned from the sql connection is incorrect.")
        
    def test_exec_sp_no_results(self):
        scopeType = 'Admin'
        conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
        conn.autocommit = True
        cursor = conn.cursor()  
        try:
            cursor.execute("CREATE TABLE tbl_test_exec_sp_no_results (test_value int)")
            cursor.execute("CREATE PROCEDURE usp_test_exec_sp_no_results AS BEGIN INSERT INTO tbl_test_exec_sp_no_results VALUES (18855); END")
            results = execute_framework_stored_procedure_no_results("EXEC usp_test_exec_sp_no_results;", scopeType)
            self.assertEqual(results, None, "Value returned from execute_framework_stored_no_results")
            rows = cursor.execute("SELECT * FROM tbl_test_exec_sp_no_results")
            row = [row for row in rows]
            self.assertEqual(18855, row[0][0])
        except Exception as e:
            raise(e)
        finally:
            cursor.execute("DROP TABLE tbl_test_exec_sp_no_results")
            cursor.execute("DROP PROCEDURE usp_test_exec_sp_no_results")

    def test_exec_sp_with_results(self):
        scopeType = 'Admin'
        conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
        conn.autocommit = True
        cursor = conn.cursor()  
        try:
            cursor.execute("CREATE PROCEDURE usp_test_exec_sp_with_results AS BEGIN SELECT 18855; END")
            results = execute_framework_stored_procedure_with_results("EXEC usp_test_exec_sp_with_results;", scopeType)
            self.assertEqual(results[0][0], 18855, "Incorrect value returned from execute_framework_stored_with_results")
        except Exception as e:
            raise(e)
        finally:
            cursor.execute("DROP PROCEDURE usp_test_exec_sp_with_results")            
            
suite = unittest.TestLoader().loadTestsFromTestCase(TestNeudesicFrameworkFunctions)
runner = unittest.TextTestRunner(verbosity=100)
results = runner.run(suite)
print(results)
