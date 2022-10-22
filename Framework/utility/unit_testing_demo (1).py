# Databricks notebook source
# MAGIC %md #Unit Tests
# MAGIC 
# MAGIC Link: [unittest - Unit testing framework documentation](https://docs.python.org/3/library/unittest.html)
# MAGIC 
# MAGIC A unit test is a piece of code that will test the functionality of your solution. 
# MAGIC 
# MAGIC To effectively use unit tests, you need to write your functions in a way that makes it testable. To make code testable:
# MAGIC * the function should only perform a single activity
# MAGIC * it should return a value so that value can be part of the test
# MAGIC 
# MAGIC The value returned could be anything as long as there is something to "assert" or test.

# COMMAND ----------

# MAGIC %md #Sample Function
# MAGIC Normally you would test other notebooks by using the %run command to make that code available in this notebook.
# MAGIC For simplicity, I'm putting the function in here.

# COMMAND ----------

def add_numbers(num1, num2):
    
    return num1 + num2

# COMMAND ----------

# MAGIC %md #Sample Test Case
# MAGIC In the below example, we are using a function from the unit_testing_sample to create a unit test.
# MAGIC 
# MAGIC First, we need to **import unittest** as this is the framework being used. It is built to be similar to JUnit and has everything you need for testing.
# MAGIC 
# MAGIC Next, we **create a class where we implement unittest.TestCase**. According to the Python documentation (linked above), "A test case is the individual unit of testing. It checks for a specific response to a particular set of inputs. unittest provides a base class, TestCase, which may be used to create new test cases."
# MAGIC 
# MAGIC There are two functions that will run before and after running every test within the class: **setUp()** and **tearDown()**. This is where you will create anything needed for tests and then cleanup afterwards.
# MAGIC 
# MAGIC To create a test function within the class, **create a function with a name that starts with test**. unittest will discover any function that begins with test and run it. This means, too, that if you need to have some support functions, you can create them. Just don't start the name of the function with test.

# COMMAND ----------

import unittest
# import logging

#create a class to test
class TestAddNumbers(unittest.TestCase):

        # the test has to begin with the letters "test" or else it won't be discovered and ran
    def test_add_numbers(self):
        num1 = 1
        num2 = 2
        total = add_numbers(num1, num2)

        # assertEqual is used to compare the values that should be equal.
        # First value is the value you expect.
        # Second value is the value you get from your test.
        # Third value is the message to return when the test fails. 
        # If you are using a formatted string then prepare it in a variable before the assert is ran.
        message = f"num1: ({num1}) num2: ({num2}) did not equal 3"
        self.assertEqual(3, total, message)
        
    def test_add_letters(self):
        # This test is to make sure that when an error occurs, it fails in an expected way.
        # If we add a letter to a number, we get the TypeError message seen below.
        # Using a try-except, we can check that the error message thrown is the one expected.
        num1 = 'a'
        num2 = 2
        try:
            total = add_numbers(num1,num2)
        except Exception as e:
            self.assertIn('can only concatenate str (not "int") to str', str(e), "Wrong error message thrown for test_add_letters")

# For databricks, you will need to run using TestLoader.
# You can also use the TestLoader to make a custom load for testing.
suite = unittest.TestLoader().loadTestsFromTestCase(TestAddNumbers)
runner = unittest.TextTestRunner(verbosity=100)
results = runner.run(suite)
print(results)
