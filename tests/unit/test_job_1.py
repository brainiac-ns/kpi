import unittest
from src.script_example.job import Job1

class TestJob1(unittest.TestCase):
    def setUp(self):
        print("Beggining of every test")
    
    def test_job_1(self):
        res = Job1()("Message")
        self.assertEqual(res,"Message!")

    def tearDown(self):
        print("End of every test")