import sys
import unittest
import pathlib
sys.path.append(pathlib.Path(__file__).parent.absolute())

from pyspark_store import format_schema_to_string
from pyspark_store import get_partition_type

class TestSchema(unittest.TestCase):

    def test_format_schema_to_string(self):
        data = {"a":"int","b":"string", "c":"date"}
        expected = "a int, c date"
        result = format_schema_to_string(data, "b")
        self.assertEqual(expected, result)

    def test_get_partition_type(self):
        data = {"a":"int","b":"string", "c":"date"}
        expected = "c date"
        result = get_partition_type(data, "c")
        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()