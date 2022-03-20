import argparse
from pprint import pprint
import src.notion_extractor as ne
import unittest
import os


class CustomTests(unittest.TestCase):

    def setUp(self):
        """"""
        pass

    def tearDown(self):
        """"""
        pass

    def test_load_to_flink(self):
        from test import load_to_flink
        load_to_flink.transactions_job()


if __name__ == '__main__':
    unittest.main()