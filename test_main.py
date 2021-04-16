"""
Run the test using the command py.test
"""
import pytest  # noqa
from main import DataPipeline


def test_table_counts():
    """
    Test the overall product count
    """
    number_of_test_run = 2  # Run the pipeline twice
    for i in range(number_of_test_run):
        dp = DataPipeline()
        dp.run()

    dp = DataPipeline()
    assert dp.get_product_count() == (500000,)
    assert dp.get_duplicate_count(from_table="products") == (0,)
    dp.close()
