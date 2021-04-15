"""
Run the test using the command py.test
"""
import pytest  # noqa
from sqlalchemy import create_engine
from main import main, get_product_count, get_duplicate_count



def test_product_count():
    """
    Test the overall product count
    """
    db_string = "postgresql://postgres:password@localhost:5432"
    db = create_engine(db_string)
    main(db)
    assert get_product_count(db) == (500000,)
    assert get_duplicate_count(db) == (0,)


def test_product_count_second_run():
    """
    Test the overall product run again
    """
    db_string = "postgresql://postgres:password@localhost:5432"
    db = create_engine(db_string)
    main(db)
    assert get_product_count(db) == (500000,)
    assert get_duplicate_count(db) == (0,)