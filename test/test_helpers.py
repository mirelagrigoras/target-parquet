import pytest

from target_parquet.helpers import flatten


def test_flatten():
    in_dict = {
        "key_1": 1,
        "key_2": {"key_3": 2, "key_4": {"key_5": 3, "key_6": ["10", "11"]}},
    }
    expected = {
        "key_1": 1,
        "key_2__key_3": 2,
        "key_2__key_4__key_5": 3,
        "key_2__key_4__key_6": "['10', '11']",
    }

    output = flatten(in_dict)
    assert output == expected
