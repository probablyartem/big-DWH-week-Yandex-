import yt.wrapper as yt
import pytest
from hello_yt import get_string

class Case:
    def __init__(self, name: str):
        self._name = name

    def __str__(self) -> str:
        return 'test_{}'.format(self._name)


TEST_CASES = [
    Case(name='basic')
]

@pytest.mark.parametrize('test_case', TEST_CASES, ids=str)
def test_hello_world(test_case: Case) -> None:

    string = get_string()

    assert yt.exists(string)
