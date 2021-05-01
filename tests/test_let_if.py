from jasyncq.util import let_if


def test_if_value_none():
    assert let_if(None, lambda x: x + 1) is None


def test_if_value_not_none():
    assert let_if(1, lambda x: x + 1) == 2


def test_if_value_not_none_but_falsy():
    assert let_if(0, lambda x: x + 1) == 1
