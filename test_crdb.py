import pytest
from crdb import query


@pytest.mark.parametrize(
    "args",
    (
        "H",
        "e+",
        "e-",
        ("B", "C"),
    ),
)
def test_query(args):
    if not isinstance(args, tuple):
        args = (args,)

    tab = query(*args)
    assert len(tab) > 1


def test_bad_query_1():
    with pytest.raises(ValueError):
        query("Foobar")


def test_bad_query_2():
    with pytest.raises(ValueError):
        query("H", energy_type="Foobar")
