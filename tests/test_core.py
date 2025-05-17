from entsoe_client._core import hello_from_bin


def test_hello_from_bin():
    assert hello_from_bin() == "Hello from entsoe-rs!"
