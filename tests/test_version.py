import denki_client


def test_get_version():
    assert isinstance(denki_client.__version__, str)
