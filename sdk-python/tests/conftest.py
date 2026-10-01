import pytest
from fixtures.sqlite import close


@pytest.fixture(autouse=True)
def sqlite_databases():
    yield
    close()
