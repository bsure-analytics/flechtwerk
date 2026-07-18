"""Shared pytest fixtures."""
import pytest

from flechtwerk.keyring import _override_secret_runtime, _restore_secret_runtime


@pytest.fixture(autouse=True)
def _clean_secret_runtime():
    """Isolate the process-global secret runtime (keyring + observer) per test.

    The secrets feature installs into module-global state; this saves it before
    each test, resets to empty, and restores it after — so an install in one
    test cannot leak into another. A no-op for tests that never touch secrets.
    """
    previous = _override_secret_runtime(None, None)
    yield
    _restore_secret_runtime(previous)
