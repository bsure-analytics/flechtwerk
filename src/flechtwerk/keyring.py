"""The process-global secret runtime, and the joserfc-free seam onto the `Keyring` value object.

The `Keyring` value object — pure key material — and its errors live in the
`flechtctl` package, which `flechtwerk[secrets]` installs; this module
re-exports them, so `flechtwerk.keyring.Keyring is flechtctl.Keyring`. What
stays here is the runtime: the one installed keyring and the context-scoped
secret observer. The module holds **no cryptography and imports no joserfc** —
it is the seam `module.py` imports at decoration time to annotate its
`keyring` slot (the `MqttBrokerConfig` precedent, for the same
reactor-di-resolves-annotations-at-decoration reason). The actual encryption
lives in `flechtwerk.secrets`, which reads the process-global installed here.

The keyring is a **process-global** installed once at startup: attributes are
module-level constants, so their `ENCRYPTED` codecs cannot carry key material
at declaration time and must resolve it lazily at encode/decode time. This is
process-global mutable state — the honest price of module-level `Attribute`
constants — not per-instance state like `Stage.configs`. `install_keyring` is
idempotent for byte-identical material and raises on a conflicting install, so
two embedded modules with different keyrings fail loudly rather than
silently last-writer-wins into wrong-key encryption.

A plain `flechtwerk` install has no `flechtctl`: the key-material names are then
placeholders, so the `keyring` annotation still resolves, and building a keyring
raises a `ModuleNotFoundError` that says to install `flechtwerk[secrets]`.
"""
import logging
from contextvars import ContextVar, Token

from .observer import Observer

try:
    from flechtctl.keyring import KEY_BYTES, Ed25519Key, Keyring, KeyringError, UnknownKeyError, X25519Key
except ModuleNotFoundError as e:
    if e.name is None or e.name.partition(".")[0] != "flechtctl":
        raise

    # flechtwerk without the [secrets] extra. reactor-di resolves module.py's
    # `keyring: lookup[Keyring | None]` annotation at decoration time, so the
    # names must exist; a keyring can only be built with flechtctl installed.
    KEY_BYTES = 32

    class KeyringError(Exception):  # type: ignore[no-redef]
        """Base class for keyring problems (construction, install, key lookup)."""

    class UnknownKeyError(KeyringError):  # type: ignore[no-redef]
        """A token names a `kid` absent from the active keyring."""

        def __init__(self, kid: str) -> None:
            super().__init__(f"no key {kid!r} in the active keyring")
            self.kid = kid

    def _needs_flechtctl(name: str) -> ModuleNotFoundError:
        return ModuleNotFoundError(
            f"{name} needs the flechtctl package — install flechtwerk[secrets]", name="flechtctl",
        )

    class _Placeholder:
        """Stands in for a flechtctl key-material class; every way of building one raises."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            raise _needs_flechtctl(type(self).__name__)

        @classmethod
        def from_json(cls, text: str) -> "_Placeholder":
            raise _needs_flechtctl(cls.__name__)

        @classmethod
        def of(cls, *args: object, **kwargs: object) -> "_Placeholder":
            raise _needs_flechtctl(cls.__name__)

    class Ed25519Key(_Placeholder):  # type: ignore[no-redef]
        pass

    class Keyring(_Placeholder):  # type: ignore[no-redef]
        pass

    class X25519Key(_Placeholder):  # type: ignore[no-redef]
        pass

log = logging.getLogger(__name__)

__all__ = [
    "Ed25519Key",
    "Keyring",
    "KeyringConflictError",
    "KeyringError",
    "KeyringNotInstalledError",
    "UnknownKeyError",
    "X25519Key",
    "active_keyring",
    "active_observer",
    "install_keyring",
    "set_secret_observer",
]


class KeyringConflictError(KeyringError):
    """A second, different keyring was installed in a process that already has one."""


class KeyringNotInstalledError(KeyringError):
    """An `ENCRYPTED` codec ran with no keyring installed in this process."""


# --- secret runtime: process-global keyring, context-scoped observer ---

_keyring: Keyring | None = None
_observer: ContextVar[Observer] = ContextVar("flechtwerk_secret_observer", default=Observer())


def install_keyring(keyring: Keyring) -> None:
    """Install THE process keyring. Idempotent for identical material; else raises.

    Called by `Flechtwerk.of(keyring=...)` and directly by ops tooling that
    encrypts without running a stage. A second install of byte-identical
    material is a no-op; a second install of *different* material raises
    `KeyringConflictError`.
    """
    global _keyring
    if _keyring is not None and _keyring != keyring:
        raise KeyringConflictError(
            "a different keyring is already installed in this process — one keyring per process (v1)"
        )
    if _keyring is None:
        log.info("Installed keyring: %d key(s), primary %r", len(keyring.kids()), keyring.primary)
    _keyring = keyring


def current_keyring() -> Keyring | None:
    """The installed keyring, or None — the non-raising query.

    Internal (not in `__all__`): no production caller yet, so it is not an API
    commitment. Used by the test fixtures to observe install state.
    """
    return _keyring


def active_keyring() -> Keyring:
    """The installed keyring, or raise `KeyringNotInstalledError`.

    The codec resolves the keyring through this on every encode/decode: an
    `ENCRYPTED` attribute with no keyring installed is a deployment error to
    surface, never a plaintext pass-through.
    """
    if _keyring is None:
        raise KeyringNotInstalledError(
            "no keyring installed — call flechtwerk.secrets.install_keyring(...) "
            "or pass keyring= to Flechtwerk.of(...)"
        )
    return _keyring


def set_secret_observer(observer: Observer) -> Token[Observer]:
    """Bind the observer the codec emits secret events through — in THIS context.

    A stage binds its own observer in `__aenter__` and resets it with the
    returned token in `__aexit__` (`token.var.reset(token)`, same task). The
    binding lives in the asyncio context, and every task the runner spawns
    afterwards — poll cycles, gathered buckets, token tasks, the paho socket
    callbacks — inherits a copy, so a `secret_decrypted` /
    `secret_plaintext_read` fired deep in a lazy `ConfigStore.get()`, where
    no observer is in scope, reaches THIS stage's observer and carries its
    `metrics_labels`. Several stages in one process are several tasks with
    several contexts, so each stage's secret metrics are its own — nothing is
    shared, nothing is first-wins. (Fanning one event out to every stage's
    observer would count each decrypt once per stage.) Outside any stage —
    ops tooling that only encrypts — the default is the no-op `Observer`.

    Unlike the keyring, which stays process-global: co-hosted stages reading
    one config topic must agree on key material anyway.
    """
    return _observer.set(observer)


def active_observer() -> Observer:
    """The secret observer bound in the current context (no-op `Observer` by default)."""
    return _observer.get()


def _override_secret_runtime(
    keyring: Keyring | None, observer: Observer | None,
) -> tuple[Keyring | None, Token[Observer]]:
    """Test-only: force the runtime and return the previous state for restore.

    Not public API — the sanctioned test entry point is
    `flechtwerk.testing.installed_keyring`, which pairs this with
    `_restore_secret_runtime` so suites cannot leak keyrings across tests.
    The observer half is a context binding, so both halves must run in the
    same context (a fixture's setup and teardown do).
    """
    global _keyring
    previous = (_keyring, _observer.set(observer if observer is not None else Observer()))
    _keyring = keyring
    return previous


def _restore_secret_runtime(previous: tuple[Keyring | None, Token[Observer]]) -> None:
    """Test-only: restore a runtime snapshot taken by `_override_secret_runtime`."""
    global _keyring
    _keyring, token = previous
    _observer.reset(token)
