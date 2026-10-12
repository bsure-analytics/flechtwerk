import ast
import subprocess
import sys
from importlib.metadata import requires
from pathlib import Path

SRC = Path(__file__).parent.parent / "src" / "flechtctl"
TESTS = Path(__file__).parent


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {
        alias.name.partition(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {
        node.module.partition(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
    }


def run(code: str) -> str:
    return subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, check=True).stdout.strip()


def test_importing_flechtctl_does_not_load_joserfc_but_using_it_does():
    out = run(
        "import sys, flechtctl\n"
        "before = 'joserfc' in sys.modules\n"
        "flechtctl.encrypt\n"
        "print(before, 'joserfc' in sys.modules)"
    )
    assert out == "False True"


def test_the_source_imports_no_flechtwerk_no_cryptography_and_joserfc_only_in_tokens():
    for path in SRC.glob("*.py"):
        modules = imported_modules(path)
        assert "flechtwerk" not in modules, path
        assert "cryptography" not in modules, path
        assert ("joserfc" in modules) == (path.name == "tokens.py"), path


def test_the_tests_import_no_flechtwerk():
    for path in TESTS.glob("*.py"):
        assert "flechtwerk" not in imported_modules(path), path


def test_the_only_requirement_is_joserfc():
    assert requires("flechtctl") == ["joserfc~=1.7"]


def test_every_module_declares_all_alphabetically():
    for path in SRC.glob("*.py"):
        if path.name == "__main__.py":
            continue
        tree = ast.parse(path.read_text())
        declared = next(
            node.value for node in tree.body
            if isinstance(node, ast.Assign) and node.targets[0].id == "__all__"  # type: ignore[attr-defined]
        )
        names = [element.value for element in declared.elts]  # type: ignore[attr-defined]
        assert names == sorted(names), path
