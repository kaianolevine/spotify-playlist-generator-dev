#!/usr/bin/env python3
"""
Rename the template package from `project_name` to a new name.

Usage:
    python init_project.py my_new_project
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SRC_DIR = ROOT / "src"
TESTS_DIR = ROOT / "tests"
OLD = "project_name"


def replace_in_file(path: Path, old: str, new: str):
    if not path.exists() or not path.is_file():
        return
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return
    text_new = text.replace(old, new)
    if text_new != text:
        path.write_text(text_new, encoding="utf-8")


def main():
    if len(sys.argv) != 2:
        print("Usage: python init_project.py <new_package_name>")
        sys.exit(1)
    new = sys.argv[1].strip()
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", new):
        print("Error: package name must be a valid Python identifier.")
        sys.exit(1)

    # Update pyproject.toml (name and packages include)
    pyproj = ROOT / "pyproject.toml"
    if pyproj.exists():
        txt = pyproj.read_text(encoding="utf-8")
        txt = re.sub(r'(?m)^name\s*=\s*"(.*?)"', f'name = "{new}"', txt)
        txt = txt.replace(f'include = "{OLD}"', f'include = "{new}"')
        pyproj.write_text(txt, encoding="utf-8")

    # Rename src/ and tests/ package directories
    src_old = SRC_DIR / OLD
    src_new = SRC_DIR / new
    if src_old.exists():
        src_old.rename(src_new)

    tests_old = TESTS_DIR / OLD
    tests_new = TESTS_DIR / new
    if tests_old.exists():
        tests_old.rename(tests_new)

    # Update basic imports in tests and src
    for base in [src_new, tests_new]:
        for p in base.rglob("*.py"):
            replace_in_file(p, OLD, new)

    print(f"✅ Renamed package '{OLD}' → '{new}'.")
    print("Next steps:")
    print("  poetry install")
    print("  pre-commit install")
    print("  poetry run pytest")


if __name__ == "__main__":
    main()
