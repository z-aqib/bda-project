#!/usr/bin/env python
# structure-maker.py
"""
Generates a visually beautiful tree view of the current directory (like `tree` command)
and writes it to structure.txt
"""

from pathlib import Path

DEFAULT_IGNORES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    ".env",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
    "dist",
    "build",
    ".DS_Store",
    "node_modules",
    ".ipynb_checkpoints",
}


def is_hidden(p: Path) -> bool:
    return p.name.startswith(".") and p.name not in {
        ".gitignore",
        ".gitattributes",
        ".gitkeep",
        ".github",
    }


def build_tree(directory: Path, prefix: str = "", ignores=None, max_depth=None, depth=0):
    """Recursively builds the tree structure"""
    if ignores is None:
        ignores = set()
    if max_depth is not None and depth > max_depth:
        return []

    entries = sorted(
        [e for e in directory.iterdir() if not is_hidden(e) and e.name not in ignores],
        key=lambda x: (x.is_file(), x.name.lower()),
    )
    lines = []
    for idx, entry in enumerate(entries):
        connector = "└── " if idx == len(entries) - 1 else "├── "
        line = f"{prefix}{connector}{entry.name}"
        lines.append(line)
        if entry.is_dir():
            extension = "    " if idx == len(entries) - 1 else "│   "
            lines.extend(build_tree(entry, prefix + extension, ignores, max_depth, depth + 1))
    return lines


def main():
    root = Path(".").resolve()
    print(f"📁 Generating beautiful tree for: {root}")
    lines = [f"{root.name}/"] + build_tree(root, "", DEFAULT_IGNORES)
    output_path = root / "structure.txt"
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Done! Tree written to: {output_path}")


if __name__ == "__main__":
    main()
