"""Minimal CLI entry point; simulation commands will be added incrementally."""

import argparse
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version


def main(argv: Sequence[str] | None = None) -> int:
    """Print CLI help or the installed distribution version."""
    parser = argparse.ArgumentParser(
        prog="lammps-manager",
        description=(
            "LAMMPS task manager (repository scaffold). "
            "Simulation dispatch is not implemented yet."
        ),
    )
    try:
        package_version = version("lammps-task-manager")
    except PackageNotFoundError:
        package_version = "development (not installed)"
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {package_version}"
    )
    parser.parse_args(argv)
    parser.print_help()
    return 0
