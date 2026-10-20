"""Minimal CLI entry point; simulation commands will be added incrementally."""

import argparse
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

from .config import ConfigurationError, load_config


def main(argv: Sequence[str] | None = None) -> int:
    """Validate configuration, or print help / version information."""
    parser = argparse.ArgumentParser(
        prog="lammps-manager",
        description=(
            "LAMMPS task manager: configuration validation. "
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
    commands = parser.add_subparsers(dest="command")
    validate = commands.add_parser("validate-config", help="Validate manager or worker YAML")
    validate.add_argument("kind", choices=("manager", "worker"))
    validate.add_argument("path")
    validate.add_argument("--check-local-paths", action="store_true",
                          help="Check paths on this host; never contacts remote workers")
    args = parser.parse_args(argv)
    if args.command == "validate-config":
        try:
            load_config(args.path, args.kind, check_local_paths=args.check_local_paths)
        except ConfigurationError as exc:
            parser.error(str(exc))
        print("Configuration valid (" + ("local paths checked" if args.check_local_paths
              else "structure only; deployment paths not checked") + ").")
        return 0
    parser.print_help()
    return 0
