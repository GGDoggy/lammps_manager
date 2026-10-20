"""Configuration validation and explicit local batch commands."""

import argparse
import json
import sqlite3
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

from .config import ConfigurationError, load_config
from .local import submit_batch, run_local, inspect_job
from .store import Store


def main(argv: Sequence[str] | None = None) -> int:
    """Validate configuration, operate local batches, or print help / version."""
    parser = argparse.ArgumentParser(
        prog="lammps-manager",
        description=(
            "LAMMPS task manager: local batches and configuration validation. "
            "Remote simulation dispatch is not implemented yet."
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
    for name in ('submit', 'run-local', 'status', 'inspect'):
        command = commands.add_parser(name, help=f'{name} local jobs')
        command.add_argument('--config', required=True, help='Manager YAML configuration')
        if name == 'submit':
            command.add_argument('manifest', help='JSON list of source_directory / input_file jobs')
        elif name == 'run-local':
            command.add_argument('--worker-config', required=True)
        elif name == 'inspect':
            command.add_argument('job_id')
    args = parser.parse_args(argv)
    if args.command == "validate-config":
        try:
            load_config(args.path, args.kind, check_local_paths=args.check_local_paths)
        except ConfigurationError as exc:
            parser.error(str(exc))
        print("Configuration valid (" + ("local paths checked" if args.check_local_paths
              else "structure only; deployment paths not checked") + ").")
        return 0
    if args.command in ('submit', 'run-local', 'status', 'inspect'):
        try:
            config = load_config(args.config, 'manager')
            with Store(config.manager.database_path) as store:
                if args.command == 'submit':
                    result = submit_batch(store, config, args.manifest)
                elif args.command == 'run-local':
                    worker = load_config(args.worker_config, 'worker')
                    result = run_local(store, worker)
                elif args.command == 'status':
                    result = store.jobs()
                else:
                    result = inspect_job(store, args.job_id)
            print(json.dumps(result, indent=2))
        except (ValueError, OSError, sqlite3.Error) as exc:
            parser.error(str(exc))
        return 0
    parser.print_help()
    return 0
