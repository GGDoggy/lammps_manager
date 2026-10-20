"""Configuration, batches and authenticated manager / worker commands."""

import argparse
import json
from pathlib import Path
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
            "Authenticated polling workers support shared-storage execution."
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
    for name in ('submit', 'run-local', 'status', 'inspect', 'cancel', 'retry'):
        command = commands.add_parser(name, help=f'{name} jobs')
        command.add_argument('--config', required=True, help='Manager YAML configuration')
        if name == 'submit':
            command.add_argument('manifest', help='JSON list of source_directory / input_file jobs')
        elif name == 'run-local':
            command.add_argument('--worker-config', required=True)
        elif name in ('inspect', 'cancel', 'retry'):
            command.add_argument('job_id')
    manager = commands.add_parser('serve', help='Run the authenticated manager API')
    manager.add_argument('--config', required=True)
    manager.add_argument('--credentials', required=True, help='JSON worker ID to private token-file path mapping')
    manager.add_argument('--host', default='127.0.0.1')
    manager.add_argument('--port', type=int, default=8443)
    manager.add_argument('--tls-cert')
    manager.add_argument('--tls-key')
    worker_command = commands.add_parser('worker', help='Run a persistent polling worker')
    worker_command.add_argument('--config', required=True, help='Worker YAML configuration')
    worker_command.add_argument('--state-directory', required=True, help='Durable local worker journal directory')
    for name in ('pause', 'resume'):
        control = commands.add_parser(name, help=f'{name} new execution on one worker')
        control.add_argument('--config', required=True)
        control.add_argument('worker_id')
    args = parser.parse_args(argv)
    if args.command in ('serve', 'worker', 'pause', 'resume'):
        from .manager import ManagerService, make_server
        from .worker import Worker
        try:
            if args.command == 'worker':
                config = load_config(args.config, 'worker', check_local_paths=True)
                instance = Worker(config, args.state_directory)
                try:
                    instance.run()
                finally:
                    instance.close()
            else:
                config = load_config(args.config, 'manager', check_local_paths=True)
                if args.command in ('pause', 'resume'):
                    if args.worker_id not in {w.id for w in config.workers}:
                        raise ValueError('Unregistered worker')
                    with Store(config.manager.database_path) as store:
                        store.set_worker(args.worker_id, paused=args.command == 'pause')
                    print(f'{args.worker_id}: {args.command}')
                else:
                    credentials = json.loads(Path(args.credentials).read_text())
                    service = ManagerService(config, credentials)
                    try:
                        server = make_server(service, args.host, args.port,
                                             certfile=args.tls_cert, keyfile=args.tls_key)
                        try:
                            print(f'Manager listening on {args.host}:{server.server_port}', flush=True)
                            server.serve_forever()
                        finally:
                            server.server_close()
                    finally:
                        service.close()
        except KeyboardInterrupt:
            return 130
        except (ValueError, OSError, sqlite3.Error) as exc:
            parser.error(str(exc))
        return 0

    if args.command == "validate-config":
        try:
            load_config(args.path, args.kind, check_local_paths=args.check_local_paths)
        except ConfigurationError as exc:
            parser.error(str(exc))
        print("Configuration valid (" + ("local paths checked" if args.check_local_paths
              else "structure only; deployment paths not checked") + ").")
        return 0
    if args.command in ('submit', 'run-local', 'status', 'inspect', 'cancel', 'retry'):
        try:
            config = load_config(args.config, 'manager')
            with Store(config.manager.database_path) as store:
                if args.command == 'submit':
                    result = submit_batch(store, config, args.manifest)
                elif args.command == 'run-local':
                    worker = load_config(args.worker_config, 'worker')
                    result = run_local(store, worker)
                elif args.command == 'cancel':
                    attempts = store.job_attempts(args.job_id)
                    if not attempts:
                        raise ValueError('Cancellation requires an assigned job')
                    store.request_cancel(attempts[-1].attempt_id)
                    result = inspect_job(store, args.job_id)
                elif args.command == 'retry':
                    store.retry(args.job_id)
                    result = inspect_job(store, args.job_id)
                elif args.command == 'status':
                    result = [{**job, 'result_collection':
                               (_collection_summary(inspect_job(store, job['job_id'])['attempts'][-1]['result_collection'])
                                if store.job_attempts(job['job_id']) else None)} for job in store.jobs()]
                else:
                    result = inspect_job(store, args.job_id)
            print(json.dumps(result, indent=2))
        except (ValueError, OSError, sqlite3.Error) as exc:
            parser.error(str(exc))
        return 0
    parser.print_help()
    return 0


def _collection_summary(collection: dict | None) -> dict | None:
    return {key: collection.get(key) for key in ('status', 'reason')} if collection else None
