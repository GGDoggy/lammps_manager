"""Offline boundary evidence with LAMMPS-style accumulated binary64 time."""
from dataclasses import asdict, replace
import json
import math
from pathlib import Path
import tempfile
import unittest

from lammps_manager import checkpoints as cp
from lammps_manager.abp_parameters import PARAMETERIZED_VERSION, model_summary, parameterized_digest
from lammps_manager.continuation import (ENGINEERING_VERSION, MAX_RUN_COMMAND, Recipe,
    engineering_parameters, engineering_digest)
from test_checkpoints import BUILD, provenance


class CheckpointClockTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def contract(self, ends, *, version=ENGINEERING_VERSION, parameters=None, ordinal=None):
        recipe = Recipe('wca-abp-2d-v1', 'statistical')
        model = (model_summary(recipe, parameters or {}) if version == PARAMETERIZED_VERSION
                 else engineering_parameters(recipe))
        digest = (parameterized_digest(recipe, model) if version == PARAMETERIZED_VERSION
                  else engineering_digest(recipe))
        ordinal = len(ends) - 1 if ordinal is None else ordinal
        provenance = cp.canonical(dict(version=version, policy='fresh-random-per-segment-v1',
            allocation_sha256='0' * 64, segment=ordinal, seeds=[1, 2, 3],
            recipe_sha256=digest, model=model, evidence_status='not_assessed'))
        return cp.CheckpointContract('run', ordinal, 'attempt', 'job', recipe, BUILD,
            ends[-1], tuple(ends), ends[ordinal], ends[ordinal] * model['dt'], provenance,
            recipe_sha256=digest)

    def validate(self, contract, elapsed, *, step=None, atoms=None):
        model = json.loads(contract.stream_provenance).get('model', {'particles': 1024})
        (self.root / 'stdout').write_text(
            '1 by 1 by 1 MPI processor grid\nusing 1 OpenMP thread(s) per MPI task\n'
            f'Step Time Atoms\n{contract.global_step if step is None else step} '
            f'{elapsed:.17g} {model["particles"] if atoms is None else atoms}\nLoop time\n')
        cp.validate_boundary(self.root, asdict(contract))

    def test_reported_wca_accumulation_and_wrong_clocks(self):
        ends = tuple(99999 * n for n in range(1, 1002))
        contract = self.contract(ends, ordinal=450)
        elapsed = 0.0
        for _ in range(451):
            elapsed += 99999 * .0001
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)
        for bad in (elapsed + .0001, elapsed - .0001, contract.global_step * .005,
                    float('nan'), float('inf'), -float('inf')):
            with self.subTest(elapsed=bad), self.assertRaisesRegex(ValueError, 'clock mismatch'):
                self.validate(contract, bad)
        with self.assertRaises(ValueError):
            self.validate(contract, elapsed, step=contract.global_step - 1)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, elapsed, atoms=1024)

    def test_maximum_length_parameterized_chain_accumulates_custom_dt(self):
        ends = tuple(range(10, 1000001, 10))
        contract = self.contract(ends, version=PARAMETERIZED_VERSION, parameters={'dt': .003})
        elapsed = 0.0
        for _ in ends:
            elapsed += 10 * .003
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, elapsed + .003)

    def test_single_segment_counts_engine_command_splits(self):
        commands = 10000
        contract = self.contract((MAX_RUN_COMMAND * commands + 7,))
        elapsed = 0.0
        for _ in range(commands):
            elapsed += MAX_RUN_COMMAND * .0001
        elapsed += 7 * .0001
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)

    def test_future_segments_do_not_loosen_first_boundary(self):
        contract = self.contract(tuple(range(99999, 9999900001, 99999)), ordinal=0)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, contract.global_time * (1 + 1e-12))

    def test_legacy_clock_tolerance_is_unchanged(self):
        contract = cp.CheckpointContract('run', 2, 'attempt', 'job',
            Recipe('ideal-abp-2d-v1', 'statistical'), BUILD, 2000, (600, 1400, 2000),
            2000, 10.0, provenance())
        self.validate(contract, 10.0)
        self.validate(contract, 10.0 * (1 + 5e-15))
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, 10.0 * (1 + 2e-14))
        # Exact contract metadata still rejects altered clocks independently of
        # the tolerance used for the engine's rounded stdout evidence.
        with self.assertRaisesRegex(ValueError, 'boundary/clock mismatch'):
            self.validate(replace(contract, global_time=10.0 + 1e-13), 10.0)

