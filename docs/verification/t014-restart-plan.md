# T014 external restart acceptance plan (not executed)

Local unittest evidence does not establish real-host acceptance. Execute this
plan only under separate authorization covering the selected test hosts, worker
service restarts, host reboot and test simulations. Do not reboot a shared host
or interrupt unrelated work as part of default development checks.

1. Record manager/worker versions, dedicated roots, boot UUIDs, service identities,
   immutable submission policy, MPI/LAMMPS versions and resource reservations.
   Use a small identifiable simulation, a finite one-recovery budget and retained
   output. Confirm there are no unrelated workloads affected by the test.
2. Restart only the worker service while its simulation continues. Verify the
   same PID/start ticks/session is monitored, reservations stay held, heartbeat
   and cancellation recover, and there is exactly one invocation.
3. With explicit host reboot authorization, reboot the dedicated worker host.
   Verify its boot UUID changes, the saved launch identity belongs to the old
   boot, and no retry is dispatched before conclusive evidence is reported.
   Confirm exactly one queued recovery, its durable deadline and a fresh attempt.
4. Interrupt the report response after the manager commits; restart the manager
   before the deadline. Verify the same charge/deadline, no duplicate attempt,
   preserved old output and eventual new output in shared and staged modes.
5. Repeat an infrastructure interruption to exhaust the budget. Verify the
   actionable failure, no further automatic launch and distinct application
   retry accounting. Cancel a recovery during its delay and confirm no launch.
6. Separately with isolated fake metadata, exercise missing/corrupt process
   identity and surviving MPI descendants. Verify unknown state and held
   reservations; absence of a leader or a changed worker session is insufficient.

Retain commands, timestamps, job/attempt/generation IDs, immutable specifications,
event history, reservation snapshots, process-tree observations, output hashes
and collection acknowledgements. Do not claim checkpoint continuation: T018-T019
are prerequisites. Record any unsupported launch/session behavior as a blocker.
