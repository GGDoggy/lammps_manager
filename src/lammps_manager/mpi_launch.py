"""Single-host Open MPI launch policy shared by all local attempts."""


def effective_mpi_args(arguments: tuple[str, ...]) -> tuple[str, ...]:
    """Default to unbound scheduling; preserve one explicit binding policy.

    Accept Open MPI's separate and equals forms and legacy --bind-to-* aliases.
    Conflicting or repeated binding options fail before starting a process.
    """
    policies = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in ('--bind-to', '-bind-to'):
            if index + 1 == len(arguments) or not arguments[index + 1].strip() or arguments[index + 1].startswith('-'):
                raise ValueError('mpi_args: --bind-to requires a binding policy')
            policies.append(arguments[index + 1])
            index += 1
        elif argument.startswith(('--bind-to=', '-bind-to=')):
            policy = argument.split('=', 1)[1]
            if not policy.strip():
                raise ValueError('mpi_args: --bind-to requires a binding policy')
            policies.append(policy)
        elif argument.startswith('--bind-to-'):
            policies.append(argument.removeprefix('--bind-to-'))
        index += 1
    if len(policies) > 1:
        raise ValueError('mpi_args: repeated or conflicting binding policies')
    return arguments if policies else ('--bind-to', 'none', *arguments)
