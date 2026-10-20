"""Workload identities shared by the TOTP runner and report generator."""

ALGORITHMS = (
    ('sha1', b'12345678901234567890'),
    ('sha256', b'12345678901234567890123456789012'),
    (
        'sha512',
        b'1234567890123456789012345678901234567890123456789012345678901234',
    ),
)


def case_definitions(impl: str) -> tuple[dict[str, str | None], ...]:
    cases: list[dict[str, str | None]] = []
    for algorithm, _raw_secret in ALGORITHMS:
        display_algorithm = algorithm.upper().replace('SHA', 'SHA-')
        for operation, label in (
            ('generate', 'Generate current code'),
            ('verify-current', 'Verify current code'),
            ('verify-previous', 'Verify previous-step code'),
            ('verify-invalid', 'Reject incorrect code'),
        ):
            display = f'{label} · {display_algorithm}'
            cases.append({
                'name': display,
                'group': 'comparison',
                'operation': operation,
                'algorithm': algorithm,
                'label': display,
            })
        if impl == 'totp-rs':
            for operation, label in (
                ('generate-raw-key', 'Generate from raw key'),
                ('generate-counter', 'Generate from time counter'),
            ):
                display = f'{label} · {display_algorithm}'
                cases.append({
                    'name': display,
                    'group': 'native-only',
                    'operation': operation,
                    'algorithm': algorithm,
                    'label': display,
                })
    if impl == 'totp-rs':
        cases.append({
            'name': 'Extract time counter',
            'group': 'native-only',
            'operation': 'time-window',
            'algorithm': None,
            'label': 'Extract time counter',
        })
    return tuple(cases)
