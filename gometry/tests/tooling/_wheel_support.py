"""Helpers for constructing synthetic wheel records in tooling tests."""

from __future__ import annotations

import base64
import csv
import hashlib
import io


def record_digest(payload: bytes) -> str:
    encoded = base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
    return 'sha256=' + encoded.rstrip(b'=').decode('ascii')


def record_bytes(
    members: dict[str, bytes],
    *,
    rows: list[tuple[str, str, str]] | None = None,
) -> bytes:
    if rows is None:
        record_name = next(
            name for name in members if name.endswith('.dist-info/RECORD')
        )
        existing = members[record_name]
        if existing:
            source_rows = csv.reader(
                io.StringIO(existing.decode('utf-8'), newline=''),
                strict=True,
            )
            rows = []
            recorded: set[str] = set()
            signatures = {
                name
                for name in members
                if name.endswith(('/RECORD.jws', '/RECORD.p7s'))
            }
            for row in source_rows:
                if len(row) != 3:
                    raise AssertionError('wheel RECORD rows must have three columns')
                name = row[0]
                if name == record_name:
                    rows.append((name, '', ''))
                    recorded.add(name)
                elif name in members and name not in signatures:
                    rows.append((
                        name,
                        record_digest(members[name]),
                        str(len(members[name])),
                    ))
                    recorded.add(name)
            rows.extend(
                (
                    name,
                    record_digest(members[name]),
                    str(len(members[name])),
                )
                for name in sorted(set(members) - recorded - signatures)
            )
        else:
            rows = [
                (name, record_digest(payload), str(len(payload)))
                for name, payload in sorted(members.items())
                if not name.endswith(('/RECORD.jws', '/RECORD.p7s'))
                and not name.endswith('/RECORD')
            ]
            rows.append((record_name, '', ''))
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, lineterminator='\n')
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8')
