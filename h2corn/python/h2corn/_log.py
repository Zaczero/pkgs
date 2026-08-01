"""Diagnostic output for the Python half of the server.

The Rust side owns the same two encodings (`src/log/`); this is the mirror for
the supervisor, the reloader and the CLI, which run in processes that never
enter Rust. Both halves write to the same stderr, so a consumer that asked for
JSON must not receive prose from one of them.
"""

from __future__ import annotations

import json
import sys
from enum import Enum
from typing import Literal

LogFormat = Literal['text', 'json']


class Event(Enum):
    """Every diagnostic the Python half writes.

    The JSON `event` field is a contract with whatever consumes the stream, so
    the vocabulary is a closed type rather than a string spelled at each call
    site. Severity travels with the event: the same event can never be logged
    at two different levels.
    """

    WORKER_STARTED = ('worker_started', 'info')
    WORKER_STOPPED = ('worker_stopped', 'info')
    WORKER_EXITED_UNEXPECTEDLY = ('worker_exited_unexpectedly', 'error')
    WORKER_KILLED = ('worker_killed', 'error')
    WORKER_HEALTHCHECK_FAILED = ('worker_healthcheck_failed', 'error')
    QUIESCE_SIGNAL_FAILED = ('quiesce_signal_failed', 'error')
    SUPERVISOR_STOPPING = ('supervisor_stopping', 'info')
    SUPERVISOR_FAILED = ('supervisor_failed', 'error')
    RELOAD_ENABLED = ('reload_enabled', 'info')
    RELOAD_TRIGGERED = ('reload_triggered', 'info')
    CLI_ERROR = ('cli_error', 'error')

    def __init__(self, name: str, level: Literal['info', 'error']) -> None:
        self.event = name
        self.level = level

    def log(self, template: str, /, **fields: object) -> None:
        """Write this event in the configured encoding.

        `template` is the human sentence, written as a format string over
        `fields` — so a value appears exactly once and the two renderings
        cannot drift apart::

            Event.WORKER_STARTED.log('Started worker [{pid}]', pid=process.pid)

        A sentence built elsewhere is passed as a field rather than as the
        template, so a brace inside it stays data::

            Event.WORKER_KILLED.log('{message}', message=message, pid=pid)
        """
        if _FORMAT == 'json':
            record: dict[str, object] = {'level': self.level, 'event': self.event}
            record.update(fields)
            # `separators` keeps one record on one line; `default=str` means a
            # Path or an exception logs as its text rather than raising here.
            line = json.dumps(record, separators=(',', ':'), default=str)
        else:
            line = template.format(**fields)
        sys.stderr.write(f'{line}\n')
        sys.stderr.flush()


#: Process-wide, published once at startup. The supervisor logs from signal
#: handlers and reaper callbacks that hold no configuration.
_FORMAT: LogFormat = 'text'


def set_format(value: LogFormat) -> None:
    global _FORMAT
    _FORMAT = value
