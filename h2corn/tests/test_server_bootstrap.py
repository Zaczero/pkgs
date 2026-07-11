import io
import os
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from h2corn import Config


def _ipv6_loopback_is_bindable() -> bool:
    """Whether this kernel namespace has IPv6 enabled, not merely compiled."""
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(('::1', 0))
    except OSError:
        return False
    return True


def test_event_loop_factory_selection_is_explicit() -> None:
    import asyncio

    from h2corn._server import _event_loop_factory

    assert _event_loop_factory('asyncio') is asyncio.new_event_loop
    auto = _event_loop_factory('auto')
    try:
        import uvloop
    except ImportError:
        assert auto is asyncio.new_event_loop
    else:
        assert auto is uvloop.new_event_loop
        assert _event_loop_factory('uvloop') is uvloop.new_event_loop


def test_python_m_h2corn_runs_cli_without_target() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


def test_python_m_h2corn_accepts_target_before_arguments() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn', 'example:app', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


def test_python_m_h2corn_server_runs_without_runpy_warning() -> None:
    result = subprocess.run(
        [sys.executable, '-Werror', '-m', 'h2corn._server', '--check-config'],
        check=False,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0
    assert 'RuntimeWarning' not in result.stderr


@pytest.fixture
def bind_listeners():
    """Build real bound listeners from a Config and close them at teardown.

    These tests exercise the actual socket setup against the running kernel
    rather than mocking the platform and asserting setsockopt call sequences —
    the listener's observable end state (non-blocking, port, options) is what
    matters and it stays meaningful on every OS.
    """
    from h2corn import _socket

    opened: list[socket.socket] = []

    def _bind(**config_kwargs: Any) -> tuple[Config, list[socket.socket]]:
        config = Config(**config_kwargs)
        sockets, _owned = _socket._build_sockets(config)
        opened.extend(sockets)
        return config, list(sockets)

    yield _bind
    for sock in opened:
        sock.close()


def test_listener_is_nonblocking(bind_listeners) -> None:
    # Whichever way it is made non-blocking (SOCK_NONBLOCK at creation on Linux,
    # setblocking(False) afterwards elsewhere), the listener must end up so.
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets
    assert all(not sock.getblocking() for sock in sockets)


def test_listener_sets_reuseaddr(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets[0].getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)


def test_build_sockets_records_kernel_allocated_port(bind_listeners) -> None:
    config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    bound_port = sockets[0].getsockname()[1]
    assert bound_port > 0
    assert config.bind == (f'127.0.0.1:{bound_port}',)


@pytest.mark.skipif(
    not _ipv6_loopback_is_bindable(),
    reason='IPv6 loopback is disabled in this kernel namespace',
)
def test_build_sockets_shares_kernel_port_across_zero_binds(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0', '[::1]:0'))
    assert len(sockets) == 2
    assert len({sock.getsockname()[1] for sock in sockets}) == 1


def test_build_sockets_rolls_back_listeners_on_partial_failure() -> None:
    from h2corn import _socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        free_port = probe.getsockname()[1]

    # The first listener binds the free port; the second targets a non-local
    # address (TEST-NET-1, RFC 5737) that no host can bind, so the build fails
    # partway and must roll back — leaving the first port bindable again.
    config = Config(bind=(f'127.0.0.1:{free_port}', f'192.0.2.1:{free_port}'))
    with pytest.raises(OSError):
        _socket._build_sockets(config)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as rebind:
        rebind.bind(('127.0.0.1', free_port))


@pytest.mark.skipif(sys.platform != 'linux', reason='TCP_DEFER_ACCEPT is Linux-only')
def test_listener_sets_tcp_defer_accept_on_linux(bind_listeners) -> None:
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets[0].getsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT) > 0


@pytest.mark.skipif(
    sys.platform != 'linux',
    reason='server-side TCP Fast Open via setsockopt is Linux-only',
)
def test_listener_enables_tcp_fastopen_on_linux(bind_listeners) -> None:
    # On a Linux listener TCP_FASTOPEN stores the accept-queue length we request.
    _config, sockets = bind_listeners(bind=('127.0.0.1:0',))
    assert sockets[0].getsockopt(socket.IPPROTO_TCP, socket.TCP_FASTOPEN) > 0


@pytest.mark.skipif(sys.platform == 'win32', reason='unix sockets are not supported')
def test_build_unix_socket_applies_owner_ids(
    monkeypatch: pytest.MonkeyPatch,
    unix_socket_dir: Path,
) -> None:
    from h2corn import _socket

    socket_path = unix_socket_dir / 'owned.sock'
    config = Config(bind=(f'unix:{socket_path}',))

    # chown(2) needs privileges we lack in CI, so record the request rather than
    # perform it; everything else — creating and binding the socket — is real.
    chowns = []
    monkeypatch.setattr(_socket.os, 'chown', lambda *args: chowns.append(args))

    sock = _socket._build_unix_socket(
        socket_path,
        config,
        owner_uid=1000,
        owner_gid=1001,
    )
    try:
        assert socket_path.is_socket()
        assert chowns == [(str(socket_path), 1000, 1001)]
    finally:
        sock.close()


def test_resolve_process_identity_uses_user_primary_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    monkeypatch.setattr(_server.sys, 'platform', 'linux')
    monkeypatch.setitem(
        sys.modules,
        'pwd',
        SimpleNamespace(
            getpwnam=lambda value: SimpleNamespace(
                pw_name=value,
                pw_uid=1000,
                pw_gid=1001,
            ),
            getpwuid=lambda value: SimpleNamespace(
                pw_name='www-data',
                pw_uid=value,
                pw_gid=1001,
            ),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        'grp',
        SimpleNamespace(
            getgrnam=lambda _value: SimpleNamespace(gr_gid=2001),
            getgrgid=lambda value: SimpleNamespace(gr_gid=value),
        ),
    )

    identity = _server._resolve_process_identity(Config(user='www-data'))

    assert identity == _server._ProcessIdentity(
        uid=1000,
        gid=1001,
        username='www-data',
    )


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX privilege drop')
def test_drop_process_privileges_sets_groups_before_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    calls = []
    monkeypatch.setattr(_server.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(_server.os, 'getegid', lambda: 0)
    monkeypatch.setattr(
        _server.os,
        'initgroups',
        lambda *args: calls.append(('initgroups', args)),
    )
    monkeypatch.setattr(
        _server.os,
        'setgid',
        lambda *args: calls.append(('setgid', args)),
    )
    monkeypatch.setattr(
        _server.os,
        'setuid',
        lambda *args: calls.append(('setuid', args)),
    )

    _server._drop_process_privileges(
        _server._ProcessIdentity(uid=1000, gid=1001, username='www-data')
    )

    assert calls == [
        ('initgroups', ('www-data', 1001)),
        ('setgid', (1001,)),
        ('setuid', (1000,)),
    ]


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_serve_cli_target_defers_import_when_privilege_drop_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}
    imported = False

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    monkeypatch.setattr(_server.sys, 'platform', 'linux')
    monkeypatch.setattr(_server, '_import_target', _import_target)

    from h2corn import _supervisor

    monkeypatch.setattr(
        _supervisor,
        '_serve_supervisor',
        lambda app, config: captured.setdefault('supervisor', (app, config)),
    )

    import_settings = ImportSettings(target='example:app')
    config = Config(user='www-data')

    _server._serve_cli_target(import_settings, config)

    assert imported is False
    assert captured['supervisor'] == (import_settings, config)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX worker supervisor')
def test_worker_entry_imports_after_privilege_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server, _supervisor
    from h2corn._cli import ImportSettings

    order: list[str] = []
    captured = {}

    async def imported_app(_scope, _receive, _send):
        return None

    monkeypatch.setattr(
        _server,
        '_drop_process_privileges',
        lambda _identity: order.append('drop'),
    )
    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda _import_settings: order.append('import') or imported_app,
    )

    class FakeServer:
        def __init__(self, app, config):
            captured['app'] = app
            self.app = app
            self.config = config

        async def _serve_fds(self, *_args, **_kwargs):
            return None

        def shutdown(self, kind='stop'):
            return None

        def restart(self):
            return None

    async def _serve_with_lifespan(_app, _serve, **_kwargs):
        return None

    monkeypatch.setattr(_server, 'Server', FakeServer)
    monkeypatch.setattr(_supervisor, '_serve_with_lifespan', _serve_with_lifespan)

    import_settings = ImportSettings(target='example:app')
    _supervisor._worker_entry(import_settings, Config(), (), object())

    assert order == ['drop', 'import']
    assert captured['app'] is imported_app


def test_pidfile_writes_and_cleans_up_regular_file(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'

    with _server._pidfile(Config(pid=pid_path)):
        assert pid_path.read_text() == f'{os.getpid()}\n'

    assert not pid_path.exists()


@pytest.mark.skipif(sys.platform == 'win32', reason='symlink semantics differ')
def test_pidfile_rejects_preexisting_symlink(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'
    victim = tmp_path / 'victim.txt'
    victim.write_text('SECRET\n')
    pid_path.symlink_to(victim)

    with pytest.raises(OSError), _server._pidfile(Config(pid=pid_path)):
        pass

    assert victim.read_text() == 'SECRET\n'
    assert pid_path.is_symlink()


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='an open pidfile cannot be replaced on Windows (it is locked)',
)
def test_pidfile_cleanup_keeps_replaced_path(tmp_path: Path) -> None:
    from h2corn import _server

    pid_path = tmp_path / 'h2corn.pid'

    with _server._pidfile(Config(pid=pid_path)):
        pid_path.unlink()
        pid_path.write_text('replacement\n')

    assert pid_path.read_text() == 'replacement\n'


def test_import_target_requires_module_app_form() -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    with pytest.raises(ValueError, match='module:app form'):
        _server._import_target(ImportSettings(target='demoapp'))


def test_import_target_requires_callable_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_not_callable'
    (tmp_path / f'{module_name}.py').write_text('app = 1\n')
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(
        TypeError,
        match=rf"import target '{module_name}:app' is not callable",
    ):
        _server._import_target(ImportSettings(target=f'{module_name}:app'))


def test_import_target_calls_factory_when_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_factory'
    (tmp_path / f'{module_name}.py').write_text(
        """
def create_app():
    async def app(scope, receive, send):
        return None
    return app
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    app = _server._import_target(
        ImportSettings(target=f'{module_name}:create_app', factory=True)
    )

    assert callable(app)


def test_import_target_requires_factory_result_to_be_callable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    module_name = 'demoapp_factory_value'
    (tmp_path / f'{module_name}.py').write_text(
        """
def create_app():
    return 1
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(
        TypeError,
        match=rf"import target '{module_name}:create_app' factory returned a non-callable",
    ):
        _server._import_target(
            ImportSettings(target=f'{module_name}:create_app', factory=True)
        )


def test_import_target_uses_app_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    (app_dir / 'demoapp_in_app_dir.py').write_text(
        """
async def app(scope, receive, send):
    return None
"""
    )
    monkeypatch.chdir(tmp_path)

    app = _server._import_target(
        ImportSettings(target='demoapp_in_app_dir:app', app_dir=app_dir)
    )

    assert callable(app)


def test_import_target_moves_app_dir_to_sys_path_front(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    shadow_dir = tmp_path / 'shadow'
    app_dir.mkdir()
    shadow_dir.mkdir()
    (app_dir / 'demoapp_precedence.py').write_text(
        """
async def app(scope, receive, send):
    return None

app.source = 'app-dir'
"""
    )
    (shadow_dir / 'demoapp_precedence.py').write_text(
        """
async def app(scope, receive, send):
    return None

app.source = 'shadow'
"""
    )
    monkeypatch.setattr(sys, 'path', [str(shadow_dir), str(app_dir), *sys.path])
    monkeypatch.chdir(tmp_path)

    app = _server._import_target(
        ImportSettings(target='demoapp_precedence:app', app_dir=app_dir)
    )

    assert app.source == 'app-dir'


def test_import_target_loads_env_file_before_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    env_file = tmp_path / '.env'
    env_file.write_text('DEMO_APP_VALUE=loaded\n')
    (app_dir / 'demoapp_from_env.py').write_text(
        """
import os

async def app(scope, receive, send):
    return None

app.loaded = os.environ['DEMO_APP_VALUE']
"""
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('DEMO_APP_VALUE', raising=False)

    app = _server._import_target(
        ImportSettings(
            target='demoapp_from_env:app',
            app_dir=app_dir,
            env_file=env_file,
        )
    )

    assert app.loaded == 'loaded'


def test_import_target_env_file_does_not_override_existing_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    app_dir = tmp_path / 'src'
    app_dir.mkdir()
    env_file = tmp_path / '.env'
    env_file.write_text('DEMO_APP_VALUE=loaded\n')
    (app_dir / 'demoapp_existing_env.py').write_text(
        """
import os

async def app(scope, receive, send):
    return None

app.loaded = os.environ['DEMO_APP_VALUE']
"""
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('DEMO_APP_VALUE', 'existing')

    app = _server._import_target(
        ImportSettings(
            target='demoapp_existing_env:app',
            app_dir=app_dir,
            env_file=env_file,
        )
    )

    assert app.loaded == 'existing'


@pytest.mark.parametrize(
    ('family', 'is_unix'),
    [
        (socket.AF_INET, False),
        pytest.param(
            getattr(socket, 'AF_UNIX', None),
            True,
            marks=pytest.mark.skipif(
                sys.platform == 'win32',
                reason='unix sockets are not supported on Windows',
            ),
        ),
    ],
)
def test_build_sockets_records_fd_listener_family(
    monkeypatch: pytest.MonkeyPatch,
    family: int,
    is_unix: bool,
) -> None:
    from h2corn import _socket

    class FakeSocket:
        def __init__(self) -> None:
            self.family = family

        def setblocking(self, _blocking: bool) -> None:
            pass

    config = Config(bind=('fd://7',))

    def fake_socket(*, fileno: int):
        assert fileno == 7
        return FakeSocket()

    monkeypatch.setattr(_socket.socket, 'socket', fake_socket)

    sockets, _owned_socket_paths = _socket._build_sockets(config)

    assert len(sockets) == 1
    assert config.bind == ('fd://7',)
    assert config._bind_fd_is_unix == (is_unix,)


@pytest.mark.skipif(
    sys.platform == 'win32', reason='the signal wakeup pipe is a POSIX mechanism'
)
def test_signal_wakeup_pipe_yields_nonblocking_fd() -> None:
    # However the pipe is created (os.pipe2 on Linux, os.pipe + set_blocking
    # elsewhere), it must hand back a valid non-blocking read fd and close it
    # again on exit.
    from h2corn import _socket

    with _socket._signal_wakeup_pipe() as read_fd:
        assert read_fd >= 0
        assert os.get_blocking(read_fd) is False
    with pytest.raises(OSError):
        os.fstat(read_fd)


def test_cli_trusted_proxy_flags_replace_base_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
forwarded_allow_ips = ["127.0.0.1", "::1"]
""".strip()
    )
    captured = {}

    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda import_settings: import_settings,
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('result', (_app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--forwarded-allow-ips',
            '10.0.0.1, unix',
            'example:app',
        ],
    )

    _server.main()

    app, config = captured['result']
    assert app.target == 'example:app'
    assert app.factory is False
    assert config.forwarded_allow_ips == ('10.0.0.1', 'unix')


def test_cli_repeated_bind_replaces_base_bind_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["127.0.0.1:9010"]')
    captured = {}

    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda _import_settings: object(),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--bind',
            '127.0.0.1:9030',
            '--bind',
            'unix:/tmp/h2corn.sock',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].bind == ('127.0.0.1:9030', 'unix:/tmp/h2corn.sock')


def test_cli_arguments_override_env_and_toml_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
port = 9010
http1 = false
access_log = false
""".strip()
    )
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_ACCESS_LOG', 'false')
    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda _import_settings: object(),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--port',
            '9030',
            '--http1',
            '--access-log',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].port == 9030
    assert captured['config'].http1 is True
    assert captured['config'].access_log is True


def test_cli_legacy_env_port_overrides_toml_listener(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('port = 9010')
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda _import_settings: object(),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--config', str(config_path), 'example:app'],
    )

    _server.main()

    assert captured['config'].port == 9020


def test_cli_factory_flag_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda import_settings: captured.setdefault('import', import_settings),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('serve', (_app, config)),
    )
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--factory', 'example:create_app'])

    _server.main()

    assert captured['import'] == ImportSettings(
        target='example:create_app',
        factory=True,
    )
    assert captured['serve'][0] == ImportSettings(
        target='example:create_app',
        factory=True,
    )


def test_cli_app_dir_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda import_settings: captured.setdefault('import', import_settings),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('serve', (_app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--app-dir', str(tmp_path / 'src'), 'example:app'],
    )

    _server.main()

    assert captured['import'] == ImportSettings(
        target='example:app',
        app_dir=(tmp_path / 'src').resolve(),
    )
    assert captured['serve'][0] == ImportSettings(
        target='example:app',
        app_dir=(tmp_path / 'src').resolve(),
    )


def test_cli_env_file_is_forwarded_to_import_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server
    from h2corn._cli import ImportSettings

    captured = {}

    monkeypatch.setattr(
        _server,
        '_import_target',
        lambda import_settings: captured.setdefault('import', import_settings),
    )
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('serve', (_app, config)),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--env-file', str(tmp_path / '.env'), 'example:app'],
    )

    _server.main()

    assert captured['import'] == ImportSettings(
        target='example:app',
        env_file=(tmp_path / '.env').resolve(),
    )
    assert captured['serve'][0] == ImportSettings(
        target='example:app',
        env_file=(tmp_path / '.env').resolve(),
    )


def test_cli_check_config_exits_before_import_and_serve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, '_import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--check-config', 'example:app'])

    _server.main()

    assert imported is False
    assert served is False


def test_cli_check_config_works_without_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, '_import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--check-config'])

    _server.main()

    assert imported is False
    assert served is False


def test_cli_print_config_exits_before_import_and_serve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False
    stdout = io.StringIO()

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, '_import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'stdout', stdout)
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--print-config', '--workers', '2', 'example:app'],
    )

    _server.main()

    assert imported is False
    assert served is False
    assert 'workers = 2' in stdout.getvalue()


def test_cli_print_config_works_without_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    imported = False
    served = False
    stdout = io.StringIO()

    def _import_target(_import_settings):
        nonlocal imported
        imported = True
        return object()

    def _serve(_app, config=None):
        nonlocal served
        served = True

    monkeypatch.setattr(_server, '_import_target', _import_target)
    monkeypatch.setattr(_server, 'serve', _serve)
    monkeypatch.setattr(sys, 'stdout', stdout)
    monkeypatch.setattr(sys, 'argv', ['h2corn', '--print-config'])

    _server.main()

    assert imported is False
    assert served is False
    assert 'workers = 1' in stdout.getvalue()
