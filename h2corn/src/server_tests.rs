use std::io;
use std::iter::repeat_with;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixListener;

use rustix::fs::{OFlags, fcntl_setfl};
use rustix::io::{FdFlags, fcntl_getfd};
use rustix::pipe::pipe;

use super::{ListenerFd, OwnFdsError, adopt_all, adopt_listeners, format_failure, own_serve_fds};
use crate::config::BindTarget;
use crate::error::H2CornError;

/// Pipe write ends to hand to the code under test, paired with their read ends.
///
/// Closure is observed on the *read* end (a pipe reports EOF once its last
/// writer is gone), not by probing the raw descriptor number: numbers are
/// recycled process-wide, so a sibling test thread opening a file between the
/// close and the check would make a descriptor-number oracle report failure.
fn owned_pipe_writes(count: usize) -> (Box<[ListenerFd]>, Vec<OwnedFd>) {
    let mut writes = Vec::with_capacity(count);
    let mut reads = Vec::with_capacity(count);
    for _ in 0..count {
        let (read, write) = pipe().expect("pipe creation succeeds");
        // Non-blocking, so a leaked writer fails the assertion instead of
        // hanging the suite.
        fcntl_setfl(&read, OFlags::NONBLOCK).expect("read end accepts O_NONBLOCK");
        reads.push(read);
        writes.push(write);
    }
    (writes.into_boxed_slice(), reads)
}

fn assert_closed(reads: &[OwnedFd]) {
    for read in reads {
        let mut buffer = [0_u8; 1];
        assert_eq!(
            rustix::io::read(read, &mut buffer),
            Ok(0),
            "the write end is still open, so the handle was not released"
        );
    }
}

fn binds(count: usize) -> Vec<BindTarget> {
    repeat_with(|| BindTarget::Tcp {
        host: Box::from("127.0.0.1"),
        port: 0,
    })
    .take(count)
    .collect()
}

fn assert_pipe_eof(read: &OwnedFd) {
    let mut byte = [0_u8; 1];
    loop {
        match rustix::io::read(read, &mut byte) {
            Ok(0) => return,
            Ok(_) => continue,
            Err(error) => panic!("unexpected pipe read error: {error}"),
        }
    }
}

fn invalid_fd_outside_rlimit() -> Option<i64> {
    let mut limit = std::mem::MaybeUninit::uninit();
    // SAFETY: `limit` is a valid output pointer and the resource selector is
    // supported on Unix targets running this test module.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // `rlim_cur` is the first descriptor number outside the permitted table.
    let limit = unsafe { limit.assume_init() }.rlim_cur;
    if limit == libc::RLIM_INFINITY {
        return None;
    }
    i64::try_from(limit)
        .ok()
        .filter(|fd| *fd <= i32::MAX as i64)
}

#[test]
fn serve_fd_duplicates_are_independent_of_sources() {
    let (read, source) = pipe().expect("pipe creation succeeds");
    let (duplicates, _) = own_serve_fds(vec![source.as_raw_fd() as i64], None).unwrap();
    drop(duplicates);
    rustix::io::write(&source, b"source").expect("source remains usable");
    drop(source);
    assert_pipe_eof(&read);

    let (read, source) = pipe().expect("pipe creation succeeds");
    let (duplicates, _) = own_serve_fds(vec![source.as_raw_fd() as i64], None).unwrap();
    drop(source);
    rustix::io::write(&duplicates[0], b"duplicate").expect("duplicate remains usable");
    drop(duplicates);
    assert_pipe_eof(&read);
}

#[test]
fn listener_duplication_failure_is_atomic_and_keeps_sources_open() {
    let Some(invalid_fd) = invalid_fd_outside_rlimit() else {
        eprintln!("skipping: RLIMIT_NOFILE is infinite or not representable as an fd");
        return;
    };
    let (read, source) = pipe().expect("pipe creation succeeds");
    let error = own_serve_fds(vec![source.as_raw_fd() as i64, invalid_fd], None)
        .expect_err("closed descriptor must fail duplication");
    assert!(matches!(error, OwnFdsError::Io(_)));
    rustix::io::write(&source, b"still open").expect("source remains open");
    drop(source);
    assert_pipe_eof(&read);
}

#[test]
fn quiesce_duplication_failure_is_atomic_and_keeps_sources_open() {
    let Some(invalid_fd) = invalid_fd_outside_rlimit() else {
        eprintln!("skipping: RLIMIT_NOFILE is infinite or not representable as an fd");
        return;
    };
    let (read, source) = pipe().expect("pipe creation succeeds");
    let error = own_serve_fds(vec![source.as_raw_fd() as i64], Some(invalid_fd))
        .expect_err("closed quiesce descriptor must fail duplication");
    assert!(matches!(error, OwnFdsError::Io(_)));
    rustix::io::write(&source, b"still open").expect("source remains open");
    drop(source);
    assert_pipe_eof(&read);
}

#[test]
fn duplicated_descriptors_are_cloexec_without_changing_sources() {
    let (read, source) = pipe().expect("pipe creation succeeds");
    let source_flags = fcntl_getfd(&source).expect("source flags readable");
    let (duplicates, _) = own_serve_fds(vec![source.as_raw_fd() as i64], None).unwrap();
    let duplicate_flags = fcntl_getfd(&duplicates[0]).expect("duplicate flags readable");
    assert_eq!(source_flags, fcntl_getfd(&source).unwrap());
    assert!(duplicate_flags.contains(FdFlags::CLOEXEC));
    drop(duplicates);
    drop(source);
    assert_pipe_eof(&read);
}

#[test]
fn structural_collisions_fail_before_any_duplication() {
    let (read, source) = pipe().expect("pipe creation succeeds");
    let number = source.as_raw_fd() as i64;
    assert!(matches!(
        own_serve_fds(vec![number], Some(number)),
        Err(OwnFdsError::Structural(_))
    ));
    rustix::io::write(&source, b"still open").expect("source remains open");
    drop(source);
    assert_pipe_eof(&read);
}

#[test]
fn count_mismatch_closes_every_unadopted_handle() {
    let (fds, raw) = owned_pipe_writes(2);
    let result = adopt_all(&binds(1), fds, |_, fd| Ok::<_, io::Error>(fd));
    let _ = result.unwrap_err();
    assert_closed(&raw);
}

#[test]
fn mid_adoption_failure_closes_prior_current_and_remaining_handles() {
    let (fds, raw) = owned_pipe_writes(3);
    let mut index = 0;
    let result = adopt_all(&binds(3), fds, |_, fd| {
        let current = index;
        index += 1;
        if current == 1 {
            return Err(io::Error::other("injected adoption failure"));
        }
        Ok(fd)
    });
    let _ = result.unwrap_err();
    assert_closed(&raw);
}

#[test]
fn tls_adoption_rejects_unix_listeners_at_the_ownership_boundary() {
    let path = std::env::temp_dir().join(format!("h2corn-adopt-tls-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("temporary Unix listener binds");
    let fd: OwnedFd = listener.into();
    let binds = [BindTarget::Fd { fd: 0 }];

    let Err(error) = adopt_listeners(&binds, vec![fd].into_boxed_slice(), true) else {
        panic!("TLS must not adopt a Unix listener");
    };
    assert!(
        error
            .to_string()
            .contains("TLS is supported only on TCP listeners")
    );
    std::fs::remove_file(path).expect("temporary Unix listener path is removable");
}

#[test]
fn application_failures_keep_the_python_traceback() {
    pyo3::Python::initialize();
    pyo3::Python::attach(|py| {
        let err = py
            .run(
                pyo3::ffi::c_str!("def crash():\n    1 / 0\ncrash()"),
                None,
                None,
            )
            .expect_err("the Python application crashes");
        let message = format_failure(&H2CornError::from(err));

        assert!(message.starts_with("Traceback (most recent call last):"));
        assert!(message.contains("line 2, in crash"));
        assert!(message.contains("ZeroDivisionError: division by zero"));
    });
}
