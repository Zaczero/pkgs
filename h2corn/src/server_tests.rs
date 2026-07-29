use std::io;
use std::iter::repeat_with;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixListener;

use rustix::fs::{OFlags, fcntl_setfl};
use rustix::pipe::pipe;

use super::{ListenerFd, adopt_all, adopt_listeners};
use crate::config::BindTarget;

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
