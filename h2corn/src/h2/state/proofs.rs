use super::ReceiveWindowState;

#[kani::proof]
#[kani::unwind(8)]
fn receive_release_and_update_conserve_credit() {
    let initial: u32 = kani::any();
    kani::assume(initial <= 0x7FFF_FFFF);
    let mut state = ReceiveWindowState::new(initial);
    let mut available = u64::from(initial);
    let mut outstanding = 0_u64;
    let mut pending = 0_u64;

    for _ in 0..6 {
        let operation: u8 = kani::any();
        let amount: u32 = kani::any();
        let wide = u64::from(amount);
        match operation {
            0 => {
                let received = state.receive(amount);
                assert_eq!(received.is_ok(), wide <= available);
                if wide <= available {
                    available -= wide;
                    outstanding += wide;
                }
            },
            1 => {
                // Applications can release only bytes they still own.
                kani::assume(wide <= outstanding);
                state.release(amount);
                outstanding -= wide;
                pending += wide;
            },
            _ => {
                let update = state.take_update(amount);
                if pending != 0 && pending >= wide {
                    let increment = update.expect("released credit meets the threshold");
                    assert_eq!(u64::from(increment.get()), pending);
                    state.commit_update(increment);
                    available += pending;
                    pending = 0;
                } else {
                    assert_eq!(update, None);
                }
            },
        }
        assert_eq!(state.recv_window.0 as u64, available);
        assert_eq!(
            state
                .pending_update
                .map_or(0, |value| u64::from(value.get())),
            pending
        );
        assert_eq!(available + outstanding + pending, u64::from(initial));
    }
}
