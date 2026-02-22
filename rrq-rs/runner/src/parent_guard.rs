use std::error::Error;

#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
#[cfg(unix)]
use std::thread::{self, JoinHandle};
#[cfg(unix)]
use std::time::Duration;

#[cfg(target_os = "linux")]
use nix::sys::{prctl, signal::Signal};
#[cfg(unix)]
use nix::unistd::Pid;

#[cfg(unix)]
const WATCHDOG_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(unix)]
static PARENT_WATCHDOG: OnceLock<Mutex<Option<ParentWatchdog>>> = OnceLock::new();

pub(crate) fn install_parent_lifecycle_guard() -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        install_parent_lifecycle_guard_with_hooks(
            nix::unistd::getppid,
            configure_linux_parent_death_signal,
            |initial_ppid| {
                ParentWatchdog::spawn(
                    initial_ppid,
                    WATCHDOG_POLL_INTERVAL,
                    nix::unistd::getppid,
                    |original_ppid, current_ppid| {
                        eprintln!(
                            "rrq-runner exiting after parent PID changed ({} -> {})",
                            original_ppid.as_raw(),
                            current_ppid.as_raw()
                        );
                        std::process::exit(1);
                    },
                )
            },
        )
    }
    #[cfg(not(unix))]
    {
        Ok(())
    }
}

#[cfg(unix)]
fn install_parent_lifecycle_guard_with_hooks<GP, LS, SW>(
    mut get_ppid: GP,
    linux_signal_setup: LS,
    spawn_watchdog: SW,
) -> Result<(), Box<dyn Error>>
where
    GP: FnMut() -> Pid,
    LS: FnOnce() -> Result<(), Box<dyn Error>>,
    SW: FnOnce(Pid) -> Result<ParentWatchdog, Box<dyn Error>>,
{
    if parent_watchdog_installed()? {
        return Ok(());
    }

    let initial_ppid = get_ppid();
    if is_orphan_ppid(initial_ppid) {
        return Err(orphaned_parent_error("startup"));
    }

    linux_signal_setup()?;

    let current_ppid = get_ppid();
    if is_orphan_ppid(current_ppid) {
        return Err(orphaned_parent_error("post-pdeathsig"));
    }

    if parent_watchdog_installed()? {
        return Ok(());
    }
    let watchdog = spawn_watchdog(initial_ppid)?;
    store_parent_watchdog(watchdog)?;
    Ok(())
}

#[cfg(unix)]
fn parent_watchdog_slot() -> &'static Mutex<Option<ParentWatchdog>> {
    PARENT_WATCHDOG.get_or_init(|| Mutex::new(None))
}

#[cfg(unix)]
fn parent_watchdog_installed() -> Result<bool, Box<dyn Error>> {
    let slot = parent_watchdog_slot()
        .lock()
        .map_err(|_| io::Error::other("parent watchdog lock poisoned"))?;
    Ok(slot.is_some())
}

#[cfg(unix)]
fn store_parent_watchdog(watchdog: ParentWatchdog) -> Result<(), Box<dyn Error>> {
    let mut slot = parent_watchdog_slot()
        .lock()
        .map_err(|_| io::Error::other("parent watchdog lock poisoned"))?;
    if slot.is_none() {
        *slot = Some(watchdog);
    }
    Ok(())
}

#[cfg(unix)]
const fn is_orphan_ppid(pid: Pid) -> bool {
    pid.as_raw() == 1
}

#[cfg(unix)]
fn orphaned_parent_error(stage: &str) -> Box<dyn Error> {
    io::Error::other(format!("runner parent is not alive ({stage})")).into()
}

#[cfg(target_os = "linux")]
fn configure_linux_parent_death_signal() -> Result<(), Box<dyn Error>> {
    prctl::set_pdeathsig(Some(Signal::SIGKILL)).map_err(|err| {
        io::Error::other(format!("failed to configure PR_SET_PDEATHSIG: {err}")).into()
    })
}

#[cfg(not(target_os = "linux"))]
fn configure_linux_parent_death_signal() -> Result<(), Box<dyn Error>> {
    Ok(())
}

#[cfg(unix)]
struct ParentWatchdog {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[cfg(unix)]
impl ParentWatchdog {
    fn spawn<P, C>(
        initial_ppid: Pid,
        poll_interval: Duration,
        mut poll_parent: P,
        mut on_parent_change: C,
    ) -> Result<Self, Box<dyn Error>>
    where
        P: FnMut() -> Pid + Send + 'static,
        C: FnMut(Pid, Pid) + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("rrq-runner-parent-watchdog".to_string())
            .spawn(move || {
                loop {
                    if stop_for_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    thread::sleep(poll_interval);
                    if stop_for_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    let current_ppid = poll_parent();
                    if current_ppid != initial_ppid {
                        on_parent_change(initial_ppid, current_ppid);
                        break;
                    }
                }
            })
            .map_err(|err| io::Error::other(format!("failed to spawn parent watchdog: {err}")))?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

#[cfg(unix)]
impl Drop for ParentWatchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[cfg(unix)]
    #[test]
    fn watchdog_triggers_callback_when_parent_changes() {
        let initial_ppid = Pid::from_raw(2000);
        let poll_count = Arc::new(AtomicUsize::new(0));
        let poll_count_for_watchdog = Arc::clone(&poll_count);
        let (tx, rx) = std::sync::mpsc::channel::<(i32, i32)>();

        let watchdog = ParentWatchdog::spawn(
            initial_ppid,
            Duration::from_millis(10),
            move || {
                if poll_count_for_watchdog.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    initial_ppid
                } else {
                    Pid::from_raw(2001)
                }
            },
            move |original_ppid, current_ppid| {
                let _ = tx.send((original_ppid.as_raw(), current_ppid.as_raw()));
            },
        )
        .expect("watchdog should spawn");

        let observed = rx
            .recv_timeout(Duration::from_millis(300))
            .expect("watchdog callback should fire");
        assert_eq!(observed, (2000, 2001));
        drop(watchdog);
    }

    #[cfg(all(unix, target_os = "linux"))]
    #[test]
    fn configure_linux_parent_death_signal_sets_sigkill() {
        use nix::sys::{prctl, signal::Signal};

        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK.get_or_init(|| Mutex::new(())).lock();
        let _guard = lock.expect("lock should not be poisoned");

        let original = prctl::get_pdeathsig().expect("get pdeathsig");
        configure_linux_parent_death_signal().expect("configure pdeathsig");
        let current = prctl::get_pdeathsig().expect("get pdeathsig after configure");
        assert_eq!(current, Some(Signal::SIGKILL));
        prctl::set_pdeathsig(original).expect("restore pdeathsig");
    }

    #[cfg(all(unix, target_os = "linux"))]
    #[test]
    fn install_fails_when_parent_is_orphaned_after_pdeathsig_setup() {
        let poll_calls = Arc::new(AtomicUsize::new(0));
        let poll_calls_for_hook = Arc::clone(&poll_calls);
        let err = install_parent_lifecycle_guard_with_hooks(
            move || {
                if poll_calls_for_hook.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    Pid::from_raw(4000)
                } else {
                    Pid::from_raw(1)
                }
            },
            || Ok(()),
            |_initial_ppid| {
                ParentWatchdog::spawn(
                    Pid::from_raw(4000),
                    Duration::from_secs(1),
                    || Pid::from_raw(4000),
                    |_old, _new| {},
                )
            },
        )
        .expect_err("expected orphan detection failure");
        assert!(
            err.to_string().contains("post-pdeathsig"),
            "unexpected error: {err}"
        );
    }
}
