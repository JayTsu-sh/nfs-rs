//! Fixed, bounded protocol backpressure handling, separate from transport replay.
//! RFC 1813 §2.6, RFC 7530 §13.1.1.3 and RFC 8881 §15.1.1.3.
use crate::{NfsError, RequestContext, Result, RetryExhaustion, ServerBusyError};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

pub(crate) const MAX_RETRIES: u32 = 16;
const MAX_ELAPSED: Duration = Duration::from_secs(60);
const BASE_MS: u64 = 200;
const CAP_MS: u64 = 5000;

tokio::task_local! { static BUDGET: Arc<Budget>; }
tokio::task_local! { static CALLER_DEADLINE: Instant; }
#[cfg(feature = "python-bindings")]
tokio::task_local! { static READ_CANCEL: Arc<ReadCancel>; }

#[cfg(feature = "python-bindings")]
struct ReadCancel {
    cancelled: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

/// Dropping a read waiter stops further busy reissues. An already in-flight
/// request still settles, preserving the adapter's cursor and OPEN ownership.
#[cfg(feature = "python-bindings")]
pub(crate) struct ReadWaiter(Arc<ReadCancel>);
#[cfg(feature = "python-bindings")]
impl Drop for ReadWaiter {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

#[cfg(feature = "python-bindings")]
pub(crate) fn read_waiter<F: Future>(
    enabled: bool,
    future: F,
) -> (Option<ReadWaiter>, impl Future<Output = F::Output>) {
    let state = enabled.then(|| {
        Arc::new(ReadCancel {
            cancelled: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        })
    });
    let guard = state.as_ref().map(|s| ReadWaiter(s.clone()));
    (guard, async move {
        match state {
            Some(state) => READ_CANCEL.scope(state, future).await,
            None => future.await,
        }
    })
}

pub(crate) struct Budget {
    started: Instant,
    deadline: Instant,
    retries: AtomicU32,
}

impl Budget {
    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            deadline: cap_deadline(started + MAX_ELAPSED),
            retries: AtomicU32::new(0),
        }
    }

    fn exhausted(
        &self,
        context: RequestContext,
        source: NfsError,
        reason: RetryExhaustion,
    ) -> NfsError {
        tracing::warn!(operation=%context.operation,protocol=?context.protocol,
            retry_count=self.retries.load(Ordering::Relaxed),elapsed_ms=self.started.elapsed().as_millis(),
            ?reason,status=%source,"NFS server-busy retry budget exhausted");
        NfsError::ServerBusy(Box::new(ServerBusyError {
            context,
            source: Box::new(source),
            reason,
            retries: self.retries.load(Ordering::Relaxed),
            elapsed: self.started.elapsed(),
        }))
    }

    pub(crate) async fn backoff(&self, context: RequestContext, source: NfsError) -> Result<()> {
        let deadline = cap_deadline(self.deadline);
        if Instant::now() >= deadline {
            return Err(self.exhausted(context, source, RetryExhaustion::Deadline));
        }
        let attempt = self
            .retries
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < MAX_RETRIES).then_some(n + 1)
            });
        let attempt = match attempt {
            Ok(attempt) => attempt,
            Err(_) => return Err(self.exhausted(context, source, RetryExhaustion::RetryLimit)),
        };
        let maximum = BASE_MS.saturating_mul(1u64 << attempt).min(CAP_MS);
        let wait = Duration::from_millis(rand::random_range(maximum / 2..maximum));
        tracing::debug!(operation=%context.operation, protocol=?context.protocol,
            retry=attempt+1, delay_ms=wait.as_millis(), elapsed_ms=self.started.elapsed().as_millis(),
            status=%source, "temporary NFS rejection; bounded retry");
        let wake = Instant::now() + wait;
        let sleep = tokio::time::sleep_until(wake.min(deadline));
        #[cfg(feature = "python-bindings")]
        if let Ok(cancel) = READ_CANCEL.try_with(Arc::clone) {
            let notified = cancel.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if cancel.cancelled.load(Ordering::Acquire) {
                return Err(cancelled_read());
            }
            tokio::select! { _=sleep=>{}, _=notified=>return Err(cancelled_read()) }
        } else {
            sleep.await;
        }
        #[cfg(not(feature = "python-bindings"))]
        sleep.await;
        if Instant::now() >= deadline {
            return Err(self.exhausted(context, source, RetryExhaustion::Deadline));
        }
        Ok(())
    }
}

#[cfg(feature = "python-bindings")]
fn cancelled_read() -> NfsError {
    NfsError::transport(
        crate::RequestTransmission::NotSent,
        NfsError::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "read waiter cancelled during server-busy backoff",
        )),
    )
}

/// Nested protocol calls, pNFS refresh and MDS fallback share the same budget.
/// This scope is per NFS operation, not per entire file transfer.
pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    let budget = BUDGET
        .try_with(Arc::clone)
        .unwrap_or_else(|_| Arc::new(Budget::new()));
    BUDGET.scope(budget, future).await
}

pub(crate) fn current() -> Arc<Budget> {
    BUDGET.with(Arc::clone)
}

/// Bound existing transport retries by the same operation deadline.
pub(crate) fn cap_deadline(deadline: Instant) -> Instant {
    let deadline = BUDGET
        .try_with(|b| deadline.min(b.deadline))
        .unwrap_or(deadline);
    CALLER_DEADLINE
        .try_with(|caller| deadline.min(*caller))
        .unwrap_or(deadline)
}

/// The adapter's explicit timeout may be shorter than the fixed protocol bound.
/// Capture it before spawning, so task scheduling does not restart the clock.
#[cfg(feature = "python-bindings")]
pub(crate) fn with_timeout<F: Future>(
    timeout: Option<Duration>,
    future: F,
) -> impl Future<Output = F::Output> {
    let deadline = timeout.map(|duration| cap_deadline(Instant::now() + duration));
    async move {
        match deadline {
            Some(deadline) => CALLER_DEADLINE.scope(deadline, future).await,
            None => future.await,
        }
    }
}

/// Temporary OPENs still need CLOSE after the caller's operation deadline.
/// Cleanup gets its own bound; it never resubmits the original mutation.
#[cfg(feature = "python-bindings")]
pub(crate) async fn cleanup<F: Future>(future: F) -> F::Output {
    let started = Instant::now();
    let deadline = started + MAX_ELAPSED;
    let budget = Arc::new(Budget {
        started,
        deadline,
        retries: AtomicU32::new(0),
    });
    let cancel = Arc::new(ReadCancel {
        cancelled: std::sync::atomic::AtomicBool::new(false),
        notify: tokio::sync::Notify::new(),
    });
    READ_CANCEL
        .scope(
            cancel,
            CALLER_DEADLINE.scope(deadline, BUDGET.scope(budget, future)),
        )
        .await
}

/// Preserve an active budget when admitted work is detached from its caller.
pub(crate) fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let budget = BUDGET.try_with(Arc::clone).ok();
    let caller = CALLER_DEADLINE.try_with(|d| *d).ok();
    #[cfg(feature = "python-bindings")]
    let cancel = READ_CANCEL.try_with(Arc::clone).ok();
    async move {
        let work = async move {
            match budget {
                Some(budget) => BUDGET.scope(budget, future).await,
                None => future.await,
            }
        };
        let work = async move {
            match caller {
                Some(deadline) => CALLER_DEADLINE.scope(deadline, work).await,
                None => work.await,
            }
        };
        #[cfg(feature = "python-bindings")]
        {
            match cancel {
                Some(cancel) => READ_CANCEL.scope(cancel, work).await,
                None => work.await,
            }
        }
        #[cfg(not(feature = "python-bindings"))]
        work.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> RequestContext {
        RequestContext {
            operation: "read".into(),
            protocol: crate::NFSVersion::NFSv3,
            request_id: None,
        }
    }
    fn busy() -> NfsError {
        NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_JUKEBOX)
    }

    #[tokio::test(start_paused = true)]
    async fn busy_retry_limit_preserves_original_status() {
        let started = Instant::now();
        let budget = Arc::new(Budget {
            started,
            deadline: started + Duration::from_secs(120),
            retries: AtomicU32::new(0),
        });
        BUDGET
            .scope(budget, async {
                let budget = current();
                for attempt in 0..MAX_RETRIES {
                    let before = Instant::now();
                    budget.backoff(context(), busy()).await.unwrap();
                    let maximum = BASE_MS.saturating_mul(1u64 << attempt).min(CAP_MS);
                    assert!(before.elapsed() >= Duration::from_millis(maximum / 2));
                    // Tokio timers may round a sampled delay to the next tick.
                    assert!(before.elapsed() <= Duration::from_millis(maximum + 1));
                }
                let NfsError::ServerBusy(error) =
                    budget.backoff(context(), busy()).await.unwrap_err()
                else {
                    panic!()
                };
                assert_eq!(error.retries, 16);
                assert_eq!(error.reason, RetryExhaustion::RetryLimit);
                assert!(error.source.is_server_busy());
                assert!(error.elapsed < Duration::from_millis(61200));
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn busy_nested_calls_share_deadline_and_retry_count() {
        scope(async {
            let outer = current();
            scope(async {
                assert!(Arc::ptr_eq(&outer, &current()));
                current().backoff(context(), busy()).await.unwrap();
            })
            .await;
            tokio::time::advance(MAX_ELAPSED).await;
            let NfsError::ServerBusy(error) =
                current().backoff(context(), busy()).await.unwrap_err()
            else {
                panic!()
            };
            assert_eq!(error.reason, RetryExhaustion::Deadline);
            assert_eq!(error.retries, 1);
            assert!(cap_deadline(Instant::now() + MAX_ELAPSED) <= Instant::now());
        })
        .await;
    }

    #[cfg(feature = "python-bindings")]
    #[tokio::test(start_paused = true)]
    async fn busy_caller_timeout_and_detached_work_share_the_shorter_deadline() {
        let work = with_timeout(Some(Duration::from_millis(50)), async {
            scope(async {
                let budget = current();
                let child =
                    tokio::spawn(inherit(
                        async move { budget.backoff(context(), busy()).await },
                    ));
                child.await.unwrap()
            })
            .await
        });
        let NfsError::ServerBusy(error) = work.await.unwrap_err() else {
            panic!()
        };
        assert_eq!(error.reason, RetryExhaustion::Deadline);
        assert_eq!(error.elapsed, Duration::from_millis(50));
    }

    #[cfg(feature = "python-bindings")]
    #[tokio::test(start_paused = true)]
    async fn busy_expired_caller_does_not_prevent_temporary_open_cleanup() {
        with_timeout(Some(Duration::from_millis(1)), async {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(cap_deadline(Instant::now() + MAX_ELAPSED) <= Instant::now());
            cleanup(scope(async {
                current().backoff(context(), busy()).await.unwrap();
                assert!(cap_deadline(Instant::now() + MAX_ELAPSED) > Instant::now());
            }))
            .await;
            assert!(cap_deadline(Instant::now() + MAX_ELAPSED) <= Instant::now());
        })
        .await;
    }

    #[cfg(feature = "python-bindings")]
    #[tokio::test(start_paused = true)]
    async fn busy_cancelled_python_read_stops_detached_backoff() {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (waiter, work) = read_waiter(
            true,
            scope(async {
                tokio::spawn(inherit(async {
                    let _ = entered_tx.send(());
                    current().backoff(context(), busy()).await
                }))
                .await
                .unwrap()
            }),
        );
        let task = tokio::spawn(work);
        entered_rx.await.unwrap();
        drop(waiter);
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert_eq!(
            error.request_transmission(),
            Some(crate::RequestTransmission::NotSent)
        );
    }
}
