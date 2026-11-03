//! Keep bridge notifications alive until they have left the Python API.
//!
//! asyncio may run a completion callback while call_soon_threadsafe's sender
//! is still reacquiring the GIL after its socket write. A done callback,
//! registered before the awaiting task, fences that sender before user code
//! can exit the interpreter. It also runs after the bridge's cancellation
//! callback, so cancelled futures can settle without blocking cancellation.
use pyo3::prelude::*;
use pyo3_async_runtimes::{TaskLocals, generic};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};

tokio::task_local! {
    static WORK: Arc<Pending>;
    static LOCALS: TaskLocals;
}

#[derive(Default)]
struct Pending {
    // This is a Condvar predicate, not an independent statistics counter.
    // It must share the waiter's mutex so the zero transition and sleeping
    // cannot lose a wakeup. This is the intentional exception to the general
    // shared-counter/atomics rule; all critical sections remain short.
    count: Mutex<usize>,
    drained: Condvar,
    #[cfg(feature = "python-test-support")]
    pause: Option<Arc<test_support::Pause>>,
}

struct WorkGuard(Arc<Pending>);

impl Pending {
    fn enter(self: &Arc<Self>) -> WorkGuard {
        *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        WorkGuard(self.clone())
    }

    fn wait(&self) {
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *count != 0 {
            count = self
                .drained
                .wait(count)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        let mut count = self
            .0
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *count -= 1;
        if *count == 0 {
            self.0.drained.notify_all();
        }
    }
}

struct Runtime;

impl generic::Runtime for Runtime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(future: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // All bridge spawning is inside the scope installed by future_into_py
        // or an inherited task scope.
        let pending = WORK.with(Arc::clone);
        let guard = pending.enter();
        pyo3_async_runtimes::tokio::get_runtime().spawn(WORK.scope(pending, async move {
            let _guard = guard;
            future.await;
        }))
    }

    fn spawn_blocking<F>(work: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let pending = WORK.with(Arc::clone);
        let guard = pending.enter();
        pyo3_async_runtimes::tokio::get_runtime().spawn_blocking(move || {
            let _guard = guard;
            work();
            #[cfg(feature = "python-test-support")]
            if let Some(pause) = &pending.pause {
                pause.hold_sender();
            }
        })
    }
}

impl generic::ContextExt for Runtime {
    fn scope<F, R>(locals: TaskLocals, future: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        Box::pin(LOCALS.scope(locals, future))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        LOCALS.try_with(Clone::clone).ok()
    }
}

#[pyclass]
struct CompletionFence(Arc<Pending>);

#[pymethods]
impl CompletionFence {
    fn __call__(&self, py: Python<'_>, _future: &Bound<'_, PyAny>) {
        #[cfg(feature = "python-test-support")]
        if let Some(pause) = &self.0.pause {
            pause.enter_fence();
        }
        // The sender must be allowed to reacquire and release the GIL.
        py.detach(|| self.0.wait());
    }
}

pub(super) fn future_into_py<F, T>(py: Python<'_>, future: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let pending = Arc::new(Pending::default());
    with_pending(py, future, pending)
}

fn with_pending<F, T>(
    py: Python<'_>,
    future: F,
    pending: Arc<Pending>,
) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let result = WORK.sync_scope(pending.clone(), || {
        generic::future_into_py::<Runtime, _, _>(py, future)
    })?;
    // generic::future_into_py registers its cancellation callback first.
    // Register the fence before returning the Future to its awaiting task.
    result.call_method1(
        "add_done_callback",
        (Py::new(py, CompletionFence(pending))?,),
    )?;
    Ok(result)
}

#[cfg(feature = "python-test-support")]
pub(super) mod test_support;
