//! Per-call, test-wheel-only control of the notification teardown race.
use super::{Pending, with_pending};
use pyo3::exceptions::{PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
struct State {
    sender_returned: bool,
    fence_entered: bool,
    released: bool,
}

#[derive(Default)]
pub(super) struct Pause {
    state: Mutex<State>,
    changed: Condvar,
}

impl Pause {
    pub(super) fn hold_sender(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sender_returned = true;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) fn enter_fence(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fence_entered = true;
        self.changed.notify_all();
    }
}

#[pyclass]
struct TestCompletionBarrier {
    pending: Arc<Pending>,
    pause: Arc<Pause>,
}

#[pymethods]
impl TestCompletionBarrier {
    fn wait_until_fenced(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let state = self
                .pause
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (state, _) = self
                .pause
                .changed
                .wait_timeout_while(state, Duration::from_secs(5), |s| {
                    !(s.sender_returned && s.fence_entered)
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.sender_returned && state.fence_entered {
                Ok(())
            } else {
                Err(PyTimeoutError::new_err("completion fence was not reached"))
            }
        })
    }

    fn release(&self) {
        self.pause
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .released = true;
        self.pause.changed.notify_all();
    }

    fn pending_count(&self) -> usize {
        *self
            .pending
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[pyfunction]
fn _completion_test_future<'py>(
    py: Python<'py>,
    outcome: &str,
) -> PyResult<(Bound<'py, PyAny>, TestCompletionBarrier)> {
    if !matches!(outcome, "success" | "error" | "cancel") {
        return Err(PyValueError::new_err("unknown completion test outcome"));
    }
    let outcome = outcome.to_string();
    let pause = Arc::new(Pause::default());
    let pending = Arc::new(Pending {
        pause: Some(pause.clone()),
        ..Pending::default()
    });
    let future = with_pending(
        py,
        async move {
            match outcome.as_str() {
                "error" => Err(PyValueError::new_err("injected bridge failure")),
                "cancel" => std::future::pending::<PyResult<u32>>().await,
                _ => Ok(42),
            }
        },
        pending.clone(),
    )?;
    Ok((future, TestCompletionBarrier { pending, pause }))
}

pub(in crate::python_adapter) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(_completion_test_future, module)?)
}
