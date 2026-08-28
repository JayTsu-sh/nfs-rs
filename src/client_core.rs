// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Lifecycle seam used by private language adapters.

use crate::{NfsError, Result};
use async_trait::async_trait;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const READY: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientLifecycle {
    Ready,
    Closing,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceKey(u64);

impl fmt::Display for ResourceKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[async_trait]
pub trait ClientDriver: fmt::Debug + Send + Sync + 'static {
    async fn close_resource(&self, key: ResourceKey) -> Result<()>;
    async fn umount(&self) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct ClientCloseReport {
    errors: Vec<Arc<NfsError>>,
}

impl ClientCloseReport {
    pub fn errors(&self) -> &[Arc<NfsError>] {
        &self.errors
    }
}

#[derive(Debug)]
pub struct ClientCore {
    driver: Arc<dyn ClientDriver>,
    lifecycle: AtomicU8,
    next_resource_key: AtomicU64,
    in_flight: AtomicU64,
    in_flight_notify: Notify,
    resources: Mutex<Vec<ResourceKey>>,
    owned_tasks: AtomicU64,
    owned_tasks_notify: Notify,
    close_state: Mutex<CloseState>,
    close_notify: Notify,
}

#[derive(Debug, Default)]
struct CloseState {
    started: bool,
    report: Option<Arc<ClientCloseReport>>,
}

impl ClientCore {
    pub fn new(driver: Arc<dyn ClientDriver>) -> Arc<Self> {
        Arc::new(Self {
            driver,
            lifecycle: AtomicU8::new(READY),
            next_resource_key: AtomicU64::new(1),
            in_flight: AtomicU64::new(0),
            in_flight_notify: Notify::new(),
            resources: Mutex::new(Vec::new()),
            owned_tasks: AtomicU64::new(0),
            owned_tasks_notify: Notify::new(),
            close_state: Mutex::new(CloseState::default()),
            close_notify: Notify::new(),
        })
    }

    pub fn lifecycle(&self) -> ClientLifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            READY => ClientLifecycle::Ready,
            CLOSING => ClientLifecycle::Closing,
            _ => ClientLifecycle::Closed,
        }
    }

    pub fn register_resource(&self) -> Result<ResourceKey> {
        if self.lifecycle.load(Ordering::Acquire) != READY {
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        let key = ResourceKey(self.next_resource_key.fetch_add(1, Ordering::Relaxed));
        let mut resources = self
            .resources
            .lock()
            .map_err(|_| NfsError::Rpc("client resource registry lock poisoned".to_string()))?;
        if self.lifecycle.load(Ordering::Acquire) != READY {
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        resources.push(key);
        Ok(key)
    }

    pub fn unregister_resource(&self, key: ResourceKey) -> Result<bool> {
        let mut resources = self
            .resources
            .lock()
            .map_err(|_| NfsError::Rpc("client resource registry lock poisoned".to_string()))?;
        let Some(position) = resources.iter().position(|candidate| *candidate == key) else {
            return Ok(false);
        };
        resources.remove(position);
        Ok(true)
    }

    pub fn resource_count(&self) -> usize {
        self.resources
            .lock()
            .map(|resources| resources.len())
            .unwrap_or_default()
    }

    pub fn begin_operation(self: &Arc<Self>) -> Result<OperationGuard> {
        if self.lifecycle.load(Ordering::Acquire) != READY {
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if self.lifecycle.load(Ordering::Acquire) != READY {
            self.finish_operation();
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        Ok(OperationGuard {
            core: Some(Arc::clone(self)),
        })
    }

    fn finish_operation(&self) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.in_flight_notify.notify_waiters();
        }
    }

    pub fn spawn_owned<F>(self: &Arc<Self>, future: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.lifecycle.load(Ordering::Acquire) != READY {
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        self.owned_tasks.fetch_add(1, Ordering::AcqRel);
        if self.lifecycle.load(Ordering::Acquire) != READY {
            self.finish_owned_task();
            return Err(NfsError::InvalidInput(
                "connected client is closing or closed".to_string(),
            ));
        }
        let core = Arc::clone(self);
        tokio::spawn(async move {
            let _guard = OwnedTaskGuard { core };
            future.await;
        });
        Ok(())
    }

    fn finish_owned_task(&self) {
        if self.owned_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.owned_tasks_notify.notify_waiters();
        }
    }

    pub fn owned_task_count(&self) -> u64 {
        self.owned_tasks.load(Ordering::Acquire)
    }

    pub async fn close(self: &Arc<Self>) -> Arc<ClientCloseReport> {
        let start_cleanup = self
            .close_state
            .lock()
            .map(|mut state| {
                if state.started {
                    false
                } else {
                    state.started = true;
                    true
                }
            })
            .unwrap_or(false);
        if start_cleanup {
            self.lifecycle.store(CLOSING, Ordering::Release);
            let core = Arc::clone(self);
            tokio::spawn(async move {
                core.run_close().await;
            });
        }

        loop {
            let notified = self.close_notify.notified();
            if let Some(report) = self
                .close_state
                .lock()
                .ok()
                .and_then(|state| state.report.clone())
            {
                return report;
            }
            notified.await;
        }
    }

    async fn run_close(&self) {
        while self.in_flight.load(Ordering::Acquire) != 0 {
            let notified = self.in_flight_notify.notified();
            if self.in_flight.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
        while self.owned_tasks.load(Ordering::Acquire) != 0 {
            let notified = self.owned_tasks_notify.notified();
            if self.owned_tasks.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
        let mut errors = Vec::new();
        let resources = self
            .resources
            .lock()
            .map(|mut resources| std::mem::take(&mut *resources))
            .unwrap_or_default();
        for key in resources {
            if let Err(error) = self.driver.close_resource(key).await {
                errors.push(Arc::new(error));
            }
        }
        if let Err(error) = self.driver.umount().await {
            errors.push(Arc::new(error));
        }
        self.lifecycle.store(CLOSED, Ordering::Release);
        let report = Arc::new(ClientCloseReport { errors });
        if let Ok(mut state) = self.close_state.lock() {
            state.report = Some(report);
        }
        self.close_notify.notify_waiters();
    }
}

#[derive(Debug)]
pub struct OperationGuard {
    core: Option<Arc<ClientCore>>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if let Some(core) = self.core.take() {
            core.finish_operation();
        }
    }
}

struct OwnedTaskGuard {
    core: Arc<ClientCore>,
}

impl Drop for OwnedTaskGuard {
    fn drop(&mut self) {
        self.core.finish_owned_task();
    }
}
