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

use crate::{NFSVersion, NfsError, OperationOutcome, RecoveryAction, Result};
use async_trait::async_trait;
use std::collections::VecDeque;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreOperation {
    pub name: String,
    pub safe_path: Option<String>,
}

#[async_trait]
pub trait ClientDriver: fmt::Debug + Send + Sync + 'static {
    async fn execute(&self, operation: CoreOperation) -> Result<()>;
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
    in_flight: DrainCounter,
    resources: Mutex<Vec<ResourceKey>>,
    owned_tasks: DrainCounter,
    recovery_events: Mutex<RecoveryEventQueue>,
    close_state: Mutex<CloseState>,
    close_notify: Notify,
    lifecycle_notify: Notify,
}

#[derive(Debug, Default)]
struct CloseState {
    started: bool,
    report: Option<Arc<ClientCloseReport>>,
}

#[derive(Debug, Default)]
struct DrainCounter {
    count: AtomicU64,
    notify: Notify,
}

impl DrainCounter {
    fn increment(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }

    fn decrement(&self) {
        if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.notify.notify_waiters();
        }
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    async fn wait_for_zero(&self) {
        while self.count() != 0 {
            let notified = self.notify.notified();
            if self.count() != 0 {
                notified.await;
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreRecoveryEvent {
    pub operation: String,
    pub safe_path: Option<String>,
    pub protocol: NFSVersion,
    pub outcome: OperationOutcome,
    pub recovery: RecoveryAction,
    pub completed_bytes: Option<u64>,
    pub message: String,
}

#[derive(Debug)]
struct RecoveryEventQueue {
    capacity: usize,
    dropped: u64,
    events: VecDeque<CoreRecoveryEvent>,
}

impl ClientCore {
    pub fn new(driver: Arc<dyn ClientDriver>) -> Arc<Self> {
        Self::build(driver, 256)
    }

    pub fn with_recovery_event_capacity(
        driver: Arc<dyn ClientDriver>,
        recovery_event_capacity: usize,
    ) -> Result<Arc<Self>> {
        if recovery_event_capacity == 0 {
            return Err(NfsError::InvalidInput(
                "recovery-event capacity must be positive".to_string(),
            ));
        }
        Ok(Self::build(driver, recovery_event_capacity))
    }

    fn build(driver: Arc<dyn ClientDriver>, recovery_event_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            driver,
            lifecycle: AtomicU8::new(READY),
            next_resource_key: AtomicU64::new(1),
            in_flight: DrainCounter::default(),
            resources: Mutex::new(Vec::new()),
            owned_tasks: DrainCounter::default(),
            recovery_events: Mutex::new(RecoveryEventQueue {
                capacity: recovery_event_capacity,
                dropped: 0,
                events: VecDeque::new(),
            }),
            close_state: Mutex::new(CloseState::default()),
            close_notify: Notify::new(),
            lifecycle_notify: Notify::new(),
        })
    }

    fn ensure_ready(&self) -> Result<()> {
        if self.lifecycle.load(Ordering::Acquire) == READY {
            Ok(())
        } else {
            Err(NfsError::ClientClosed(
                "connected client is closing or closed".to_string(),
            ))
        }
    }

    pub fn lifecycle(&self) -> ClientLifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            READY => ClientLifecycle::Ready,
            CLOSING => ClientLifecycle::Closing,
            _ => ClientLifecycle::Closed,
        }
    }

    pub async fn wait_for_lifecycle(&self, expected: ClientLifecycle) {
        while self.lifecycle() != expected {
            let notified = self.lifecycle_notify.notified();
            if self.lifecycle() != expected {
                notified.await;
            }
        }
    }

    pub async fn execute(self: &Arc<Self>, operation: CoreOperation) -> Result<()> {
        let _operation = self.begin_operation()?;
        self.driver.execute(operation).await
    }

    pub fn record_recovery_event(&self, event: CoreRecoveryEvent) -> Result<()> {
        let mut queue = self
            .recovery_events
            .lock()
            .map_err(|_| NfsError::Rpc("recovery-event queue lock poisoned".to_string()))?;
        if queue.events.len() == queue.capacity {
            queue.events.pop_front();
            queue.dropped = queue.dropped.saturating_add(1);
        }
        queue.events.push_back(event);
        Ok(())
    }

    pub fn recovery_events(&self) -> Result<Vec<CoreRecoveryEvent>> {
        self.recovery_events
            .lock()
            .map(|queue| queue.events.iter().cloned().collect())
            .map_err(|_| NfsError::Rpc("recovery-event queue lock poisoned".to_string()))
    }

    pub fn drain_recovery_events(&self) -> Result<Vec<CoreRecoveryEvent>> {
        self.recovery_events
            .lock()
            .map(|mut queue| queue.events.drain(..).collect())
            .map_err(|_| NfsError::Rpc("recovery-event queue lock poisoned".to_string()))
    }

    pub fn dropped_recovery_event_count(&self) -> Result<u64> {
        self.recovery_events
            .lock()
            .map(|queue| queue.dropped)
            .map_err(|_| NfsError::Rpc("recovery-event queue lock poisoned".to_string()))
    }

    pub fn register_resource(&self) -> Result<ResourceKey> {
        let key = self.allocate_resource_key()?;
        self.publish_resource(key)?;
        Ok(key)
    }

    pub fn allocate_resource_key(&self) -> Result<ResourceKey> {
        self.ensure_ready()?;
        Ok(ResourceKey(
            self.next_resource_key.fetch_add(1, Ordering::Relaxed),
        ))
    }

    pub fn publish_resource(&self, key: ResourceKey) -> Result<()> {
        let mut resources = self
            .resources
            .lock()
            .map_err(|_| NfsError::Rpc("client resource registry lock poisoned".to_string()))?;
        self.ensure_ready()?;
        resources.push(key);
        Ok(())
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
        self.ensure_ready()?;
        self.in_flight.increment();
        if let Err(error) = self.ensure_ready() {
            self.finish_operation();
            return Err(error);
        }
        Ok(OperationGuard {
            core: Some(Arc::clone(self)),
        })
    }

    fn finish_operation(&self) {
        self.in_flight.decrement();
    }

    pub fn spawn_owned<F>(self: &Arc<Self>, future: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.ensure_ready()?;
        self.owned_tasks.increment();
        if let Err(error) = self.ensure_ready() {
            self.finish_owned_task();
            return Err(error);
        }
        let core = Arc::clone(self);
        tokio::spawn(async move {
            let _guard = OwnedTaskGuard { core };
            future.await;
        });
        Ok(())
    }

    fn finish_owned_task(&self) {
        self.owned_tasks.decrement();
    }

    pub fn owned_task_count(&self) -> u64 {
        self.owned_tasks.count()
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
            self.lifecycle_notify.notify_waiters();
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
        self.in_flight.wait_for_zero().await;
        self.owned_tasks.wait_for_zero().await;
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
        self.lifecycle_notify.notify_waiters();
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
