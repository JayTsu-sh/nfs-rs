use async_trait::async_trait;
use nfs_rs::client_core::{ClientCore, ClientDriver, ClientLifecycle, ResourceKey};
use nfs_rs::{
    NFSVersion, NfsError, OperationClass, OperationOutcome, RecoveryAction, RequestContext,
    RequestTransmission, Result,
};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct RecordingDriver {
    events: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug, Default)]
struct FailingDriver {
    events: Mutex<Vec<String>>,
}

#[async_trait]
impl ClientDriver for FailingDriver {
    async fn close_resource(&self, key: ResourceKey) -> Result<()> {
        self.events.lock().unwrap().push(format!("close:{key}"));
        if key.to_string() == "1" {
            Err(nfs_rs::NfsError::Rpc("first close failed".to_string()))
        } else {
            Ok(())
        }
    }

    async fn umount(&self) -> Result<()> {
        self.events.lock().unwrap().push("umount".to_string());
        Err(nfs_rs::NfsError::Rpc("umount failed".to_string()))
    }
}

#[async_trait]
impl ClientDriver for RecordingDriver {
    async fn close_resource(&self, key: ResourceKey) -> Result<()> {
        self.events.lock().unwrap().push(format!("close:{key}"));
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        self.events.lock().unwrap().push("umount".to_string());
        Ok(())
    }
}

#[tokio::test]
async fn connected_client_closes_resources_in_registration_order_before_umount() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    let first = core.register_resource().expect("client is ready");
    let second = core.register_resource().expect("client is ready");

    let report = core.close().await;

    assert!(report.errors().is_empty());
    assert_eq!(core.lifecycle(), ClientLifecycle::Closed);
    assert_eq!(
        *driver.events.lock().unwrap(),
        vec![
            format!("close:{first}"),
            format!("close:{second}"),
            "umount".to_string(),
        ]
    );
}

#[tokio::test]
async fn concurrent_close_waiters_share_one_cleanup_and_terminal_report() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    core.register_resource().expect("client is ready");

    let (first, second) = tokio::join!(core.close(), core.close());

    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(
        *driver.events.lock().unwrap(),
        vec!["close:1".to_string(), "umount".to_string()]
    );
    assert!(Arc::ptr_eq(&first, &core.close().await));
}

#[tokio::test]
async fn close_rejects_new_work_and_waits_for_in_flight_work() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    core.register_resource().expect("client is ready");
    let in_flight = core.begin_operation().expect("client is ready");

    let close_task = {
        let core = core.clone();
        tokio::spawn(async move { core.close().await })
    };
    core.wait_for_lifecycle(ClientLifecycle::Closing).await;

    assert_eq!(core.lifecycle(), ClientLifecycle::Closing);
    assert!(core.begin_operation().is_err());
    assert!(core.register_resource().is_err());
    assert!(driver.events.lock().unwrap().is_empty());

    drop(in_flight);
    assert!(close_task.await.unwrap().errors().is_empty());
    assert_eq!(core.lifecycle(), ClientLifecycle::Closed);
}

#[tokio::test]
async fn core_owned_work_survives_the_callers_waiter_and_gates_close() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_for_task = entered.clone();
    let release_for_task = release.clone();
    let events = driver.events.clone();

    core.spawn_owned(async move {
        entered_for_task.notify_one();
        release_for_task.notified().await;
        events.lock().unwrap().push("owned-work".to_string());
    })
    .expect("client is ready");
    entered.notified().await;

    let close_task = {
        let core = core.clone();
        tokio::spawn(async move { core.close().await })
    };
    core.wait_for_lifecycle(ClientLifecycle::Closing).await;
    assert!(driver.events.lock().unwrap().is_empty());

    release.notify_one();
    assert!(close_task.await.unwrap().errors().is_empty());
    assert_eq!(core.owned_task_count(), 0);
    assert_eq!(
        *driver.events.lock().unwrap(),
        vec!["owned-work".to_string(), "umount".to_string()]
    );
}

#[tokio::test]
async fn explicitly_unregistered_resource_is_not_closed_again_by_client_close() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    let first = core.register_resource().expect("client is ready");
    core.register_resource().expect("client is ready");

    assert!(
        core.unregister_resource(first)
            .expect("registry is available")
    );
    assert_eq!(core.resource_count(), 1);
    core.close().await;

    assert_eq!(
        *driver.events.lock().unwrap(),
        vec!["close:2".to_string(), "umount".to_string()]
    );
    assert_eq!(core.resource_count(), 0);
}

#[tokio::test]
async fn cleanup_continues_after_resource_errors_and_reports_them_in_order() {
    let driver = Arc::new(FailingDriver::default());
    let core = ClientCore::new(driver.clone());
    core.register_resource().expect("client is ready");
    core.register_resource().expect("client is ready");

    let report = core.close().await;

    assert_eq!(
        *driver.events.lock().unwrap(),
        vec![
            "close:1".to_string(),
            "close:2".to_string(),
            "umount".to_string()
        ]
    );
    assert_eq!(report.errors().len(), 2);
    assert!(report.errors()[0].to_string().contains("first close"));
    assert!(report.errors()[1].to_string().contains("umount"));
}

#[tokio::test]
async fn cancelling_a_close_waiter_does_not_cancel_core_owned_cleanup() {
    let driver = Arc::new(RecordingDriver::default());
    let core = ClientCore::new(driver.clone());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_for_task = entered.clone();
    let release_for_task = release.clone();
    core.spawn_owned(async move {
        entered_for_task.notify_one();
        release_for_task.notified().await;
    })
    .expect("client is ready");
    entered.notified().await;

    let waiter = {
        let core = core.clone();
        tokio::spawn(async move { core.close().await })
    };
    core.wait_for_lifecycle(ClientLifecycle::Closing).await;
    waiter.abort();
    let _ = waiter.await;
    release.notify_one();

    let report = core.close().await;
    assert!(report.errors().is_empty());
    assert_eq!(core.lifecycle(), ClientLifecycle::Closed);
    assert_eq!(*driver.events.lock().unwrap(), vec!["umount".to_string()]);
}

#[tokio::test]
async fn injected_operation_has_explicit_barriers_and_gates_close() {
    let core = ClientCore::new(Arc::new(RecordingDriver::default()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let operation = {
        let core = core.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            core.execute(async move {
                entered.notify_one();
                release.notified().await;
                Err::<(), _>(NfsError::before_send_failure(
                    OperationClass::ReadOnly,
                    RequestContext {
                        operation: "injected".to_string(),
                        protocol: NFSVersion::NFSv3,
                        request_id: None,
                    },
                    None,
                    NfsError::Rpc("injected failure".to_string()),
                ))
            })
            .await
        })
    };
    entered.notified().await;
    let close = {
        let core = core.clone();
        tokio::spawn(async move { core.close().await })
    };
    core.wait_for_lifecycle(ClientLifecycle::Closing).await;
    assert!(!close.is_finished());

    release.notify_one();
    let error = operation.await.unwrap().unwrap_err();
    assert_eq!(
        error.request_transmission(),
        Some(RequestTransmission::NotSent)
    );
    assert!(close.await.unwrap().errors().is_empty());
}

fn recovery_event(operation: &str) -> nfs_rs::client_core::CoreRecoveryEvent {
    nfs_rs::client_core::CoreRecoveryEvent {
        operation: operation.to_string(),
        safe_path: Some("/safe/path".to_string()),
        protocol: NFSVersion::NFSv3,
        outcome: OperationOutcome::DefiniteFailure,
        recovery: RecoveryAction::Retry,
        completed_bytes: None,
        message: "retry operation".to_string(),
    }
}

#[tokio::test]
async fn recovery_event_queue_is_bounded_observable_and_atomically_drained() {
    let core =
        ClientCore::with_recovery_event_capacity(Arc::new(RecordingDriver::default()), 2).unwrap();
    core.record_recovery_event(recovery_event("one")).unwrap();
    core.record_recovery_event(recovery_event("two")).unwrap();
    core.record_recovery_event(recovery_event("three")).unwrap();

    assert_eq!(core.dropped_recovery_event_count().unwrap(), 1);
    assert_eq!(
        core.recovery_events()
            .unwrap()
            .iter()
            .map(|event| event.operation.as_str())
            .collect::<Vec<_>>(),
        vec!["two", "three"]
    );
    assert_eq!(core.drain_recovery_events().unwrap().len(), 2);
    assert!(core.recovery_events().unwrap().is_empty());
}
