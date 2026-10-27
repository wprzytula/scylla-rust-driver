//! Tests of [`Session::shutdown`], on a dry-mode proxy.

use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::StreamExt;
use scylla_proxy::{
    Condition, Proxy, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, RunningProxy,
};

use super::Session;
use crate::client::session_builder::SessionBuilder;
use crate::errors::{
    ConnectionPoolError, ExecutionError, MetadataError, NextPageError, NextRowError,
    PagerExecutionError, PrepareError, UseKeyspaceError,
};
use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use crate::test_utils::{dry_mode_handshake_rules, setup_tracing};

/// The proxy delays the response to this query by `SLOW_RESPONSE_DELAY`.
const SLOW_QUERY: &str = "SELECT a FROM ks.slow";
const SLOW_RESPONSE_DELAY: Duration = Duration::from_millis(300);

/// The proxy answers this query with pages that never end.
const PAGED_QUERY: &str = "SELECT a FROM ks.paged";

/// Bounds the waits for the shutdown in tests where the shutdown must not wait.
const TEST_TIMEOUT: Duration = Duration::from_secs(1);

fn rows_response(frame: RequestFrame, paging_state: Option<&[u8]>) -> ResponseFrame {
    let col_specs = [ColumnSpec::borrowed(
        "a",
        ColumnType::Native(NativeType::Int),
        TableSpec::borrowed("ks", "t"),
    )];
    ResponseFrame::forged_rows(frame.params, &col_specs, [(1_i32,)], paging_state).unwrap()
}

fn query_rule(query: &str, reaction: RequestReaction) -> RequestRule {
    RequestRule(
        Condition::RequestOpcode(RequestOpcode::Query).and(Condition::BodyContainsCaseSensitive(
            query.as_bytes().into(),
        )),
        reaction,
    )
}

/// Starts a dry-mode proxy, and connects a session to it.
async fn connect() -> (Arc<Session>, RunningProxy) {
    let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

    let mut rules = dry_mode_handshake_rules();
    rules.extend([
        query_rule(
            SLOW_QUERY,
            RequestReaction::forge_response_with_delay(
                SLOW_RESPONSE_DELAY,
                Arc::new(|frame: RequestFrame| rows_response(frame, None)),
            ),
        ),
        query_rule(
            PAGED_QUERY,
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                rows_response(frame, Some(b"next page"))
            })),
        ),
        // For example, the metadata queries of the control connection.
        RequestRule(
            Condition::any([
                Condition::RequestOpcode(RequestOpcode::Query),
                Condition::RequestOpcode(RequestOpcode::Prepare),
            ]),
            RequestReaction::forge().server_error(),
        ),
    ]);

    let proxy = Proxy::builder()
        .with_node(
            scylla_proxy::Node::builder()
                .proxy_address(proxy_addr)
                .request_rules(rules)
                .build_dry_mode(),
        )
        .build()
        .run()
        .await
        .unwrap();

    let session = SessionBuilder::new()
        .known_node_addr(proxy_addr)
        .build()
        .await
        .unwrap();

    (Arc::new(session), proxy)
}

#[tokio::test]
async fn shutdown_waits_for_requests_in_flight() {
    setup_tracing();
    let (session, _proxy) = connect().await;

    let mut slow_request = tokio::spawn({
        let session = Arc::clone(&session);
        async move { session.query_unpaged(SLOW_QUERY, ()).await }
    });
    while session.in_flight.count() == 0 {
        tokio::task::yield_now().await;
    }

    // The first poll closes the session to new requests.
    let mut shutdown = pin!(session.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    assert_matches!(
        session.query_unpaged(SLOW_QUERY, ()).await,
        Err(ExecutionError::SessionShutDown(_))
    );

    // The admitted request completes normally, before the shutdown does.
    tokio::select! {
        biased;
        result = &mut slow_request => {
            result.unwrap().unwrap();
        }
        () = &mut shutdown => panic!("the shutdown did not wait for a request in flight"),
    }
    tokio::time::timeout(TEST_TIMEOUT, shutdown).await.unwrap();
}

#[tokio::test]
async fn shutdown_closes_connections_and_rejects_requests() {
    setup_tracing();
    let (session, _proxy) = connect().await;
    let cluster_state = session.get_cluster_state();
    assert!(cluster_state.get_nodes_info()[0].is_connected());
    assert!(session.background_tasks.len() > 0);

    // Concurrent calls are allowed.
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(session.shutdown(), session.shutdown())
    })
    .await
    .unwrap();

    // All background tasks are gone, so all sockets are closed.
    assert_eq!(session.background_tasks.len(), 0);
    // The pools release their connections, also in an old ClusterState.
    for node in cluster_state.get_nodes_info() {
        assert!(!node.is_connected());
        assert_matches!(
            node.get_working_connections().err(),
            Some(ConnectionPoolError::Closed)
        );
    }

    assert_matches!(
        session.query_unpaged(SLOW_QUERY, ()).await,
        Err(ExecutionError::SessionShutDown(_))
    );
    assert_matches!(
        session.query_iter(PAGED_QUERY, ()).await,
        Err(PagerExecutionError::SessionShutDown(_))
    );
    assert_matches!(
        session.prepare(SLOW_QUERY).await,
        Err(PrepareError::SessionShutDown(_))
    );
    assert_matches!(
        session.use_keyspace("ks", false).await,
        Err(UseKeyspaceError::SessionShutDown(_))
    );
    assert_matches!(
        session.refresh_metadata().await,
        Err(MetadataError::SessionShutDown(_))
    );

    // A later call returns at once.
    assert!(futures::poll!(pin!(session.shutdown())).is_ready());
}

#[tokio::test]
async fn unread_pager_does_not_delay_shutdown() {
    setup_tracing();
    let (session, _proxy) = connect().await;
    let mut rows = session
        .query_iter(PAGED_QUERY, ())
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    tokio::time::timeout(TEST_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    // The rows fetched before the shutdown stay available.
    // Then the pager must fetch a new page, which the session rejects.
    let mut fetched_rows = 0;
    let error = loop {
        match rows.next().await.unwrap() {
            Ok(_) => fetched_rows += 1,
            Err(error) => break error,
        }
    };
    assert!(fetched_rows >= 1);
    assert_matches!(
        error,
        NextRowError::NextPageError(NextPageError::SessionShutDown(_))
    );
}
