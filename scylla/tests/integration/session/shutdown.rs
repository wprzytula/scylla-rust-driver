use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::StreamExt as _;
use scylla::client::session::Session;
use scylla::errors::{ExecutionError, NextPageError, NextRowError};
use scylla::statement::Statement;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

async fn create_table(session: &Session, ks: &str) {
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH \
             REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (pk int, ck int, PRIMARY KEY (pk, ck))"
        ))
        .await
        .unwrap();
}

/// A request that runs during a shutdown either completes, or fails because
/// the session rejected it. The shutdown does not break admitted requests.
#[tokio::test]
async fn test_shutdown_under_load() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();
    create_table(&session, &ks).await;
    let insert = session
        .prepare(format!("INSERT INTO {ks}.t (pk, ck) VALUES (?, ?)"))
        .await
        .unwrap();

    let writers: Vec<_> = (0..8)
        .map(|pk: i32| {
            let session = Arc::clone(&session);
            let insert = insert.clone();
            tokio::spawn(async move {
                let mut completed = 0;
                for ck in 0.. {
                    match session.execute_unpaged(&insert, (pk, ck)).await {
                        Ok(_) => completed += 1,
                        Err(ExecutionError::SessionShutDown(_)) => break,
                        Err(err) => panic!("A request failed during the shutdown: {err}"),
                    }
                }
                completed
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(200)).await;
    tokio::time::timeout(SHUTDOWN_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    for writer in writers {
        assert!(writer.await.unwrap() > 0);
    }
    let cluster_state = session.get_cluster_state();
    assert!(
        cluster_state
            .get_nodes_info()
            .iter()
            .all(|node| !node.is_connected())
    );
}

/// A pager that the user does not read does not delay the shutdown.
/// After the shutdown, it fails when it must fetch a new page.
#[tokio::test]
async fn test_shutdown_with_unread_pager() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    create_table(&session, &ks).await;

    const ROWS: i32 = 10;
    for ck in 0..ROWS {
        session
            .query_unpaged(format!("INSERT INTO {ks}.t (pk, ck) VALUES (0, ?)"), (ck,))
            .await
            .unwrap();
    }

    let mut select = Statement::new(format!("SELECT ck FROM {ks}.t WHERE pk = 0"));
    select.set_page_size(1);
    let mut rows = session
        .query_iter(select, ())
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    tokio::time::timeout(SHUTDOWN_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    let mut fetched_rows = 0;
    let error = loop {
        match rows.next().await {
            Some(Ok(_)) => fetched_rows += 1,
            Some(Err(error)) => break error,
            None => panic!("The pager returned all rows after the shutdown"),
        }
    };
    assert!((1..ROWS).contains(&fetched_rows));
    assert_matches!(
        error,
        NextRowError::NextPageError(NextPageError::SessionShutDown(_))
    );
}
