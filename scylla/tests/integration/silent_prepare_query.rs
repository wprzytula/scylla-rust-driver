use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::unprepared::Statement;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
#[ntest::timeout(30000)]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_prepare_query_with_values() {
    setup_tracing();
    // unprepared query with non empty values should be prepared
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let s: Statement = Statement::from("INSERT INTO t (a) VALUES (?)");

        let drop_unprepared_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_unprepared_frame_rule]));

        tokio::select! {
            _res = session.query_unpaged(s, (0,)) => (),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => panic!("Rules did not work: no received response"),
        };

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[ntest::timeout(30000)]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_query_with_no_values() {
    setup_tracing();
    // unprepared query with empty values should not be prepared
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let s: Statement = Statement::from("INSERT INTO t (a) VALUES (1)");

        let drop_prepared_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_prepared_frame_rule]));

        tokio::select! {
            _res = session.query_unpaged(s, ()) => (),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => panic!("Rules did not work: no received response"),
        };

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
