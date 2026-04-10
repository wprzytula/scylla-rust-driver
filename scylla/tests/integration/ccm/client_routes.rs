//! Integration tests for custom routing via `system.client_routes`.
//!
//! These tests require a CCM cluster and exercise the full client-routes flow:
//! driver →  NLB →  proxy →  Scylla, with `system.client_routes` providing
//! address translation rules.
//!
//! Each test uses per-node CQL-aware proxies with feedback channels to verify:
//! 1. The driver opens connections to ALL nodes
//! 2. The driver can query ALL nodes
//! 3. The driver connects through NLBs (address translation works)
//!
//! Proxy feedback alone proves all 3 requirements: if a proxy sees CQL traffic,
//! it necessarily went through the NLB (since the driver only knows NLB
//! addresses from `client_routes` and real node addresses, but not proxy addresses).

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::ccm::lib::client_routes::{
    ClientRoutesCluster, FeedbackItem, drain_feedback, run_client_routes_test,
};
use crate::ccm::lib::cluster::ClusterOptions;
use crate::ccm::lib::node::NodeId;
use crate::utils::{setup_tracing, unique_keyspace_name};

use scylla::client::session::Session;
use tracing::info;

/// ScyllaDB version that supports the `system.client_routes` table and the
/// `POST /v2/client-routes` REST API required for client-routes testing.
const CLIENT_ROUTES_VERSION: &str = "release:2026.1.0";

/// Number of queries per test phase. Must be large enough to statistically
/// hit all nodes via random-replica token-aware routing.
const QUERIES_PER_PHASE: i32 = 100;

/// Timeout for waiting for the driver to open connections to all proxy nodes.
/// Must be generous enough for the driver to discover new/restarted nodes,
/// but short enough to fail promptly if the driver has a bug (e.g.,
/// `Untranslatable` marking prevents address translation for a node).
const CONNECTION_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Cluster option factories
// ---------------------------------------------------------------------------

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "client_routes_3_nodes".to_string(),
        version: CLIENT_ROUTES_VERSION.to_string(),
        nodes_per_dc: vec![3],
        ..ClusterOptions::default()
    }
}

fn cluster_2dc_2_2() -> ClusterOptions {
    ClusterOptions {
        name: "client_routes_2dc".to_string(),
        version: CLIENT_ROUTES_VERSION.to_string(),
        nodes_per_dc: vec![2, 2],
        ..ClusterOptions::default()
    }
}

// ---------------------------------------------------------------------------
// Helper: create keyspace + table and run N queries, returning the session
// ---------------------------------------------------------------------------

async fn create_test_schema(session: &Session, ks_name: &str, rf: &str) {
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} \
                 WITH replication = {{'class': 'NetworkTopologyStrategy', {}}}",
                ks_name, rf
            ),
            &[],
        )
        .await
        .expect("Failed to create keyspace");

    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.data (id int PRIMARY KEY, value text)",
                ks_name
            ),
            &[],
        )
        .await
        .expect("Failed to create table");
}

async fn run_queries(session: &Session, ks_name: &str, count: i32) {
    for i in 0..count {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {}.data (id, value) VALUES ({}, 'v{}')",
                    ks_name, i, i
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("Query {} failed: {}", i, e));
    }
}

// ---------------------------------------------------------------------------
// Test 1: Basic connectivity (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// Connect via NLBs using client routes, create a keyspace/table, run 100 queries.
/// Assert: total feedback == 100, each node >= 1.
async fn basic_connectivity(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'replication_factor': 3").await;

    // Set up feedback AFTER schema creation (schema queries would pollute counts).
    let mut rxs = plc.setup_query_feedback();

    run_queries(&session, &ks, QUERIES_PER_PHASE).await;

    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Feedback: per_node={:?}, total={}", per_node, total);

    assert_eq!(
        total, QUERIES_PER_PHASE as usize,
        "Total feedback ({}) must equal queries performed ({})",
        total, QUERIES_PER_PHASE
    );
    for (&node_id, &count) in &per_node {
        assert!(
            count >= 1,
            "Node {} received 0 queries — driver didn't reach all nodes",
            node_id
        );
    }
}

#[tokio::test]
async fn test_client_routes_basic_connectivity() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, basic_connectivity).await;
}

// ---------------------------------------------------------------------------
// Test 2: Node stop/resume (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// Phase 1: all 3 nodes running, N queries →  total == N, all 3 >= 1.
/// Phase 2: stop node 1, N queries →  total == N, node 1 == 0, nodes 2+3 >= 1.
/// Phase 3: restart node 1, N queries →  total == N, all 3 >= 1.
async fn node_stop_resume(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'replication_factor': 3").await;

    // --- Phase 1: all nodes running ---
    info!("=== Phase 1: all nodes running ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 1: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(count >= 1, "Phase 1: node {} got 0 queries", node_id);
    }

    // --- Stop node 1 ---
    info!("Stopping node 1...");
    {
        let node = plc
            .cluster_mut()
            .nodes_mut()
            .get_mut_by_id(1)
            .expect("Node 1 not found");
        node.stop(None).await.expect("Failed to stop node 1");
    }
    // Tear down the proxy chain for node 1 so the driver can't route
    // queries through the dead NLB/proxy. Also re-posts routes without
    // node 1.
    plc.stop_node_chain(1)
        .await
        .expect("Failed to stop proxy chain for node 1");

    // --- Phase 2: node 1 down ---
    info!("=== Phase 2: node 1 down ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 2: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    assert_eq!(
        *per_node.get(&1).unwrap_or(&0),
        0,
        "Phase 2: node 1 should receive 0 queries (it's stopped)"
    );
    for (&node_id, &count) in &per_node {
        if node_id != 1 {
            assert!(count >= 1, "Phase 2: node {} got 0 queries", node_id);
        }
    }

    // --- Restart node 1 ---
    info!("Restarting node 1...");
    {
        let node = plc
            .cluster_mut()
            .nodes_mut()
            .get_mut_by_id(1)
            .expect("Node 1 not found");
        node.start(None).await.expect("Failed to restart node 1");
    }
    // Rebuild the proxy chain for node 1 (the old proxy worker died when
    // the node was stopped) and re-post routes with the new NLB port.
    plc.restart_node_chain(1)
        .await
        .expect("Failed to restart proxy chain for node 1");
    // Wait for the driver to discover the restarted node and open a
    // connection through the new proxy chain.
    plc.wait_for_connections_to_node(1, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not reconnect to restarted node 1");

    // --- Phase 3: all nodes running again ---
    info!("=== Phase 3: all nodes running again ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 3: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(count >= 1, "Phase 3: node {} got 0 queries", node_id);
    }
}

#[tokio::test]
async fn test_client_routes_node_stop_resume() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, node_stop_resume).await;
}

// ---------------------------------------------------------------------------
// Test 3: Multi-DC basic (2 DCs, 2+2 nodes)
// ---------------------------------------------------------------------------

/// Connect via client routes with 2 DCs (different connection IDs), run 100
/// queries. Assert: total == 100, each of the 4 nodes >= 1.
async fn multi_dc_basic(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    // Wait for the driver to open connections to all proxy nodes.
    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'dc1': 2, 'dc2': 2").await;

    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;

    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Feedback: per_node={:?}, total={}", per_node, total);

    assert_eq!(total, QUERIES_PER_PHASE as usize);
    let active_nodes = plc.active_node_ids();
    assert_eq!(active_nodes.len(), 4, "Expected 4 active nodes");
    for &node_id in &active_nodes {
        let count = *per_node.get(&node_id).unwrap_or(&0);
        assert!(
            count >= 1,
            "Node {} received 0 queries — driver didn't reach all nodes across DCs",
            node_id
        );
    }
}

#[tokio::test]
async fn test_client_routes_multi_dc_basic() {
    setup_tracing();
    run_client_routes_test(cluster_2dc_2_2, multi_dc_basic).await;
}

// ---------------------------------------------------------------------------
// Test 4: Multi-DC topology change (2 DCs, decommission + add)
// ---------------------------------------------------------------------------

/// Phase 1: all 4 nodes (2+2) →  N queries →  total == N, all 4 >= 1.
/// Phase 2: decommission last node in DC2 →  M queries →  total == M,
///          decommissioned node == 0, other 3 >= 1.
/// Phase 3: add new node to DC2 →  N queries →  total == N, all 4 >= 1
///          (including new node).
async fn multi_dc_topology_change(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    // Wait for the driver to open connections to all proxy nodes.
    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'dc1': 1, 'dc2': 1").await;

    // Identify nodes. In a 2+2 setup, CCM node IDs are 1,2 (DC1) and 3,4 (DC2).
    let initial_nodes = plc.active_node_ids();
    assert_eq!(initial_nodes.len(), 4, "Expected 4 initial nodes");
    info!("Initial nodes: {:?}", initial_nodes);

    // The node to decommission: the highest ID in DC2 (should be node 4).
    let node_to_decommission = *initial_nodes.iter().max().expect("non-empty");
    info!("Will decommission node {}", node_to_decommission);

    // --- Phase 1: all 4 nodes ---
    info!("=== Phase 1: all 4 nodes ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 1: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &initial_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 1: node {} got 0 queries",
            node_id
        );
    }

    // --- Decommission node from DC2 ---
    // Routes are posted (without this node) BEFORE the actual topology change.
    info!("Decommissioning node {}...", node_to_decommission);
    plc.decommission_node(node_to_decommission)
        .await
        .expect("Failed to decommission node");

    // --- Phase 2: 3 nodes remaining ---
    info!("=== Phase 2: 3 nodes remaining ===");
    let remaining_nodes = plc.active_node_ids();
    assert_eq!(remaining_nodes.len(), 3);
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 2: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    // Decommissioned node should not have a feedback channel at all.
    assert!(
        !per_node.contains_key(&node_to_decommission),
        "Phase 2: decommissioned node {} should not have feedback",
        node_to_decommission
    );
    for &node_id in &remaining_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 2: node {} got 0 queries",
            node_id
        );
    }

    // --- Add new node to DC2 ---
    // Cluster::add_node() uses 1-based CCM DC naming: 2 = dc2.
    info!("Adding new node to DC2...");
    let new_node_id = plc.add_node(Some(2)).await.expect("Failed to add new node");
    info!("New node added: {}", new_node_id);
    // Wait for the driver to discover the new node and open a connection
    // through the new proxy chain.
    plc.wait_for_connections_to_node(new_node_id, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to newly added node");

    // --- Phase 3: 4 nodes again (with new node) ---
    info!("=== Phase 3: 4 nodes with new node ===");
    let final_nodes = plc.active_node_ids();
    assert_eq!(final_nodes.len(), 4);
    assert!(
        final_nodes.contains(&new_node_id),
        "New node {} should be in active list",
        new_node_id
    );
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 3: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &final_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 3: node {} got 0 queries (including new node)",
            node_id
        );
    }
}

#[tokio::test]
async fn test_client_routes_multi_dc_topology_change() {
    setup_tracing();
    run_client_routes_test(cluster_2dc_2_2, multi_dc_topology_change).await;
}

// ---------------------------------------------------------------------------
// Test 5: Rolling restart (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// Rolling-restart all 3 nodes one at a time. After each restart, verify the
/// driver re-reads `system.client_routes` (NLB port changes each time) and
/// routes traffic through all 3 nodes.
///
/// Phase 0: baseline — all 3 nodes, N queries, all 3 >= 1.
/// Phases 1–3: for each node: stop CCM + chain, start CCM + chain, wait for
///             connection, N queries, all 3 >= 1.
async fn rolling_restart(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'replication_factor': 3").await;

    // --- Phase 0: baseline ---
    info!("=== Phase 0: baseline (all 3 nodes) ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 0: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(count >= 1, "Phase 0: node {} got 0 queries", node_id);
    }

    // --- Rolling restart: stop + start each node in turn ---
    for target_node in 1..=3u16 {
        info!("=== Rolling restart: stopping node {} ===", target_node);

        // Stop the CCM node.
        {
            let node = plc
                .cluster_mut()
                .nodes_mut()
                .get_mut_by_id(target_node)
                .expect("node not found");
            node.stop(None)
                .await
                .unwrap_or_else(|e| panic!("Failed to stop node {}: {}", target_node, e));
        }
        plc.stop_node_chain(target_node)
            .await
            .unwrap_or_else(|e| panic!("Failed to stop chain for node {}: {}", target_node, e));

        // Start the CCM node again.
        info!("Rolling restart: starting node {}...", target_node);
        {
            let node = plc
                .cluster_mut()
                .nodes_mut()
                .get_mut_by_id(target_node)
                .expect("node not found");
            node.start(None)
                .await
                .unwrap_or_else(|e| panic!("Failed to start node {}: {}", target_node, e));
        }
        plc.restart_node_chain(target_node)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", target_node, e));

        plc.wait_for_connections_to_node(target_node, CONNECTION_WAIT_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Driver did not reconnect to restarted node {}: {}",
                    target_node, e
                )
            });

        // Verify all 3 nodes receive traffic after this restart.
        info!(
            "=== Phase after restarting node {}: verifying all 3 nodes ===",
            target_node
        );
        let mut rxs = plc.setup_query_feedback();
        run_queries(&session, &ks, QUERIES_PER_PHASE).await;
        let (per_node, total) = drain_feedback(&mut rxs);
        info!(
            "After restarting node {}: per_node={:?}, total={}",
            target_node, per_node, total
        );
        assert_eq!(
            total, QUERIES_PER_PHASE as usize,
            "After restarting node {}: total mismatch",
            target_node
        );
        for (&node_id, &count) in &per_node {
            assert!(
                count >= 1,
                "After restarting node {}: node {} got 0 queries",
                target_node,
                node_id
            );
        }
    }
}

#[tokio::test]
async fn test_client_routes_rolling_restart() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, rolling_restart).await;
}

// ---------------------------------------------------------------------------
// Test 6: NLB port remap without Scylla restart (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// Rebuild proxy chains for 2 nodes WITHOUT restarting the Scylla nodes
/// themselves. The NLB ports change, routes are re-posted, and the driver
/// must detect the route update via `CLIENT_ROUTES_CHANGE` and re-establish
/// connections through the new ports.
///
/// This is the most client-routes-specific test: it isolates route-change
/// detection from node-restart behavior.
///
/// Phase 1: all 3 nodes, N queries, all 3 >= 1.
/// Remap: `restart_node_chain(1)` + `restart_node_chain(2)` (Scylla stays up).
/// Phase 2: N queries through new NLB ports, all 3 >= 1.
async fn nlb_port_remap(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'replication_factor': 3").await;

    // --- Phase 1: baseline ---
    info!("=== Phase 1: baseline (all 3 nodes) ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 1: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(count >= 1, "Phase 1: node {} got 0 queries", node_id);
    }

    // --- Remap: rebuild proxy chains for nodes 1 and 2 ---
    // Scylla nodes stay up; only the proxy + NLB are replaced, giving
    // them new OS-assigned ports.
    info!("=== Remapping NLB ports for nodes 1 and 2 ===");
    plc.restart_node_chain(1)
        .await
        .expect("Failed to remap chain for node 1");
    plc.restart_node_chain(2)
        .await
        .expect("Failed to remap chain for node 2");

    // Wait for the driver to connect through the new NLB ports using
    // the freshly-read routes.
    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not reconnect through new NLB ports");

    // --- Phase 2: verify traffic goes through new ports ---
    info!("=== Phase 2: after NLB port remap ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 2: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(
            count >= 1,
            "Phase 2: node {} got 0 queries — driver didn't follow route update",
            node_id
        );
    }
}

#[tokio::test]
async fn test_client_routes_nlb_port_remap() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, nlb_port_remap).await;
}

// ---------------------------------------------------------------------------
// Test 7: Scale out (1 DC, 3 →  6 nodes)
// ---------------------------------------------------------------------------

/// Start with 3 nodes, add 3 more one at a time. After all additions, verify
/// all 6 nodes receive traffic through their respective NLB →  proxy chains.
///
/// Phase 1: 3 nodes, N queries, all 3 >= 1.
/// Add: `add_node(None)` x 3 (nodes 4, 5, 6 join DC1).
/// Phase 2: 6 nodes, N queries, all 6 >= 1.
async fn scale_out(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'replication_factor': 3").await;

    // --- Phase 1: initial 3 nodes ---
    info!("=== Phase 1: initial 3 nodes ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 1: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for (&node_id, &count) in &per_node {
        assert!(count >= 1, "Phase 1: node {} got 0 queries", node_id);
    }

    // --- Add 3 new nodes ---
    let mut new_node_ids = Vec::new();
    for i in 1..=3 {
        info!("Adding node {} of 3...", i);
        let new_id = plc
            .add_node(None)
            .await
            .unwrap_or_else(|e| panic!("Failed to add node {} of 3: {}", i, e));
        info!("Added node {}", new_id);
        new_node_ids.push(new_id);
    }

    // Wait for the driver to discover all 6 nodes and open connections.
    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all 6 nodes");

    // --- Phase 2: all 6 nodes ---
    info!("=== Phase 2: all 6 nodes ===");
    let active = plc.active_node_ids();
    assert_eq!(active.len(), 6, "Expected 6 active nodes, got {:?}", active);

    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 2: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &active {
        let count = *per_node.get(&node_id).unwrap_or(&0);
        assert!(
            count >= 1,
            "Phase 2: node {} got 0 queries — driver didn't discover new node",
            node_id
        );
    }
}

#[tokio::test]
async fn test_client_routes_scale_out() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, scale_out).await;
}

// ---------------------------------------------------------------------------
// Test 8: Event-driven reroute (2 DCs, 2+2 nodes)
// ---------------------------------------------------------------------------

/// Wait for any of the given feedback receivers to produce a message.
///
/// Only one proxy hosts the control connection, so only one receiver will
/// actually fire. This polls all receivers until any one gets a message.
async fn wait_for_any_feedback(
    receivers: &mut HashMap<NodeId, mpsc::UnboundedReceiver<FeedbackItem>>,
    timeout: Duration,
    context: &str,
) {
    let result = tokio::time::timeout(timeout, async {
        loop {
            for (_node_id, rx) in receivers.iter_mut() {
                match rx.try_recv() {
                    Ok(_feedback) => return,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Timed out after {:?} waiting for feedback: {}",
        timeout,
        context
    );
}

/// Verify the driver reacts to `CLIENT_ROUTES_CHANGE` events and reconnects
/// through updated routes in a multi-DC setup.
///
/// Multi-DC cluster (2 DCs, 2+2 nodes). Events are injected directly into
/// the control connection via [`RunningNode::inject_event_to_cc`] — no
/// keepalive dependency.
///
/// ## Phases
///
/// 1. **Baseline**: Build session, create schema, verify all 4 nodes get queries.
///
/// 2. **Event injection (no route change)**: Inject a `CLIENT_ROUTES_CHANGE`
///    event for DC1 nodes via the proxy. The driver must re-query
///    `system.client_routes` (detected by matching the EXECUTE request,
///    whose body contains the connection_id string). No routes actually change.
///
/// 3. **Full-DC reroute**: Restart proxy chains for both DC1 nodes (new NLB
///    ports). The route POST triggers a real ScyllaDB event. Wait for
///    connections and verify all 4 nodes receive traffic.
///
/// 4. **Cross-DC reroute**: Restart one node from each DC. Same verification.
///
/// 5. **Malformed event recovery**: Inject a malformed event (mismatched array
///    lengths). The CC breaks, the driver reconnects and performs a full
///    metadata refresh. Verify all 4 nodes still work.
async fn event_driven_reroute(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_connections_to_all_nodes(CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all proxy nodes");

    let ks = unique_keyspace_name();
    create_test_schema(&session, &ks, "'dc1': 2, 'dc2': 2").await;

    // Identify nodes by DC. In a 2+2 setup, CCM node IDs are 1,2 (DC1)
    // and 3,4 (DC2).
    let all_nodes = plc.active_node_ids();
    assert_eq!(all_nodes.len(), 4, "Expected 4 initial nodes");
    let dc1_nodes: Vec<_> = all_nodes.iter().copied().filter(|&id| id <= 2).collect();
    let dc2_nodes: Vec<_> = all_nodes.iter().copied().filter(|&id| id > 2).collect();
    assert_eq!(dc1_nodes.len(), 2);
    assert_eq!(dc2_nodes.len(), 2);
    info!("DC1 nodes: {:?}, DC2 nodes: {:?}", dc1_nodes, dc2_nodes);

    let requery_timeout = Duration::from_secs(10);

    // ---------------------------------------------------------------
    // Phase 1: Baseline
    // ---------------------------------------------------------------
    info!("=== Phase 1: baseline ===");
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 1: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &all_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 1: node {} got 0 queries",
            node_id
        );
    }

    // ---------------------------------------------------------------
    // Phase 2: Event injection — verify event processing
    // ---------------------------------------------------------------
    // Inject a CLIENT_ROUTES_CHANGE event for DC1 nodes (no actual route
    // change). The driver should re-query system.client_routes.
    info!(
        "=== Phase 2: event injection for DC1 nodes {:?} ===",
        dc1_nodes
    );
    let mut event_rxs = plc.setup_event_requery_detection();
    let injected = plc.inject_event(&dc1_nodes);
    assert!(injected >= 1, "Phase 2: event was not injected into any CC");

    wait_for_any_feedback(
        &mut event_rxs,
        requery_timeout,
        "Phase 2: driver did not re-query system.client_routes after injected event",
    )
    .await;
    info!("Phase 2: driver re-queried system.client_routes after injected event");

    // Verify all nodes still work after event processing.
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 2: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &all_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 2: node {} got 0 queries",
            node_id
        );
    }

    // ---------------------------------------------------------------
    // Phase 3: Full-DC reroute (both DC1 nodes)
    // ---------------------------------------------------------------
    // Restart proxy chains for both DC1 nodes. This gives them new NLB
    // ports and posts updated routes. ScyllaDB emits a real
    // CLIENT_ROUTES_CHANGE event, causing the driver to re-read routes
    // and reconnect through the new ports.
    info!(
        "=== Phase 3: full-DC reroute (DC1 nodes {:?}) ===",
        dc1_nodes
    );
    for &node_id in &dc1_nodes {
        plc.restart_node_chain(node_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", node_id, e));
    }

    // Wait for the driver to connect through the new NLB ports.
    for &node_id in &dc1_nodes {
        plc.wait_for_connections_to_node(node_id, CONNECTION_WAIT_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Phase 3: driver did not reconnect to node {}: {}",
                    node_id, e
                )
            });
    }

    // Verify traffic reaches all 4 nodes through the new ports.
    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 3: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &all_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 3: node {} got 0 queries",
            node_id
        );
    }

    // ---------------------------------------------------------------
    // Phase 4: Cross-DC reroute (one node from each DC)
    // ---------------------------------------------------------------
    let cross_dc_nodes = [dc1_nodes[0], dc2_nodes[1]];
    info!(
        "=== Phase 4: cross-DC reroute (nodes {:?}) ===",
        cross_dc_nodes
    );

    for &node_id in &cross_dc_nodes {
        plc.restart_node_chain(node_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", node_id, e));
    }

    for &node_id in &cross_dc_nodes {
        plc.wait_for_connections_to_node(node_id, CONNECTION_WAIT_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Phase 4: driver did not reconnect to node {}: {}",
                    node_id, e
                )
            });
    }

    let mut rxs = plc.setup_query_feedback();
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 4: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &all_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 4: node {} got 0 queries",
            node_id
        );
    }

    // ---------------------------------------------------------------
    // Phase 5: Malformed event recovery
    // ---------------------------------------------------------------
    info!("=== Phase 5: malformed event ===");

    // Set up detection for the metadata refresh that happens after the CC
    // reconnects, then inject the malformed event.
    let mut event_rxs = plc.setup_malformed_event_requery_detection();
    let injected = plc.inject_malformed_event();
    assert!(
        injected >= 1,
        "Phase 5: malformed event was not injected into any CC"
    );

    // Wait for the metadata refresh detection (PREPARE for
    // system.client_routes on the new CC). After the CC breaks and
    // reconnects, a new ControlConnection instance is created with a
    // fresh prepared-statement cache, so the PREPARE is sent again.
    wait_for_any_feedback(
        &mut event_rxs,
        requery_timeout,
        "Phase 5: driver did not perform metadata refresh after malformed event",
    )
    .await;

    let mut rxs = plc.setup_query_feedback();

    // Verify all 4 nodes still receive traffic.
    run_queries(&session, &ks, QUERIES_PER_PHASE).await;
    let (per_node, total) = drain_feedback(&mut rxs);
    info!("Phase 5: per_node={:?}, total={}", per_node, total);
    assert_eq!(total, QUERIES_PER_PHASE as usize);
    for &node_id in &all_nodes {
        assert!(
            *per_node.get(&node_id).unwrap_or(&0) >= 1,
            "Phase 5: node {} got 0 queries after malformed event recovery",
            node_id
        );
    }

    info!("All phases passed — event-driven reroute works correctly");
}

#[tokio::test]
async fn test_client_routes_event_driven_reroute() {
    setup_tracing();
    run_client_routes_test(cluster_2dc_2_2, event_driven_reroute).await;
}
