use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use scylla::load_balancing::{DefaultPolicy, LoadBalancingPolicy};
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use scylla::transport::session::Session;
use scylla::{ExecutionProfile, SessionBuilder};
use std::env;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

static PK: AtomicI32 = AtomicI32::new(0);

const CONCURRENCY: i32 = 256;
const INSERTS_PER_FIBER: i32 = 20000;

async fn prepare_schema(session: &Session) -> Result<PreparedStatement> {
    session.query("DROP KEYSPACE IF EXISTS bench", &[]).await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS bench WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}", &[]).await?;

    session.use_keyspace("bench", true).await?;

    session
        .query(
            "CREATE TABLE t (a int, b int, c text, d list<ascii>, primary key (a, b))",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO t (a, b, c, d) VALUES (?, ?, ?, ?)")
        .await?;

    Ok(prepared)
}

async fn benchmarked_query(session: Arc<Session>, prepared: Arc<PreparedStatement>) -> Result<()> {
    for _ in 0..INSERTS_PER_FIBER {
        session
            .execute(
                &prepared,
                (
                    PK.fetch_add(1, Ordering::Relaxed),
                    42,
                    "Ala ma kota.",
                    vec!["21", "37"],
                ),
            )
            .await?;
    }

    Ok(())
}

#[derive(Debug)]
struct NonShardAwareLB(Arc<dyn LoadBalancingPolicy>);
impl LoadBalancingPolicy for NonShardAwareLB {
    fn name(&self) -> String {
        "NonShardAwareLB".to_owned()
    }

    fn pick<'a>(
        &'a self,
        query: &'a scylla::load_balancing::RoutingInfo,
        cluster: &'a scylla::transport::ClusterData,
    ) -> Option<(
        scylla::transport::NodeRef<'a>,
        Option<scylla::routing::Shard>,
    )> {
        self.0
            .pick(query, cluster)
            .map(|(node, _shard)| (node, None))
    }

    fn fallback<'a>(
        &'a self,
        query: &'a scylla::load_balancing::RoutingInfo,
        cluster: &'a scylla::transport::ClusterData,
    ) -> scylla::load_balancing::FallbackPlan<'a> {
        Box::new(
            self.0
                .fallback(query, cluster)
                .map(|(node, _shard)| (node, None)),
        )
    }
}

#[tokio::main(flavor = "current_thread")]
// #[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let default_lb = DefaultPolicy::default();
    let lb = Arc::new(NonShardAwareLB(Arc::new(default_lb)));
    // let lb = Arc::new(default_lb);

    let handle = ExecutionProfile::builder()
        .consistency(Consistency::Quorum)
        .load_balancing_policy(lb)
        .build()
        .into_handle();

    let session = Arc::new(
        SessionBuilder::new()
            .known_node(uri)
            .default_execution_profile_handle(handle)
            .build()
            .await?,
    );

    let prepared = Arc::new(prepare_schema(&session).await?);

    // Spawn as many worker tasks as the concurrency allows
    let mut fibers = (0..CONCURRENCY)
        .map(|_| {
            let session = session.clone();
            let prepared = prepared.clone();
            tokio::task::spawn(async move { benchmarked_query(session, prepared).await })
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(fiber_result) = fibers.next().await {
        fiber_result??;
    }

    println!(
        "Rust driver benchmark completed: executed {} requests per fiber, with concurrency {}",
        INSERTS_PER_FIBER, CONCURRENCY
    );

    Ok(())
}
