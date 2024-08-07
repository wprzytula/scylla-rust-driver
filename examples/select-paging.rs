use anyhow::Result;
use futures::stream::StreamExt;
use scylla::{query::Query, Session, SessionBuilder};
use std::env;
use std::ops::ControlFlow;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.select_paging (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    for i in 0..16_i32 {
        session
            .query_unpaged(
                "INSERT INTO examples_ks.select_paging (a, b, c) VALUES (?, ?, 'abc')",
                (i, 2 * i),
            )
            .await?;
    }

    // Iterate through select result with paging
    let mut rows_stream = session
        .query_iter("SELECT a, b, c FROM examples_ks.select_paging", &[])
        .await?
        .into_typed::<(i32, i32, String)>();

    while let Some(next_row_res) = rows_stream.next().await {
        let (a, b, c) = next_row_res?;
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    let paged_query = Query::new("SELECT a, b, c FROM examples_ks.select_paging")
        .with_page_size(6.try_into().unwrap());

    // Manual paging in a loop, unprepared statement.
    let mut paging_continuation = None;
    loop {
        let (res, paging_state) = session
            .query_single_page(paged_query.clone(), &[], paging_continuation)
            .await?;

        println!(
            "Paging state: {:#?} ({} rows)",
            paging_state,
            res.rows_num()?,
        );

        match paging_state.into_paging_continuation() {
            ControlFlow::Break(()) => {
                // No more pages to be fetched.
                break;
            }
            ControlFlow::Continue(continuation) => {
                // Update paging continuation from the paging state, so that query
                // will be resumed from where it ended the last time.
                paging_continuation = Some(continuation);
            }
        }
    }

    let paged_prepared = session
        .prepare(
            Query::new("SELECT a, b, c FROM examples_ks.select_paging")
                .with_page_size(7.try_into().unwrap()),
        )
        .await?;

    // Manual paging in a loop, prepared statement.
    let mut paging_continuation = None;
    loop {
        let (res, paging_state) = session
            .execute_single_page(&paged_prepared, &[], paging_continuation)
            .await?;

        println!(
            "Paging state from the prepared statement execution: {:#?} ({} rows)",
            paging_state,
            res.rows_num()?,
        );

        match paging_state.into_paging_continuation() {
            ControlFlow::Break(()) => {
                // No more pages to be fetched.
                break;
            }
            ControlFlow::Continue(continuation) => {
                // Update paging continuation from the paging state, so that query
                // will be resumed from where it ended the last time.
                paging_continuation = Some(continuation);
            }
        }
    }

    println!("Ok.");

    Ok(())
}
