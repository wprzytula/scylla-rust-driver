use anyhow::Result;
use futures::stream::StreamExt;
use scylla::{query::Query, Session, SessionBuilder};
use std::env;

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
    let (res1, paging_state1) = session
        .query_single_page(paged_query.clone(), &[], None)
        .await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        paging_state1,
        res1.rows_num()?,
    );
    assert!(!paging_state1.finished());
    let (res2, paging_state2) = session
        .query_single_page(
            paged_query.clone(),
            &[],
            paging_state1.into_paging_continuation_opt(),
        )
        .await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        paging_state2,
        res2.rows_num()?,
    );
    assert!(!paging_state2.finished());
    let (res3, paging_state3) = session
        .query_single_page(
            paged_query.clone(),
            &[],
            paging_state2.into_paging_continuation_opt(),
        )
        .await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        paging_state3,
        res3.rows_num()?,
    );
    assert!(paging_state3.finished());

    let paged_prepared = session
        .prepare(
            Query::new("SELECT a, b, c FROM examples_ks.select_paging")
                .with_page_size(7.try_into().unwrap()),
        )
        .await?;
    let (res4, paging_state4) = session
        .execute_single_page(&paged_prepared, &[], None)
        .await?;
    println!(
        "Paging state from the prepared statement execution: {:#?} ({} rows)",
        paging_state4,
        res4.rows_num()?,
    );
    let (res5, paging_state5) = session
        .execute_single_page(
            &paged_prepared,
            &[],
            paging_state4.into_paging_continuation_opt(),
        )
        .await?;
    println!(
        "Paging state from the second prepared statement execution: {:#?} ({} rows)",
        paging_state5,
        res5.rows_num()?,
    );
    assert!(!paging_state5.finished());
    let (res6, paging_state6) = session
        .execute_single_page(
            &paged_prepared,
            &[],
            paging_state5.into_paging_continuation_opt(),
        )
        .await?;
    println!(
        "Paging state from the third prepared statement execution: {:#?} ({} rows)",
        paging_state6,
        res6.rows_num()?,
    );
    assert!(paging_state6.finished());
    println!("Ok.");

    Ok(())
}
