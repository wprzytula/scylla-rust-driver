use scylla::response::{PagingState, PagingStateResponse};
use scylla::routing::Token;
use scylla::statement::Statement;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

#[tokio::test]
async fn test_bound_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t2 (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();
    session
        .ddl(format!("CREATE TABLE IF NOT EXISTS {ks}.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))"))
        .await
        .unwrap();

    // Refresh metadata as `ClusterState::compute_token` use them
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement = session
        .prepare(format!("SELECT a, b, c FROM {ks}.t2"))
        .await
        .unwrap();
    let bound_statement = prepared_statement.into_bind(&()).unwrap();
    let query_result = session.execute_bound_iter(bound_statement).await.unwrap();
    let specs = query_result.column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name(), name); // Check column name.
        assert_eq!(spec.table_spec().ks_name(), ks);
    }

    let prepared_statement = session
        .prepare(format!("INSERT INTO {ks}.t2 (a, b, c) VALUES (?, ?, ?)"))
        .await
        .unwrap();

    let prepared_complex_pk_statement = session
        .prepare(format!(
            "INSERT INTO {ks}.complex_pk (a, b, c, d) VALUES (?, ?, ?, 7)"
        ))
        .await
        .unwrap();

    let values = (17_i32, 16_i32, "I'm prepared!!!");

    let bound_statement = prepared_statement.bind(&values).unwrap();
    let bound_complex_pk_statement = prepared_complex_pk_statement.bind(&values).unwrap();

    session
        .execute_bound_unpaged(&bound_statement)
        .await
        .unwrap();
    session
        .execute_bound_unpaged(&bound_complex_pk_statement)
        .await
        .unwrap();

    // Verify that token calculation is compatible with Scylla
    {
        let (value,): (i64,) = session
            .query_unpaged(format!("SELECT token(a) FROM {ks}.t2"), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = bound_statement.calculate_token().unwrap().unwrap();
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "t2", &(values.0,))
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }
    {
        let (value,): (i64,) = session
            .query_unpaged(format!("SELECT token(a,b,c) FROM {ks}.complex_pk"), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = bound_complex_pk_statement
            .calculate_token()
            .unwrap()
            .unwrap();
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "complex_pk", &values)
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }

    // Verify that correct data was inserted
    {
        let rs = session
            .query_unpaged(format!("SELECT a,b,c FROM {ks}.t2"), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(i32, i32, String)>()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let r = &rs[0];
        assert_eq!(r, &(17, 16, String::from("I'm prepared!!!")));

        let mut results_from_manual_paging = vec![];
        let query = Statement::new(format!("SELECT a, b, c FROM {ks}.t2")).with_page_size(1);
        let prepared_paged = session.prepare(query).await.unwrap();
        let bound_paged = prepared_paged.bind(&()).unwrap();
        let mut paging_state = PagingState::start();
        let mut watchdog = 0;
        loop {
            let (rs_manual, paging_state_response) = session
                .execute_bound_single_page(&bound_paged, paging_state)
                .await
                .unwrap();
            let mut page_results = rs_manual
                .into_rows_result()
                .unwrap()
                .rows::<(i32, i32, String)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            results_from_manual_paging.append(&mut page_results);
            match paging_state_response {
                PagingStateResponse::HasMorePages { state } => {
                    paging_state = state;
                }
                _ if watchdog > 30 => break,
                PagingStateResponse::NoMorePages => break,
            }
            watchdog += 1;
        }
        assert_eq!(results_from_manual_paging, rs);
    }
    {
        let (a, b, c, d, e): (i32, i32, String, i32, Option<i32>) = session
            .query_unpaged(format!("SELECT a,b,c,d,e FROM {ks}.complex_pk"), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i32, i32, String, i32, Option<i32>)>()
            .unwrap();
        assert!(e.is_none());
        assert_eq!(
            (a, b, c.as_str(), d, e),
            (17, 16, "I'm prepared!!!", 7, None)
        );
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}
