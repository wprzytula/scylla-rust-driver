use assert_matches::assert_matches;
use scylla::{
    serialize::value::SerializeValue,
    statement::bound::{
        AppendingStatementBinderError, ByIndexStatementBinderError, ByNameStatementBinderError,
    },
};

use crate::utils::create_new_session_builder;

#[tokio::test]
async fn test_binders() {
    let session = create_new_session_builder().build().await.unwrap();

    let prepared = session
        .prepare("SELECT host_id FROM system.local WHERE key=?")
        .await
        .unwrap();

    // `AppendingStatementBinder`
    {
        // Correct case.
        {
            let binder = prepared.binder_borrowed().appending_binder();
            let bound = binder.bind_next_value("local").unwrap().finish().unwrap();
            session
                .execute_bound_unpaged(&bound)
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
        }

        // Incorrect case: not enough values.
        {
            let binder = prepared.binder_borrowed().appending_binder();
            let err = binder.finish().unwrap_err();
            assert_matches!(
                err,
                AppendingStatementBinderError::TooFewValues {
                    required: 1,
                    provided: 0
                }
            )
        }

        // Incorrect case: too many values.
        {
            let binder = prepared.binder_borrowed().appending_binder();
            let err = binder
                .bind_next_value("local")
                .unwrap()
                .bind_next_value("remote")
                .unwrap_err();
            assert_matches!(
                err,
                AppendingStatementBinderError::TooManyValues { required: 1 }
            )
        }

        // Incorrect case: wrong type.
        {
            let binder = prepared.binder_borrowed().appending_binder();
            let err = binder.bind_next_value(42).unwrap_err();
            assert_matches!(err, AppendingStatementBinderError::Serialization(_))
        }

        // Static lifetime check: the owned binder can be used after the original prepared statement is dropped.
        {
            let _bound = {
                let prepared = prepared.clone();
                let binder = prepared.binder_owned().appending_binder();
                binder.bind_next_value("local").unwrap().finish().unwrap()
            };
        }
    }

    // `ByIndexStatementBinder`
    {
        // Correct case.
        {
            let binder = prepared.binder_borrowed().by_index_binder();
            let bound = binder
                .bind_value_by_index(0, &"local")
                .unwrap()
                .finish()
                .unwrap();
            session
                .execute_bound_unpaged(&bound)
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
        }

        // Incorrect case: not enough values.
        {
            let binder = prepared.binder_borrowed().by_index_binder();
            let err = binder.finish().unwrap_err();
            assert_matches!(
                err,
                ByIndexStatementBinderError::MissingValueAtIndex { idx: 0 }
            )
        }

        // Incorrect case: index out of parameter bounds.
        {
            let binder = prepared.binder_borrowed().by_index_binder();
            let err = binder.bind_value_by_index(1, &"remote").unwrap_err();
            assert_matches!(err, ByIndexStatementBinderError::NoSuchIndex { idx: 1 })
        }

        // Incorrect case: value bound twice.
        {
            let binder = prepared.binder_borrowed().by_index_binder();
            let err = binder
                .bind_value_by_index(0, &"local")
                .unwrap()
                .bind_value_by_index(0, &"remote")
                .unwrap_err();
            assert_matches!(err, ByIndexStatementBinderError::DuplicatedValue { idx: 0 })
        }

        // Incorrect case: wrong type.
        {
            let binder = prepared.binder_borrowed().by_index_binder();
            let binder = binder
                .bind_value_by_index(0, &42 as &dyn SerializeValue)
                .unwrap();
            let err = binder.finish().unwrap_err();
            assert_matches!(err, ByIndexStatementBinderError::Serialization(_))
        }

        // Static lifetime check: the owned binder can be used after the original prepared statement is dropped.
        {
            let _bound = {
                let prepared = prepared.clone();
                let binder = prepared.binder_owned().by_index_binder();
                binder
                    .bind_value_by_index(0, &"local")
                    .unwrap()
                    .finish()
                    .unwrap()
            };
        }
    }

    // `ByNameStatementBinder`
    {
        // Correct case.
        {
            let binder = prepared.binder_borrowed().by_name_binder();
            let bound = binder
                .bind_value_by_name("key", &"local")
                .unwrap()
                .finish()
                .unwrap();
            session
                .execute_bound_unpaged(&bound)
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
        }

        // Incorrect case: not enough values.
        {
            let binder = prepared.binder_borrowed().by_name_binder();
            let err = binder.finish().unwrap_err();
            assert_matches!(
                err,
                ByNameStatementBinderError::MissingValueForParameter { name } if name == "key"
            )
        }

        // Incorrect case: unknown parameter name.
        {
            let binder = prepared.binder_borrowed().by_name_binder();
            let err = binder.bind_value_by_name("value", &"remote").unwrap_err();
            assert_matches!(
                err,
                ByNameStatementBinderError::NoSuchName { name } if name == "value"
            )
        }

        // Incorrect case: value bound twice.
        {
            let binder = prepared.binder_borrowed().by_name_binder();
            let err = binder
                .bind_value_by_name("key", &"local")
                .unwrap()
                .bind_value_by_name("key", &"remote")
                .unwrap_err();
            assert_matches!(err, ByNameStatementBinderError::DuplicatedValue { name } if name == "key")
        }

        // Incorrect case: wrong type.
        {
            let binder = prepared.binder_borrowed().by_name_binder();
            let binder = binder
                .bind_value_by_name("key", &42 as &dyn SerializeValue)
                .unwrap();
            let err = binder.finish().unwrap_err();
            assert_matches!(err, ByNameStatementBinderError::Serialization(_))
        }

        // Static lifetime check: the owned binder can be used after the original prepared statement is dropped.
        {
            let _bound = {
                let prepared = prepared.clone();
                let binder = prepared.binder_owned().by_name_binder();
                binder
                    .bind_value_by_name("key", &"local")
                    .unwrap()
                    .finish()
                    .unwrap()
            };
        }
    }
}
