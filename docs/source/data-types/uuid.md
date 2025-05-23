# Uuid

`Uuid` is represented as `uuid::Uuid`.

```rust
# extern crate scylla;
# extern crate uuid;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use uuid::Uuid;

// Insert some uuid into the table
let to_insert: Uuid = Uuid::parse_str("8e14e760-7fa8-11eb-bc66-000000000001")?;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read uuid from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(Uuid,)>()?;
while let Some((uuid_value,)) = iter.try_next().await? {
    println!("{:?}", uuid_value);
}
# Ok(())
# }
```