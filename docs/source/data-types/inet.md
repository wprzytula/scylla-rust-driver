# Inet
`Inet` is represented as `std::net::IpAddr`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use std::net::{IpAddr, Ipv4Addr};

// Insert some ip address into the table
let to_insert: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read inet from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(IpAddr,)>()?;
while let Some((inet_value,)) = iter.try_next().await? {
    println!("{:?}", inet_value);
}
# Ok(())
# }
```