# Unprepared statement

Unprepared statement takes CQL statement text and values and simply executes them on a `Session`:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn unprepared_statement_example(session: &Session) -> Result<(), Box<dyn Error>> {
// Insert a value into the table
let to_insert: i32 = 12345;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;
# Ok(())
# }
```

> ***Warning***\
> Don't use unprepared queries to receive large amounts of data.\
> By default the query is unpaged and might cause heavy load on the cluster.\
> In such cases use [paged query](paged.md) instead.\
> 
> `query_unpaged` will return all results in one, possibly giant, piece
> (unless a timeout occurs due to high load incurred by the cluster).

> ***Warning***\
> If the values are not empty, driver first needs to send a `PREPARE` request
> in order to fetch information required to serialize values. This will affect
> performance because 2 round trips will be required instead of 1.

### First argument - the statement
As the first argument `Session::query_unpaged` takes anything implementing `Into<Statement>`.\
You can create a statement manually to set custom options. For example to change statement consistency:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::unprepared::Statement;
use scylla::statement::Consistency;

// Create a Statement manually to change the Consistency to ONE
let mut my_statement: Statement = Statement::new("INSERT INTO ks.tab (a) VALUES(?)");
my_statement.set_consistency(Consistency::One);

// Insert a value into the table
let to_insert: i32 = 12345;
session.query_unpaged(my_statement, (to_insert,)).await?;
# Ok(())
# }
```
See [Statement API documentation](https://docs.rs/scylla/latest/scylla/statement/struct.Statement.html) for more options

### Second argument - the values
Statement text is constant, but the values may change.
You can pass changing values to a statement by specifying a list of variables as bound values.\
Each `?` in statement text will be filled with the matching value. 

The easiest way is to pass values using a tuple:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
// Sending an integer and a string using a tuple
session
    .query_unpaged("INSERT INTO ks.tab (a, b, c) VALUES(?, ?, 'text2')", (2_i32, "Some text"))
    .await?;
# Ok(())
# }
```
Here the first `?` will be filled with `2` and the second with `"Some text"`.
> **Never** pass values by adding strings, this could lead to [SQL Injection](https://en.wikipedia.org/wiki/SQL_injection)

See [Query values](values.md) for more information about sending values in queries

### Query result
`Session::query_unpaged` returns `QueryResult`.
The result can then be operated on via helper methods which verify that the result is of appropriate type.
Here, we use the `rows` method to check that the response indeed contains rows with a single `int` column:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {

// NOTE: using unpaged queries (SELECTs) is discouraged in general.
// Query results may be so big that it is not preferable to fetch them all at once.
// Even with small results, if there are a lot of tombstones, then there can be similar bad consequences.
// However, `query_unpaged` will return all results in one, possibly giant, piece
// (unless a timeout occurs due to high load incurred by the cluster).
// This:
// - increases latency,
// - has large memory footprint,
// - puts high load on the cluster,
// - is more likely to time out (because big work takes more time than little work,
//   and returning one large piece of data is more work than returning one chunk of data).
// To sum up, **for SELECTs** (especially those that may return a lot of data)
// **prefer paged queries**, e.g. with `Session::query_iter()`.


// Query rows from the table and print them
let result = session.query_unpaged("SELECT a FROM ks.tab", &[])
    .await?
    .into_rows_result()?;

let mut iter = result.rows::<(i32,)>()?;
while let Some(read_row) = iter.next().transpose()? {
    println!("Read a value from row: {}", read_row.0);
}
# Ok(())
# }
```

See [Query result](result.md) for more information about handling query results

### Performance
Unprepared statements should not be used in places where performance matters.\
If performance matters use a [Prepared statement](prepared.md) instead.

With unprepared statement the database has to parse statement text each time it's executed, which worsens performance.\

Additionally token and shard aware load balancing does not work with unprepared statements. They are sent to random nodes.
