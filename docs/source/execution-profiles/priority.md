# Priorities of execution settings

You always have a default execution profile set for the `Session`, either the default one or overridden upon `Session` creation. Moreover, you can set a profile for specific statements, in which case the statement's profile has higher priority. Some options are also available for specific statements to be set directly on them, such as request timeout and consistency. In such case, the directly set options are preferred over those specified in execution profiles.

> **Recap**\
> Priorities are as follows:\
> `Session`'s default profile < Statement's profile < options set directly on a Statement


### Example
Priorities of execution profiles and directly set options:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::unprepared::Statement;
use scylla::statement::Consistency;
use scylla::client::execution_profile::ExecutionProfile;

let session_profile = ExecutionProfile::builder()
    .consistency(Consistency::One)
    .build();

let query_profile = ExecutionProfile::builder()
    .consistency(Consistency::Two)
    .build();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .default_execution_profile_handle(session_profile.into_handle())
    .build()
    .await?;

let mut query = Statement::from("SELECT * FROM ks.table");

// Statement is not assigned any specific profile, so session's profile is applied.
// Therefore, the statement will be executed with Consistency::One.
session.query_unpaged(query.clone(), ()).await?;

query.set_execution_profile_handle(Some(query_profile.into_handle()));
// Statement's profile is applied.
// Therefore, the statement will be executed with Consistency::Two.
session.query_unpaged(query.clone(), ()).await?;

query.set_consistency(Consistency::Three);
// An option is set directly on the Statement.
// Therefore, the statement will be executed with Consistency::Three.
session.query_unpaged(query, ()).await?;

# Ok(())
# }
```