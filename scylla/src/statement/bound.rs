//! Defines the [`BoundStatement`] type, which represents a prepared statement
//! that has already been bound with values to be executed with.

use std::{borrow::Cow, fmt::Debug};

use scylla_cql::serialize::{
    SerializationError,
    row::{SerializeRow, SerializedValues},
};

use crate::routing::Token;

use super::prepared::{
    PartitionKey, PartitionKeyError, PartitionKeyExtractionError, PreparedStatement,
};

/// Represents a prepared statement that has already had all its values bound.
#[derive(Debug, Clone)]
pub struct BoundStatement<'prepared> {
    pub(crate) prepared: Cow<'prepared, PreparedStatement>,
    pub(crate) values: SerializedValues,
}

impl BoundStatement<'static> {
    pub(crate) fn new_owned(
        prepared: PreparedStatement,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        let prepared = Cow::Owned(prepared);
        Ok(Self { prepared, values })
    }
}

impl<'prepared> BoundStatement<'prepared> {
    pub(crate) fn new_borrowed(
        prepared: &'prepared PreparedStatement,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        let prepared = Cow::Borrowed(prepared);
        Ok(Self { prepared, values })
    }

    /// Determines which values constitute the partition key and puts them in order.
    ///
    /// This is a preparation step necessary for calculating token based on a prepared statement.
    pub(crate) fn extract_partition_key<'ps>(
        &'ps self,
    ) -> Result<PartitionKey<'ps>, PartitionKeyExtractionError> {
        PartitionKey::new(self.prepared.get_prepared_metadata(), &self.values)
    }

    pub(crate) fn extract_partition_key_and_calculate_token<'ps>(
        &'ps self,
    ) -> Result<Option<(PartitionKey<'ps>, Token)>, PartitionKeyError> {
        if !self.prepared.is_token_aware() {
            return Ok(None);
        }

        let partition_key = self.extract_partition_key()?;
        let token = partition_key.calculate_token(self.prepared.get_partitioner_name())?;

        Ok(Some((partition_key, token)))
    }

    /// Calculates the token for the bound statement.
    ///
    /// Returns the token that would be computed for executing the provided bound statement.
    pub fn calculate_token(&self) -> Result<Option<Token>, PartitionKeyError> {
        self.extract_partition_key_and_calculate_token()
            .map(|p| p.map(|(_, t)| t))
    }

    /// Returns the prepared statement behind the `BoundStatement`.
    pub fn prepared(&self) -> &PreparedStatement {
        &self.prepared
    }
}
