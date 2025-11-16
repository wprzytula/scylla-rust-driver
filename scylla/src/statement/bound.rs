//! Defines the [`BoundStatement`] type, which represents a prepared statement
//! that has already been bound with values to be executed with.

use std::{borrow::Cow, fmt::Debug};

use scylla_cql::serialize::{
    SerializationError,
    row::{SerializeRow, SerializedValues},
    value::SerializeValue,
};
use thiserror::Error;

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

    pub(crate) fn new_untyped(
        prepared: Cow<'prepared, PreparedStatement>,
        values: SerializedValues,
    ) -> Self {
        Self { prepared, values }
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

/// [StatementBinder] owns or borrows a [PreparedStatement] and can bind values
/// (arguments) to it.
///
/// [StatementBinder] serializes and thus type erases the values.
pub struct StatementBinder<'prepared> {
    prepared: Cow<'prepared, PreparedStatement>,
}

impl StatementBinder<'static> {
    pub(crate) fn new_owned(prepared: PreparedStatement) -> Self {
        Self {
            prepared: Cow::Owned(prepared),
        }
    }
}

impl<'prepared> StatementBinder<'prepared> {
    pub(crate) fn new_borrowed(prepared: &'prepared PreparedStatement) -> Self {
        Self {
            prepared: Cow::Borrowed(prepared),
        }
    }

    /// Returns [AppendingStatementBinder], which can be used to bind values
    /// to the prepared statement in the order they appear in the statement.
    pub fn appending_binder(self) -> AppendingStatementBinder<'prepared> {
        AppendingStatementBinder {
            prepared: self.prepared,
            values: SerializedValues::new(),
        }
    }

    /// Returns [ByIndexStatementBinder], which can be used to bind values
    /// to the prepared statement by their index.
    pub fn by_index_binder(self) -> ByIndexStatementBinder<'prepared, 'prepared> {
        ByIndexStatementBinder {
            values: std::iter::repeat(None)
                .take(self.prepared.get_prepared_metadata().col_count)
                .collect(),
            prepared: self.prepared,
        }
    }

    /// Returns [ByNameStatementBinder], which can be used to bind values
    /// to the prepared statement by their name.
    pub fn by_name_binder(self) -> ByNameStatementBinder<'prepared, 'prepared> {
        ByNameStatementBinder {
            values: std::iter::repeat(None)
                .take(self.prepared.get_prepared_metadata().col_count)
                .collect(),
            prepared: self.prepared,
        }
    }
}

/// An error that can occur while binding values to a prepared statement by appending.
/// Returned by [AppendingStatementBinder]'s methods.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AppendingStatementBinderError {
    #[error("Too few values provided: {provided} (required: {required})")]
    TooFewValues { required: usize, provided: u16 },

    #[error("An extra value provided (required: {required})")]
    TooManyValues { required: usize },

    #[error(transparent)]
    Serialization(#[from] SerializationError),
}

/// [AppendingStatementBinder] can be used to bind values to the prepared statement
/// in the order they appear in the statement.
#[derive(Debug)]
pub struct AppendingStatementBinder<'prepared> {
    prepared: Cow<'prepared, PreparedStatement>,
    values: SerializedValues,
}

impl<'prepared> AppendingStatementBinder<'prepared> {
    /// Binds a value to the prepared statement.
    /// Requires that the value is bound in the order they appear in the statement.
    pub fn bind_next_value(
        mut self,
        value: impl SerializeValue,
    ) -> Result<Self, AppendingStatementBinderError> {
        let required_values = self.prepared.get_variable_col_specs().len();
        let provided_values = self.values.element_count();

        let Some(spec) = self
            .prepared
            .get_variable_col_specs()
            .get_by_index(provided_values as usize)
        else {
            return Err(AppendingStatementBinderError::TooManyValues {
                required: required_values,
            });
        };

        self.values.add_value(&value, spec.typ())?;

        Ok(self)
    }

    /// Finishes the binding process and returns a `BoundStatement`.
    /// Checks if all required values are provided.
    pub fn finish(self) -> Result<BoundStatement<'prepared>, AppendingStatementBinderError> {
        let Self { prepared, values } = self;

        let required_values = prepared.get_variable_col_specs().len();
        let provided_values = values.element_count();
        if required_values == provided_values as usize {
            Ok(BoundStatement { prepared, values })
        } else {
            Err(AppendingStatementBinderError::TooFewValues {
                required: required_values,
                provided: provided_values,
            })
        }
    }
}

// Exists to allow automated derive(Debug) for ByIndexStatementBinder and ByNameStatementBinder.
#[derive(Clone, Copy)]
struct DynValue<'v>(&'v dyn SerializeValue);
impl Debug for DynValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<type-erased CQL value>")
    }
}

/// An error that can occur while binding values by index to a prepared statement.
/// Returned by [ByIndexStatementBinder]'s methods.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ByIndexStatementBinderError {
    #[error("Provided index is too big - no value parameter at such index: {idx}")]
    NoSuchIndex { idx: usize },

    #[error("Missing value at index: {idx}")]
    MissingValueAtIndex { idx: usize },

    #[error("Value at index {idx} provided twice")]
    DuplicatedValue { idx: usize },

    #[error(transparent)]
    Serialization(#[from] SerializationError),
}

/// [ByIndexStatementBinder] can be used to bind values to the prepared statement
/// by their index.
#[derive(Debug)]
pub struct ByIndexStatementBinder<'prepared, 'value> {
    prepared: Cow<'prepared, PreparedStatement>,
    values: Vec<Option<DynValue<'value>>>,
}

impl<'prepared, 'value> ByIndexStatementBinder<'prepared, 'value> {
    /// Binds a value at the specified index.
    ///
    /// If the index is out of bounds or a value is already bound to the index, it returns an error.
    ///
    /// Note: serialization of the value is not performed at this stage. It is deferred until
    /// the binding is finished ([ByIndexStatementBinder::finish]).
    /// This means that all values must live at least until the binding of all values is finished.
    // TODO: consider alternative implementation that would allow to bind values
    // without lifetime restrictions, by serializing them immediately on binding.
    pub fn bind_value_by_index(
        mut self,
        index: usize,
        value: &'value dyn SerializeValue,
    ) -> Result<Self, ByIndexStatementBinderError> {
        let Some(slot) = self.values.get_mut(index) else {
            return Err(ByIndexStatementBinderError::NoSuchIndex { idx: index });
        };

        match slot {
            Some(_) => return Err(ByIndexStatementBinderError::DuplicatedValue { idx: index }),
            None => *slot = Some(DynValue(value)),
        }

        Ok(self)
    }

    /// Finishes the binding process and returns a `BoundStatement`.
    ///
    /// Actually serializes the values and checks if all required values are provided.
    /// If any value is missing, it returns an error.
    pub fn finish(self) -> Result<BoundStatement<'prepared>, ByIndexStatementBinderError> {
        let Self { prepared, values } = self;

        let mut serialized_values = SerializedValues::new();

        // Length of `values` and `col_specs` match because of how `ByIndexStatementBinder` is constructed.
        // Let's check it just in case.
        debug_assert_eq!(
            values.len(),
            prepared.get_prepared_metadata().col_specs.len()
        );

        for (idx, (value, spec)) in values
            .iter()
            .copied()
            .zip(prepared.get_prepared_metadata().col_specs.iter())
            .enumerate()
        {
            let Some(value) = value else {
                return Err(ByIndexStatementBinderError::MissingValueAtIndex { idx });
            };
            serialized_values.add_value(&value.0, spec.typ())?;
        }

        Ok(BoundStatement {
            prepared,
            values: serialized_values,
        })
    }
}

/// An error that can occur while binding values by name to a prepared statement.
/// Returned by [ByNameStatementBinder]'s methods.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ByNameStatementBinderError {
    #[error("No value parameter has the provided name: {name}")]
    NoSuchName { name: String },

    #[error("Missing value for parameter: {name}")]
    MissingValueForParameter { name: String },

    #[error("Value for name {name} provided twice")]
    DuplicatedValue { name: String },

    #[error(transparent)]
    Serialization(#[from] SerializationError),
}

/// [ByNameStatementBinder] can be used to bind values to the prepared statement
/// by their name.
#[derive(Debug)]
pub struct ByNameStatementBinder<'prepared, 'value> {
    prepared: Cow<'prepared, PreparedStatement>,
    values: Vec<Option<DynValue<'value>>>,
}

impl<'prepared, 'value> ByNameStatementBinder<'prepared, 'value> {
    /// Binds a value with the specified name.
    ///
    /// If the name is unknown or a value is already bound to the name, it returns an error.
    ///
    /// Note: serialization of the value is not performed at this stage. It is deferred until
    /// the binding is finished ([ByNameStatementBinder::finish]).
    /// This means that all values must live at least until the binding of all values is finished.
    // TODO: consider alternative implementation that would allow to bind values
    // without lifetime restrictions, by serializing them immediately on binding.
    pub fn bind_value_by_name(
        mut self,
        name: &str,
        value: &'value dyn SerializeValue,
    ) -> Result<Self, ByNameStatementBinderError> {
        let Some(slot) = self
            .values
            .iter_mut()
            .zip(self.prepared.get_prepared_metadata().col_specs.iter())
            .find_map(|(slot, spec)| (spec.name() == name).then_some(slot))
        else {
            return Err(ByNameStatementBinderError::NoSuchName {
                name: name.to_owned(),
            });
        };

        match slot {
            Some(_) => {
                return Err(ByNameStatementBinderError::DuplicatedValue {
                    name: name.to_owned(),
                });
            }
            None => *slot = Some(DynValue(value)),
        }

        Ok(self)
    }

    /// Finishes the binding process and returns a `BoundStatement`.
    ///
    /// Actually serializes the values and checks if all required values are provided.
    /// If any value is missing, it returns an error.
    pub fn finish(self) -> Result<BoundStatement<'prepared>, ByNameStatementBinderError> {
        let Self { prepared, values } = self;

        let mut serialized_values = SerializedValues::new();

        // Length of `values` and `col_specs` match because of how `ByNameStatementBinder` is constructed.
        // Let's check it just in case.
        debug_assert_eq!(
            values.len(),
            prepared.get_prepared_metadata().col_specs.len()
        );

        for (value, spec) in values
            .iter()
            .copied()
            .zip(prepared.get_prepared_metadata().col_specs.iter())
        {
            let Some(value) = value else {
                return Err(ByNameStatementBinderError::MissingValueForParameter {
                    name: spec.name().to_owned(),
                });
            };
            serialized_values.add_value(&value.0, spec.typ())?;
        }

        Ok(BoundStatement {
            prepared,
            values: serialized_values,
        })
    }
}
