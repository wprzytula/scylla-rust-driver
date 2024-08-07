use std::{sync::Arc, time::Duration};

use thiserror::Error;

use crate::transport::execution_profile::ExecutionProfileHandle;
use crate::{history::HistoryListener, retry_policy::RetryPolicy};

pub mod batch;
pub mod prepared_statement;
pub mod query;

pub use crate::frame::types::{Consistency, SerialConsistency};

// This is the default common to drivers.
const DEFAULT_PAGE_SIZE: i32 = 5000;

#[derive(Debug, Clone, Default)]
pub(crate) struct StatementConfig {
    pub(crate) consistency: Option<Consistency>,
    pub(crate) serial_consistency: Option<Option<SerialConsistency>>,

    pub(crate) is_idempotent: bool,

    pub(crate) skip_result_metadata: bool,
    pub(crate) tracing: bool,
    pub(crate) timestamp: Option<i64>,
    pub(crate) request_timeout: Option<Duration>,

    pub(crate) history_listener: Option<Arc<dyn HistoryListener>>,

    pub(crate) execution_profile_handle: Option<ExecutionProfileHandle>,
    pub(crate) retry_policy: Option<Arc<dyn RetryPolicy>>,
}

impl StatementConfig {
    /// Determines the consistency of a query
    #[must_use]
    pub(crate) fn determine_consistency(&self, default_consistency: Consistency) -> Consistency {
        self.consistency.unwrap_or(default_consistency)
    }
}

#[derive(Debug, Clone, Copy, Error)]
#[error("Invalid page size provided: {0}; valid values are [1, i32::MAX]")]
pub struct InvalidPageSize(i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageSize(i32);

impl PageSize {
    #[inline]
    pub fn new(size: i32) -> Result<Self, InvalidPageSize> {
        if size > 0 {
            Ok(Self(size))
        } else {
            Err(InvalidPageSize(size))
        }
    }

    /// Only for use in compile-time evaluated code.
    pub(crate) const fn new_const(size: i32) -> Self {
        assert!(size > 0, "page size must be positive");
        Self(size)
    }

    #[inline]
    pub fn inner(&self) -> i32 {
        self.0
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(DEFAULT_PAGE_SIZE)
    }
}

impl TryFrom<i32> for PageSize {
    type Error = InvalidPageSize;

    #[inline]
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Into<i32> for PageSize {
    #[inline]
    fn into(self) -> i32 {
        self.inner()
    }
}
