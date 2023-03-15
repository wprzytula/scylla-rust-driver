use bytes::Bytes;

use crate::frame::response::result::{ColumnSpec, RawRows, ResultMetadata};

use super::row::{mk_deser_err, BuiltinDeserializationErrorKind, ColumnIterator, DeserializeRow};
use super::{DeserializationError, FrameSlice, TypeCheckError};
use std::marker::PhantomData;

/// Iterates over the whole result, returning rows.
pub struct RowIterator<'frame> {
    specs: &'frame [ColumnSpec],
    remaining: usize,
    slice: FrameSlice<'frame>,
}

impl<'frame> RowIterator<'frame> {
    /// Creates a new iterator over rows from a serialized response.
    ///
    /// - `remaining` - number of the remaining rows in the serialized response,
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a [FrameSlice] that points to the serialized rows data.
    #[inline]
    pub fn new(remaining: usize, specs: &'frame [ColumnSpec], slice: FrameSlice<'frame>) -> Self {
        Self {
            specs,
            remaining,
            slice,
        }
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'frame [ColumnSpec] {
        self.specs
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.remaining
    }
}

impl<'frame> Iterator for RowIterator<'frame> {
    type Item = Result<ColumnIterator<'frame>, DeserializationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        let iter = ColumnIterator::new(self.specs, self.slice);

        // Skip the row here, manually
        for (column_index, spec) in self.specs.iter().enumerate() {
            if let Err(err) = self.slice.read_cql_bytes() {
                return Some(Err(mk_deser_err::<Self>(
                    BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                        column_index,
                        column_name: spec.name.clone(),
                        err: DeserializationError::new(err),
                    },
                )));
            }
        }

        Some(Ok(iter))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // The iterator will always return exactly `self.remaining`
        // elements: Oks until an error is encountered and then Errs
        // containing that same first encountered error.
        (self.remaining, Some(self.remaining))
    }
}

/// A typed version of [RowIterator] which deserializes the rows before
/// returning them.
pub struct TypedRowIterator<'frame, R> {
    inner: RowIterator<'frame>,
    _phantom: PhantomData<R>,
}

impl<'frame, R> TypedRowIterator<'frame, R>
where
    R: DeserializeRow<'frame>,
{
    /// Creates a new [TypedRowIterator] from given [RowIterator].
    ///
    /// Calls `R::type_check` and fails if the type check fails.
    #[inline]
    pub fn new(raw: RowIterator<'frame>) -> Result<Self, TypeCheckError> {
        R::type_check(raw.specs())?;
        Ok(Self {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'frame [ColumnSpec] {
        self.inner.specs()
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.inner.rows_remaining()
    }
}

impl<'frame, R> Iterator for TypedRowIterator<'frame, R>
where
    R: DeserializeRow<'frame>,
{
    type Item = Result<R, DeserializationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|raw| raw.and_then(|raw| R::deserialize(raw)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

// Technically not an iterator because it returns items that borrow from it,
// and the std Iterator interface does not allow for that.
/// A _lending_ iterator over serialized rows.
///
/// This type is similar to `RowIterator`, but keeps ownership of the serialized
/// result. Because it returns `ColumnIterator`s that need to borrow from it,
/// it does not implement the `Iterator` trait (there is no type in the standard
/// library to represent this concept yet).
#[derive(Debug)]
pub struct RawRowsLendingIterator {
    metadata: ResultMetadata,
    remaining: usize,
    at: usize,
    raw_rows: Bytes,
}

impl RawRowsLendingIterator {
    /// Creates a new `RawRowsLendingIterator`, consuming given `RawRows`.
    #[inline]
    pub fn new(raw_rows: RawRows) -> Self {
        let (metadata, rows_count, raw_rows) = raw_rows.into_inner();
        Self {
            metadata,
            remaining: rows_count,
            at: 0,
            raw_rows,
        }
    }

    /// Returns a `ColumnIterator` that represents the next row.
    ///
    /// Note: the `ColumnIterator` borrows from the `RawRowsLendingIterator`.
    /// The column iterator must be consumed before the rows iterator can
    /// continue.
    #[inline]
    #[allow(clippy::should_implement_trait)] // https://github.com/rust-lang/rust-clippy/issues/5004
    pub fn next(&mut self) -> Option<Result<ColumnIterator, DeserializationError>> {
        self.remaining = self.remaining.checked_sub(1)?;

        // First create the slice encompassing the whole frame.
        let mut remaining_frame = FrameSlice::new(&self.raw_rows);
        // Then slice it to encompass the remaining suffix of the frame.
        *remaining_frame.as_slice_mut() = &remaining_frame.as_slice()[self.at..];
        // Ideally, we would prefer to preserve the FrameSlice between calls to `next()`,
        // but borrowing from oneself is impossible, so we have to recreate it this way.

        /* RowIterator code begins */
        let iter = ColumnIterator::new(&self.metadata.col_specs, remaining_frame);

        // Skip the row here, manually
        for (column_index, spec) in self.metadata.col_specs.iter().enumerate() {
            if let Err(err) = remaining_frame.read_cql_bytes() {
                return Some(Err(mk_deser_err::<Self>(
                    BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                        column_index,
                        column_name: spec.name.clone(),
                        err: DeserializationError::new(err),
                    },
                )));
            }
        }

        Some(Ok(iter))
        /* RowIterator code ends */

        /* Old RawRowsLendingIterator code */
        /*
        let mut mem = &self.raw_rows[self.at..];

        // Skip the row here, manually
        for _ in 0..self.metadata.col_specs.len() {
            if let Err(err) = types::read_bytes_opt(&mut mem) {
                return Some(Err(err));
            }
        }

        let slice = FrameSlice::new_subslice(&self.raw_rows[self.at..], &self.raw_rows);
        let iter = ColumnIterator::new(&self.metadata.col_specs, slice);
        self.at = self.raw_rows.len() - mem.len();
        Some(Ok(iter))
        */
        /* End RawRowsLendingIterator code */
    }

    #[inline]
    pub fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }

    /// Returns the metadata associated with the response (paging state and
    /// column specifications).
    #[inline]
    pub fn metadata(&self) -> &ResultMetadata {
        &self.metadata
    }

    /// Returns the remaining number of rows that this iterator is expected
    /// to produce.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.remaining
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::frame::response::result::ColumnType;

    use super::super::tests::{serialize_cells, spec, CELL1, CELL2};
    use super::{FrameSlice, RowIterator, TypedRowIterator};

    #[test]
    fn test_row_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let mut iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));

        let mut row1 = iter.next().unwrap().unwrap();
        let c11 = row1.next().unwrap().unwrap();
        assert_eq!(c11.slice.unwrap().as_slice(), CELL1);
        let c12 = row1.next().unwrap().unwrap();
        assert_eq!(c12.slice.unwrap().as_slice(), CELL2);
        assert!(row1.next().is_none());

        let mut row2 = iter.next().unwrap().unwrap();
        let c21 = row2.next().unwrap().unwrap();
        assert_eq!(c21.slice.unwrap().as_slice(), CELL2);
        let c22 = row2.next().unwrap().unwrap();
        assert_eq!(c22.slice.unwrap().as_slice(), CELL1);
        assert!(row2.next().is_none());

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_row_iterator_too_few_rows() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let mut iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));

        iter.next().unwrap().unwrap();
        assert!(iter.next().unwrap().is_err());
    }

    #[test]
    fn test_typed_row_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));
        let mut iter = TypedRowIterator::<'_, (&[u8], Vec<u8>)>::new(iter).unwrap();

        let (c11, c12) = iter.next().unwrap().unwrap();
        assert_eq!(c11, CELL1);
        assert_eq!(c12, CELL2);

        let (c21, c22) = iter.next().unwrap().unwrap();
        assert_eq!(c21, CELL2);
        assert_eq!(c22, CELL1);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_typed_row_iterator_wrong_type() {
        let raw_data = Bytes::new();
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let iter = RowIterator::new(0, &specs, FrameSlice::new(&raw_data));
        assert!(TypedRowIterator::<'_, (i32, i64)>::new(iter).is_err());
    }
}
