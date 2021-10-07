//! Internal errors for the `GenericBPlusTree` data structure
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// Optimistic validation failed, stack must unwind to a safe state.
    #[error("optimistic validation failed")]
    Unwind,
    /// Node was retired from the data structure during the operation, try again with a different
    /// node
    #[error("called find_parent on reclaimed node")]
    Reclaimed
}

pub type Result<T> = std::result::Result<T, Error>;
