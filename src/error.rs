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

pub trait NonOptimisticExt<T> {
    fn unopt(self) -> T;
}

impl<T> NonOptimisticExt<T> for Result<T> {
    fn unopt(self) -> T {
        debug_assert!(self.is_ok());
        unsafe { self.unwrap_unchecked() }
    }
}

#[derive(Error, Debug)]
pub enum BufMgrError {
    /// Requested size class is not a valid size class
    #[error("requested size class is invalid")]
    InvalidSizeClass,

    /// Size class ran out of frames
    #[error("the frames for the size class {0} were depleted")]
    OutOfFrames(usize),

    /// Io operation failed
    #[error("io operation failed with {0}")]
    Io(#[from] nix::Error)
}
