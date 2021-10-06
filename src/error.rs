use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("optimistic validation failed")]
    Unwind,
    #[error("called find_parent on reclaimed node")]
    Reclaimed
}

pub type Result<T> = std::result::Result<T, Error>;
