use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct InserterError<E: Error> {
    source: E,
}

impl<E: Error> InserterError<E> {
    pub const fn new(source: E) -> Self {
        Self { source }
    }

    pub fn into_inner(self) -> E {
        self.source
    }
}

impl<E: Error> fmt::Display for InserterError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inserter error: {}", self.source)
    }
}

impl<E: Error + 'static> Error for InserterError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl<E: Error> From<E> for InserterError<E> {
    fn from(source: E) -> Self {
        Self::new(source)
    }
}
