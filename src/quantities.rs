#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quantities {
    pub rows: u64,
    pub transactions: u64,
}

impl Quantities {
    pub const ZERO: Self = Self {
        rows: 0,
        transactions: 0,
    };

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }
}
