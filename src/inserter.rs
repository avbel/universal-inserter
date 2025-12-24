use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

use crate::error::InserterError;
use crate::quantities::Quantities;
use crate::ticks::Ticks;

type CommitCallback = Box<dyn FnMut(&Quantities) + Send>;

pub struct Inserter<T, F, Fut, E>
where
    F: FnMut(Vec<T>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Error,
{
    insert_fn: F,
    max_rows: u64,
    buffer: Vec<T>,
    ticks: Ticks,
    pending: Quantities,
    committed: Quantities,
    in_transaction: bool,
    on_commit: Option<CommitCallback>,
    _phantom: PhantomData<(Fut, E)>,
}

impl<T, F, Fut, E> Inserter<T, F, Fut, E>
where
    F: FnMut(Vec<T>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Error,
{
    #[must_use]
    pub fn new(insert_fn: F) -> Self {
        Self {
            insert_fn,
            max_rows: u64::MAX,
            buffer: Vec::new(),
            ticks: Ticks::new(),
            pending: Quantities::ZERO,
            committed: Quantities::ZERO,
            in_transaction: false,
            on_commit: None,
            _phantom: PhantomData,
        }
    }

    #[must_use]
    pub const fn with_max_rows(mut self, max_rows: u64) -> Self {
        self.max_rows = max_rows;
        self
    }

    #[must_use]
    pub const fn with_period(mut self, period: Duration) -> Self {
        self.ticks = self.ticks.with_period(period);
        self
    }

    #[cfg(feature = "period_bias")]
    #[must_use]
    pub fn with_period_bias(mut self, bias: f64) -> Self {
        self.ticks = self.ticks.with_bias(bias);
        self
    }

    #[must_use]
    pub fn with_commit_callback<C>(mut self, callback: C) -> Self
    where
        C: FnMut(&Quantities) + Send + 'static,
    {
        self.on_commit = Some(Box::new(callback));
        self
    }

    #[must_use]
    pub const fn pending(&self) -> &Quantities {
        &self.pending
    }

    #[must_use]
    pub fn time_left(&self) -> Option<Duration> {
        self.ticks.time_left()
    }

    fn limits_reached(&self) -> bool {
        self.pending.rows >= self.max_rows || self.ticks.reached()
    }

    fn start_if_needed(&mut self) {
        self.ticks.start();
    }

    pub fn write_owned(&mut self, item: T) {
        self.start_if_needed();

        self.buffer.push(item);
        self.pending.rows += 1;

        if !self.in_transaction {
            self.pending.transactions += 1;
            self.in_transaction = true;
        }
    }

    async fn flush(&mut self) -> Result<Quantities, InserterError<E>> {
        if self.buffer.is_empty() {
            return Ok(Quantities::ZERO);
        }

        let batch = std::mem::take(&mut self.buffer);
        let flushed = self.pending;

        (self.insert_fn)(batch).await.map_err(InserterError::new)?;

        self.committed.rows += flushed.rows;
        self.committed.transactions += flushed.transactions;
        self.pending = Quantities::ZERO;
        self.in_transaction = false;

        if let Some(ref mut callback) = self.on_commit {
            callback(&flushed);
        }

        Ok(flushed)
    }

    /// Checks limits and flushes if reached.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert function fails.
    pub async fn commit(&mut self) -> Result<Quantities, InserterError<E>> {
        if !self.limits_reached() {
            self.in_transaction = false;
            return Ok(Quantities::ZERO);
        }

        self.force_commit().await
    }

    /// Flushes unconditionally, regardless of limits.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert function fails.
    pub async fn force_commit(&mut self) -> Result<Quantities, InserterError<E>> {
        let result = self.flush().await?;
        self.ticks.reschedule();
        Ok(result)
    }

    /// Consumes the inserter and flushes remaining buffered items.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert function fails.
    pub async fn end(mut self) -> Result<Quantities, InserterError<E>> {
        self.flush().await?;
        Ok(self.committed)
    }
}

impl<T, F, Fut, E> Inserter<T, F, Fut, E>
where
    T: Clone,
    F: FnMut(Vec<T>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Error,
{
    pub fn write(&mut self, item: &T) {
        self.write_owned(item.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::io;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq)]
    struct TestRow {
        id: u64,
    }

    #[test]
    fn test_basic_insert() {
        pollster::block_on(async {
            let inserted: Rc<RefCell<Vec<Vec<TestRow>>>> = Rc::new(RefCell::new(Vec::new()));
            let inserted_clone = Rc::clone(&inserted);

            let mut inserter = Inserter::new(move |batch: Vec<TestRow>| {
                let inserted = Rc::clone(&inserted_clone);
                async move {
                    inserted.borrow_mut().push(batch);
                    Ok::<_, io::Error>(())
                }
            })
            .with_max_rows(2);

            inserter.write(&TestRow { id: 1 });
            inserter.write(&TestRow { id: 2 });

            let stats = inserter.commit().await.unwrap();
            assert_eq!(stats.rows, 2);

            let batches = inserted.borrow();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].len(), 2);
        });
    }

    #[test]
    fn test_force_commit() {
        pollster::block_on(async {
            let inserted: Rc<RefCell<Vec<Vec<TestRow>>>> = Rc::new(RefCell::new(Vec::new()));
            let inserted_clone = Rc::clone(&inserted);

            let mut inserter = Inserter::new(move |batch: Vec<TestRow>| {
                let inserted = Rc::clone(&inserted_clone);
                async move {
                    inserted.borrow_mut().push(batch);
                    Ok::<_, io::Error>(())
                }
            })
            .with_max_rows(100);

            inserter.write(&TestRow { id: 1 });

            let stats = inserter.force_commit().await.unwrap();
            assert_eq!(stats.rows, 1);
        });
    }

    #[test]
    fn test_end() {
        pollster::block_on(async {
            let inserted: Rc<RefCell<Vec<Vec<TestRow>>>> = Rc::new(RefCell::new(Vec::new()));
            let inserted_clone = Rc::clone(&inserted);

            let mut inserter = Inserter::new(move |batch: Vec<TestRow>| {
                let inserted = Rc::clone(&inserted_clone);
                async move {
                    inserted.borrow_mut().push(batch);
                    Ok::<_, io::Error>(())
                }
            })
            .with_max_rows(100);

            inserter.write(&TestRow { id: 1 });
            inserter.write(&TestRow { id: 2 });
            inserter.write(&TestRow { id: 3 });

            let stats = inserter.end().await.unwrap();
            assert_eq!(stats.rows, 3);
            assert_eq!(stats.transactions, 1);
        });
    }

    #[test]
    fn test_commit_callback() {
        pollster::block_on(async {
            let callback_called: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
            let callback_clone = Arc::clone(&callback_called);

            let mut inserter =
                Inserter::new(|_batch: Vec<TestRow>| async move { Ok::<_, io::Error>(()) })
                    .with_max_rows(1)
                    .with_commit_callback(move |_stats| {
                        *callback_clone.lock().unwrap() = true;
                    });

            inserter.write(&TestRow { id: 1 });
            inserter.commit().await.unwrap();

            assert!(*callback_called.lock().unwrap());
        });
    }

    #[test]
    fn test_no_commit_when_below_limit() {
        pollster::block_on(async {
            let inserted: Rc<RefCell<Vec<Vec<TestRow>>>> = Rc::new(RefCell::new(Vec::new()));
            let inserted_clone = Rc::clone(&inserted);

            let mut inserter = Inserter::new(move |batch: Vec<TestRow>| {
                let inserted = Rc::clone(&inserted_clone);
                async move {
                    inserted.borrow_mut().push(batch);
                    Ok::<_, io::Error>(())
                }
            })
            .with_max_rows(10);

            inserter.write(&TestRow { id: 1 });
            inserter.write(&TestRow { id: 2 });

            let stats = inserter.commit().await.unwrap();
            assert_eq!(stats.rows, 0);

            let batches = inserted.borrow();
            assert!(batches.is_empty());
        });
    }
}
