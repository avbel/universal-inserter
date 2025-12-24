# universal-inserter

A runtime-agnostic Rust library implementing the batch inserter pattern (similar to ClickHouse's Inserter).
Buffers items and flushes them via a user-provided async insert function based on configurable limits.

## Features

- **Runtime-agnostic**: Works with Tokio, async-std, smol, or any async runtime
- **Row limit**: Flush when buffer reaches N items
- **Time period**: Flush after duration elapsed
- **Period bias**: Optional randomization to prevent synchronized flushes
- **Zero dependencies** by default (only `rand` for period_bias feature)

## Installation

```toml
[dependencies]
universal-inserter = "0.1"

# Optional: enable period bias randomization
universal-inserter = { version = "0.1", features = ["period_bias"] }
```

## Usage

### Basic Example

```rust
use universal_inserter::Inserter;
use std::time::Duration;

#[derive(Clone)]
struct MyRow {
    id: u64,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut inserter = Inserter::new(|batch: Vec<MyRow>| async move {
        println!("Inserting {} rows", batch.len());
        // Your insert logic here (database, API, file, etc.)
        Ok::<_, std::io::Error>(())
    })
    .with_max_rows(1000)
    .with_period(Duration::from_secs(5));

    // Write rows
    for i in 0..2500 {
        inserter.write(&MyRow { id: i, name: format!("row_{}", i) });
        inserter.commit().await?;  // Flushes if limits reached
    }

    // Flush remaining and close
    let stats = inserter.end().await?;
    println!("Total: {} rows, {} transactions", stats.rows, stats.transactions);

    Ok(())
}
```

### With Period Bias

```rust
use universal_inserter::Inserter;
use std::time::Duration;

let mut inserter = Inserter::new(insert_fn)
    .with_max_rows(500)
    .with_period(Duration::from_secs(10))
    .with_period_bias(0.2);  // ±20% randomization
```

### With Commit Callback

```rust
use universal_inserter::Inserter;

let mut inserter = Inserter::new(insert_fn)
    .with_max_rows(100)
    .with_commit_callback(|stats| {
        println!("Committed {} rows", stats.rows);
    });
```

### Force Commit

```rust
// Flush unconditionally, regardless of limits
let stats = inserter.force_commit().await?;
```

### Check Pending Stats

```rust
let pending = inserter.pending();
println!("Buffered: {} rows", pending.rows);

if let Some(time_left) = inserter.time_left() {
    println!("Next flush in: {:?}", time_left);
}
```

## API

### Inserter Methods

| Method | Description |
|--------|-------------|
| `new(insert_fn)` | Create inserter with async insert function |
| `with_max_rows(n)` | Set row limit (default: unlimited) |
| `with_period(duration)` | Set time-based flush interval |
| `with_period_bias(bias)` | Add randomization ±bias (requires `period_bias` feature) |
| `with_commit_callback(fn)` | Register callback after successful commits |
| `write(item)` | Add item to buffer (clones item) |
| `write_owned(item)` | Add item to buffer (moves item) |
| `commit()` | Check limits and flush if reached |
| `force_commit()` | Flush unconditionally |
| `end()` | Consume inserter and flush remaining |
| `pending()` | Get current buffer statistics |
| `time_left()` | Duration until next period tick |

## License

MIT
