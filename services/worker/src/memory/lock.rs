//! Memory fencing lock — implementation lives in `common::memlock` so the
//! gateway's dashboard delete honors the same protocol as worker runs.
pub use common::memlock::{acquire_lock, release_lock};
