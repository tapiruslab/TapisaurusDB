//! Document model — the fundamental data unit in TapisaurusDB.
//!
//! Every piece of data in TapisaurusDB is a [`Document`] — a flexible,
//! schema-optional JSON/BSON-compatible structure that supports:
//!
//! - Nested objects and arrays
//! - Rich types (DateTime, UUID, Binary, Decimal, Vector)
//! - Auto-generated IDs (UUID v7 — time-sortable)
//! - Optional schema validation

pub mod collection;
pub mod index;
pub mod model;

pub use collection::Collection;
pub use index::{SecondaryIndex, SecondaryIndexDef};
pub use model::{Document, DocumentId, Value};
