#![allow(
    clippy::redundant_closure,
    clippy::result_large_err,
    clippy::unnecessary_lazy_evaluations,
    clippy::useless_conversion
)]

pub mod cc_backend;
pub mod cc_index_store;
pub mod cc_op_heads_store;
pub mod cc_op_store;
pub mod cc_working_copy;
pub mod repo;
pub mod util;

pub use repo::StoreFactoriesExt;
