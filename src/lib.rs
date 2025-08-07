extern crate qsv_docopt as docopt;

pub use clitypes::{CURRENT_COMMAND, CliError, CliResult, QsvExitCode};
pub use config::SPONSOR_MESSAGE;
pub use docopt::Docopt;

// #[cfg(feature = "jemallocator")]
// #[global_allocator]
// pub static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// #[cfg(feature = "mimalloc")]
// #[global_allocator]
// pub static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod clitypes;
pub mod cmd;
pub mod config;
pub mod index;
pub mod lookup;
pub mod odhtcache;
pub mod select;
pub mod util;
