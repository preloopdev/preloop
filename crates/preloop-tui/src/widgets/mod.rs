//! Shared drawing blocks.
//!
//! Widgets draw from their inputs and never change app state. A widget that
//! needs to remember something (a cursor, a filter, a fold) keeps that state in
//! a small struct the caller owns, and the caller passes it in.

pub mod chrome;
pub mod columns;
pub mod gauge;
pub mod kv;
pub mod list;
pub mod log_view;
pub mod modal;
pub mod palette;
pub mod sparkline;
pub mod text_input;
pub mod toast;
