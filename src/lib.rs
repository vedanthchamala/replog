//! replog: a Kafka-style replicated commit log, built from scratch.
//!
//! Stage 1 is the single-node storage engine (`storage`): a partition is a
//! directory of segment files, records are CRC-protected, recovery truncates
//! torn tails, and fsync policy is an explicit, measured choice.

pub mod broker;
pub mod checker;
pub mod controller;
pub mod client;
pub mod harness;
pub mod proto;
pub mod storage;
pub mod torture;
