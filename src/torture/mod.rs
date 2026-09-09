//! Target-agnostic fault-injection harness (Stage 6).
//!
//! Stage 5's torture harness could only judge replog: its fault backend was
//! `kill -9` on child processes plus a proxy on the controller link, and its
//! workload spoke replog's own protocol. Every verdict was a statement about
//! one system with no reference point.
//!
//! This module splits the harness into pieces that vary independently:
//!
//! - a **fault backend** ([`docker::Docker`]): how a broker is killed, frozen,
//!   or cut off from its peers — identical for every containerized target;
//! - a **target** ([`cluster::FaultTarget`]): the cluster under test as a
//!   *client* can observe it (metadata-visible leaders, health), plus the
//!   fault backend wired to its containers;
//! - a **workload** ([`workload::Workload`]): how the target's client protocol
//!   is spoken to produce ids and read them back into the checker's history;
//! - the **schedule** ([`schedule`]) and **failover recorder**
//!   ([`failover`]): seeded fault sequence, per-fault timing split, per-second
//!   ack timeline — the same code for every target, so two systems' numbers
//!   mean the same thing.

pub mod cluster;
pub mod docker;
pub mod failover;
pub mod kmeta;
pub mod probe;
pub mod replog;
pub mod schedule;
pub mod workload;

#[cfg(feature = "kafka")]
pub mod kafka;
