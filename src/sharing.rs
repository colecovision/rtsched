//! Structures and traits for resource-sharing protocols and analysis.

use crate::{
    task::{Task, Set, Time},
    rsrc::{System, RequestKind, Usage, TaskRequest}
};

use dashu::{rational::Relaxed, integer::Sign};

use std::{
    ops::{Add, AddAssign, Mul},
    iter::Sum
};

/// A bound on the number and total length of requests that interfere
/// with requests from a given task to any resource.
///
/// Addition and multiplication (by `usize`) act elementwise.
#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Bound {
    /// Total length of time.
	pub length: Time,
    /// Total request count.
	pub count: usize
}

impl Bound {
    /// Constructs a new `Bound` relative to a single intefering request
    /// of length `length`.
    pub fn new(length: Time) -> Self {
        Self { count: 1, length }
    }
}

impl AddAssign<Bound> for Bound {
    fn add_assign(&mut self, other: Self) {
        self.count += other.count;
        self.length += other.length;
    }
}

impl Add for Bound {
    type Output = Bound;

    fn add(mut self, other: Self) -> Self {
        self += other;
        self
    }
}

impl Mul<usize> for Bound {
    type Output = Bound;

    fn mul(mut self, other: usize) -> Self::Output {
        self.count *= other;
        self.length *= other as u64;
        self
    }
}

impl Mul<Bound> for usize {
    type Output = Bound;

    fn mul(self, other: Bound) -> Self::Output {
        other * self
    }
}

impl Sum for Bound {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut out = Self::default();

        for x in iter {
            out += x;
        }

        out
    }
}

/// Result of a s-oblivious analysis on a [`System`].
#[derive(Default, Clone, Copy)]
pub struct ObliviousData {
    /// Bound on arrival blocking, if any.
    pub arrival: Bound,
    /// Bound on total request blocking.
    pub total:   Bound
}

/// Converts a `Bound` into an `ObliviousData`, interpreting it as
/// total request blocking with no arrival blocking.
impl From<Bound> for ObliviousData {
    fn from(total: Bound) -> Self {
        Self { total, arrival: Bound::default() }
    }
}

/// An analyzer that obtains bounds for requests of kind `K` from a task system.
///
/// Every analyzer works under the assumption of some _protocol_ being used for
/// resource sharing, whose analysis is in one way or another implemented and its
/// results returned to the user.
pub trait ObliviousAnalyzer<K: RequestKind> {
    /// Runs a single analysis pass on the task at index `task` of system `sys`.
    ///
    /// Resources are stored in task order in `sys` and retrievable using [`System::reqs_by`]
    /// as well as in `by_rsrc`, indexed by resource; it can be additional assumed that
    /// each (element of) `Usage<K>` has been sorted using
    /// [`RequestSet::sort_by_length`](`crate::rsrc::RequestSet::sort_by_length`),
    /// and thus [`BoundBlocking::bound_blocking`] returns valid bounds.
    fn pass(&self, task: usize, sys: &System<K>, by_rsrc: &[Usage<K>]) -> ObliviousData;

    /// Runs the final pass on all data returned by [`pass`](`Self::pass`).
    ///
    /// `out` contains the results for each task in `sys` in index order.
    ///
    /// The main use of this method is for computing arrival blocking for protocols that
    /// require it; **the default implementation is a no-op**.
    fn post(&self, _sys: &System<K>, _out: &mut [ObliviousData]) {}

    /// Returns the name of the protocol (or analyzer).
    ///
    /// It is recommended that this method return a constant string; due to `const fn` not
    /// being supported in traits yet, and for object-safety reasons, this requirement
    /// cannot be checked statically.
    fn name(&self) -> &'static str;
}

/// Limits for request counts to be used by [`BoundBlocking::bound_blocking`].
///
/// Multiplication (by `usize`) acts elementwise.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Total request limit for the entire set of requests.
    pub total: usize,
    /// Request limit for a single task.
    pub per_task: usize
}

impl Mul<usize> for Limits {
    type Output = Limits;

    fn mul(mut self, other: usize) -> Self {
        self.total *= other;
        self.per_task *= other;
        self
    }
}

/// Extension trait to compute blocking bounds for a set of requests.
pub trait BoundBlocking {
    /// Computes blocking bounds for a set of requests. The set must
    /// be ordered in **decreasing** order of length and must contain
    /// at most one entry per task.
    ///
    /// For every task except the one with index `task`, sums up at
    /// most `limits.total` requests, and at most `limits.per_task` for
    /// a single task.
    fn bound_blocking(self, task: usize, limits: Limits) -> Bound;
}

/// The sole implementation of `BoundBlocking`, for iterators of [`TaskRequest`]s.
impl<'a, I> BoundBlocking for I where I: IntoIterator<Item = TaskRequest<'a>> {
    fn bound_blocking(self, task: usize, limits: Limits) -> Bound {
        let mut inter = Bound::default();
        
        for TaskRequest { task: other, req } in self {
            if other == task {
                continue;
            }

            let remaining = limits.total - inter.count;

            if remaining == 0 {
                break;
            }

            let num = remaining.min(limits.per_task);
            inter += num * Bound::new(req.length);
        }

        inter
    }
}

/// Helper struct for a task with s-oblivious analysis results.
///
/// This struct implements `From<(&Task, &ObliviousData)>` which is the only
/// intended method for its construction and describes its internal structure.
/// The purpose of this structure is to allow calls on an iterator or container
/// of (both or either) [`Task`]s and [`ObliviousData`] to act as a [`Set`].
pub struct ObliviousTask<'a, 'b> {
    task: &'a Task,
    data: &'b ObliviousData
}

impl<'a, 'b> From<(&'a Task, &'b ObliviousData)> for ObliviousTask<'a, 'b> {
    fn from(x: (&'a Task, &'b ObliviousData)) -> Self {
        Self { task: x.0, data: x.1 }
    }
}

/// An `ObliviousTask` acts exactly like the [`Task`] it is converted from, except
/// that its cost is computed as the task's [`cost`](`Task::cost`) plus the
/// [`ObliviousData`]'s [`total`](`ObliviousData::total`) [`length`](`Bound::length`).
impl Set for ObliviousTask<'_, '_> {
    fn utilization(self) -> Relaxed {
        Relaxed::from_parts_const(
            Sign::Positive,
            (self.task.cost + self.data.total.length).into(),
            self.task.period.into()
        )
    }

    fn implicit(self) -> bool {
        self.task.period == self.task.deadline
    }

    fn feasible(self) -> bool {
        self.task.cost + self.data.total.length <= self.task.period
    }
}
