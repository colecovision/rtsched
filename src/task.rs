//! The task model.

use dashu::{
    rational::Relaxed,
    integer::Sign
};

/// Type of time instants and durations.
///
/// The intended semantics of this type are left to the user and do not
/// interfere with use of this library as long as it relates to integral
/// multiples of a constant time interval; in most cases it is an integral
/// number of milliseconds or microseconds.
pub type Time = u64;

/// A single task.
#[derive(Clone, Copy)]
pub struct Task {
    /// The task's period.
    pub period: Time,
    /// The task's cost, also known as WCET (worst-case execution time).
    pub cost: Time,
    /// The task's absolute deadline.
    pub deadline: Time,
    /// The task's priority, if defined.
    pub priority: u64
}

impl Task {
    /// Constructs a new `Task` with the given `cost` and `period`, implicit deadline
    /// (equal to `period`) and maximum priority.
    pub fn new(cost: Time, period: Time) -> Self {
        Self {
            period,
            cost,
            deadline: period,
            priority: 0
        }
    }

    /// Returns the task with new deadline `deadline`.
    pub fn with_deadline(self, deadline: Time) -> Self {
        Self { deadline, ..self }
    }

    /// Returns the task with EDF-like priority.
    ///
    /// This priority is not correct for simulation purposes; it is equal to the
    /// task's _absolute_ deadline (i.e. [`deadline`](`Task::deadline`)) and not
    /// its _relative_ deadline as used by EDF schedulers. This priority measure
    /// is nevertheless used to compute arrival blocking for certain classes of
    /// protocols when run under EDF, hence its name.
    pub fn edf(self) -> Self {
        Self { priority: self.deadline, ..self }
    }
}

/// Trait for tasks and collections of tasks (task-sets).
pub trait Set {
    /// Returns the exact value of the total utilization of the task-set.
    /// 
    /// For this value to be computed exactly it is required that the return type
    /// be an arbitrary-precision (non-negative) rational; since its main use is to be
    /// summed or compared, it is not immediately returned as a
    /// [`RBig`](`dashu::rational::RBig`); use [`Relaxed::canonicalize`] to convert
    /// to it if needed.
    fn utilization(self) -> Relaxed;

    /// Tests if the task-set has all implicit tasks, i.e. if all their deadlines
    /// are equal to their periods.
    fn implicit(self) -> bool;

    /// Tests if the task-set is feasible, i.e. if its utilization is no greater than `1`.
    fn feasible(self) -> bool;
}

/// A `Task` is in and of itself a `Set` of one element and is treated accordingly.
impl Set for &'_ Task {
    fn utilization(self) -> Relaxed {
        Relaxed::from_parts_const(
            Sign::Positive,
            self.cost.into(),
            self.period.into()
        )
    }

    fn implicit(self) -> bool {
        self.period == self.deadline
    }

    fn feasible(self) -> bool {
        self.cost <= self.period
    }
}

/// Any collection of `Set`s (including [`Task`]) is a `Set`, and is treated as if each
/// of its elements were a task.
///
/// Note that, while `feasible` and `implicit` are correct for a `Set` of `Set`s if it
/// represents a cluster, `utilization` will _sum_ the utilizations of each cluster.
/// This may or may not be desirable; to prevent this, call `utilization` on each `Set`
/// individually and [collect](`Iterator::collect`) the result.
impl<I, T: Set> Set for I where I: IntoIterator<Item = T> {
    fn utilization(self) -> Relaxed {
        let mut out = Relaxed::default();

        for x in self {
            out += x.utilization();
        }

        out
    }

    fn implicit(self) -> bool {
        self.into_iter()
            .all(T::implicit)
    }

    fn feasible(self) -> bool {
        self.into_iter()
            .all(T::feasible)
    }
}
