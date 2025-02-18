//! Schedulability bounds and tests.

use crate::task::Set;

use num_order::NumOrd;

/// Tests whether task-set `ts` has bounded response times under global EDF
/// and/or global FIFO for a task-set with implicit deadlines.
///
/// Both schedulers have the same condition for boundedness (though they do
/// not in general have the same tardiness bounds).
///
/// Returns the result of the test if `ts` is implicit, otherwise `None`.
pub fn soft(ts: impl Set + Clone, num_cpus: usize) -> Option<bool> {
    ts.clone().implicit().then(|| {
        ts.clone().utilization().num_le(&num_cpus) && ts.feasible()
    })
}
