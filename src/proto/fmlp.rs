use crate::{
    rsrc::{System, Mutex, Usage},
    sharing::{ObliviousAnalyzer, Bound, BoundBlocking, Limits, ObliviousData}
};

/// Block, Leontyen, Brandenburg and Anderson's _Flexible Multiprocessor Locking Protocol_ (FMLP).
pub struct FlexibleMulti;

/// Implements the analysis for FMLP as described in [10.1109/RTCSA.2007.8](https://doi.org/10.1109/RTCSA.2007.8),
/// with short requests only.
impl ObliviousAnalyzer<Mutex> for FlexibleMulti {
    fn pass(&self, task: usize, sys: &System<Mutex>, by_rsrc: &[Usage<Mutex>]) -> ObliviousData {
        sys.reqs_by(task).iter().zip(by_rsrc)
           .map(|(req, rset)| {
            if req.num == 0 {
                return Bound::default();
            }

            // every other task may block once per request
            let limits = Limits { total: sys.num_tasks() - 1, per_task: 1 };

            rset.iter().copied().bound_blocking(task, limits * req.num)
        }).sum::<Bound>().into()
    }

    fn name(&self) -> &'static str { "FMLP" }
}
