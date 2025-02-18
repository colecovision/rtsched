use crate::{
    rsrc::{System, Mutex, Rw, RwPair, Usage},
    sharing::{ObliviousAnalyzer, Bound, BoundBlocking, Limits, ObliviousData}
};

/// The global version of Ahmed and Anderson's _Optimal Locking Protocol for FIFO_ family (OLP-F).
pub struct OptimalFIFO {
    num_cpus: usize
}

impl OptimalFIFO {
    /// Constructs a new instance of `OptimalFIFO` with the given number of CPUs.
    pub fn new(num_cpus: usize) -> Self {
        Self { num_cpus }
    }
}

/// Implements the analysis for OLP-F as described in [10.4230/LIPIcs.ECRTS.2023.16](https://doi.org/10.4230/LIPIcs.ECRTS.2023.16).
impl ObliviousAnalyzer<Mutex> for OptimalFIFO {
    fn pass(&self, task: usize, sys: &System<Mutex>, by_rsrc: &[Usage<Mutex>]) -> ObliviousData {
        sys.reqs_by(task).iter().zip(by_rsrc)
           .map(|(req, rset)| {
            if req.num == 0 {
                return Bound::default();
            }

            // every other scheduled task may block once per request
            let limits = Limits { total: self.num_cpus - 1, per_task: 1 };

            rset.iter().copied().bound_blocking(task, limits * req.num)
        }).sum::<Bound>().into()
    }

    fn name(&self) -> &'static str { "OLP-F" }
}

/// Implements the analysis for RW-OLP-F as described in [10.4230/LIPIcs.ECRTS.2023.16](https://doi.org/10.4230/LIPIcs.ECRTS.2023.16).
impl ObliviousAnalyzer<Rw> for OptimalFIFO {
    fn pass(&self, task: usize, sys: &System<Rw>, by_rsrc: &[Usage<Rw>]) -> ObliviousData {
        if self.num_cpus <= 1 {
            return ObliviousData::default();
        }

        sys.reqs_by(task).iter().zip(by_rsrc)
           .map(|(RwPair { read, write }, RwPair { read: reads, write: writes })| {
            if read.num == 0 && write.num == 0 {
                return Bound::default();
            }

            // maximum read bound
            let rbound = reads.iter().copied().bound_blocking(task, Limits { total: 1, per_task: 1 });

            let rtotal = if read.num > 0 {
                writes.iter().copied().bound_blocking(task, Limits {
                    total: write.num, per_task: write.num
                }) + if rbound.length > 0 {
                    let rbound = Bound { length: rbound.length - 1, count: rbound.count };
                    rbound * read.num
                } else {
                    Bound::default()
                }
            } else {
                Bound::default()
            };

            let wsingle = if write.num > 0 {
                let case1lim = Limits { total: self.num_cpus - 1, per_task: 1 };

                let case1 = writes.iter().copied().bound_blocking(task, case1lim);

                if case1.count < case1lim.total || rbound.length == 0 {
                    // not enough writers to fill WQ or no other readers
                    case1 + (case1.count + 1) * rbound
                } else {
                    let case2lim = Limits { total: self.num_cpus - 2, per_task: 1 };
                    let case2 = writes.iter().copied().bound_blocking(task, case2lim);

                        (case1 + (self.num_cpus - 2) * rbound)
                    .max(case2 + (self.num_cpus - 1) * rbound)
                }
            } else {
                Bound::default()
            };

            rtotal + wsingle * write.num
        }).sum::<Bound>().into()
    }

    fn name(&self) -> &'static str { "RW-OLP-F" }
}
