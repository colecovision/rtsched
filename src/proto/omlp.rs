use crate::{
    rsrc::{System, Mutex, Rw, RwPair, Usage},
    sharing::{ObliviousAnalyzer, Bound, BoundBlocking, Limits, ObliviousData}
};

/// The global version of Brandenburg and Anderson's _O(m) Locking Protocol_ (OMLP).
pub struct GlobalOm {
    num_cpus: usize
}
        
impl GlobalOm {
    /// Constructs a new instance of `GlobalOm` with the given number of CPUs.
    pub fn new(num_cpus: usize) -> Self {
        Self { num_cpus }
    }
}

/// Implements the analysis for the global version of OMLP as described
/// in the appendix of [10.1109/RTSS.2010.17](https://doi.org/10.1109/RTSS.2010.17).
impl ObliviousAnalyzer<Mutex> for GlobalOm {
    fn pass(&self, task: usize, sys: &System<Mutex>, by_rsrc: &[Usage<Mutex>]) -> ObliviousData {
        sys.reqs_by(task).iter().zip(by_rsrc)
           .map(|(req, rset)| {
            if req.num == 0 {
                return Bound::default();
            }

            let nreqs = rset.iter().filter(|tr| tr.req.num > 0).count();

            let limits = if nreqs <= self.num_cpus + 1 {
                // only FIFO component is ever active
                Limits { total: nreqs - 1, per_task: 1 }
            } else {
                // twice per request (as per dissertation)
                Limits { total: 2 * self.num_cpus - 1, per_task: 2 }
            };

            rset.iter().copied().bound_blocking(task, limits * req.num)
        }).sum::<Bound>().into()
    }

    fn name(&self) -> &'static str { "OMLP" }
}

/// The clustered version of Brandenburg and Anderson's _O(m) Locking Protocol_ family
/// (C-OMLP), specialized for the single-cluster case.
pub struct SingleClusterOm {
    num_cpus: usize
}
        
impl SingleClusterOm {
    /// Constructs a new instance of `SingleClusterOm` with the given number of CPUs.
    pub fn new(num_cpus: usize) -> Self {
        Self { num_cpus }
    }
}

/// Implements the analysis for C-OMLP as described in [10.1145/2038642.2038655](https://doi.org/10.1145/2038642.2038655),
/// specialized for the single-cluster case.
impl ObliviousAnalyzer<Mutex> for SingleClusterOm {
    fn pass(&self, task: usize, sys: &System<Mutex>, by_rsrc: &[Usage<Mutex>]) -> ObliviousData {
        let mut out = ObliviousData::default();

        for (req, rset) in sys.reqs_by(task).iter().zip(by_rsrc) {
            if req.num == 0 {
                continue;
            }

            let limits = Limits { total: self.num_cpus - 1, per_task: 1 };
            let total = rset.iter().copied().bound_blocking(task, limits * req.num);

            let arrival = if req.num == 1 {
                // already single request, counts for arrival blocking
                total
            } else {
                // compute as if req.num == 1 (single request)
                rset.iter().copied().bound_blocking(task, limits)
            };

            // count our own request too
            let arrival = arrival + Bound::new(req.length);

            out.total += total;
            out.arrival = out.arrival.max(arrival);
        }

        out
    }

    fn post(&self, sys: &System<Mutex>, out: &mut [ObliviousData]) {
        // add priority donation term
        for task in 0 .. sys.num_tasks() {
            out[task].total += (0 .. sys.num_tasks())
                               .filter(|i| *i != task
                                        && sys.task(*i).priority <= sys.task(task).priority)
                               .map(|i| out[i].arrival)
                               .max()
                               .unwrap_or_default();
        }
    }

    fn name(&self) -> &'static str { "C-OMLP" }
}

/// Implements the analysis for CRW-OMLP as described in [10.1145/2038642.2038655](https://doi.org/10.1145/2038642.2038655),
/// specialized for the single-cluster case.
impl ObliviousAnalyzer<Rw> for SingleClusterOm {
    fn pass(&self, task: usize, sys: &System<Rw>, by_rsrc: &[Usage<Rw>]) -> ObliviousData {
        if self.num_cpus == 1 {
            // no interference for single-cpu execution
            return ObliviousData::default();
        }

        let mut out = ObliviousData::default();

        for (RwPair { read, write }, RwPair { read: reads, write: writes }) in sys.reqs_by(task).iter().zip(by_rsrc) {
            if read.num == 0 && write.num == 0 {
                continue;
            }

            let wlimits = Limits {
                total: read.num + write.num * (self.num_cpus - 1),
                per_task: read.num + write.num
            };

            let wtotal = writes.iter().copied().bound_blocking(task, wlimits);

            let rlimit = wlimits.total.min(wtotal.count + write.num);
            let rtotal = reads.iter().copied().bound_blocking(task, Limits { total: rlimit, per_task: rlimit });

            let warrival = if write.num == 1 && read.num == 0 {
                // already single request, counts for arrival blocking
                wtotal + rtotal
            } else if write.num > 0 {
                // compute as if write.num == 1 and read.num == 0 (single request)
                let warr = writes.iter().copied().bound_blocking(task, Limits {
                    total: self.num_cpus - 1, per_task: 1
                });
                let rlimit = (self.num_cpus - 1).min(warr.count + 1);
                warr + reads.iter().copied().bound_blocking(task, Limits { total: rlimit, per_task: rlimit })
            } else {
                // no arrival blocking for writes for this task
                Bound::default()
            };

            let rarrival = if read.num == 1 && write.num == 0 {
                // already single request, counts for arrival blocking
                wtotal + rtotal
            } else if read.num > 0 {
                // compute as if read.num == 1 and write.num == 0 (single request)
                let warr = writes.iter().copied().bound_blocking(task, Limits { total: 1, per_task: 1 });
                let rlimit = warr.count.min(1);
                warr + reads.iter().copied().bound_blocking(task, Limits { total: rlimit, per_task: rlimit })
            } else {
                // no arrival blocking for reads for this task
                Bound::default()
            };

            // count our own requests too

            let warrival = warrival + if write.num > 0 {
                Bound::new(write.length)
            } else {
                Bound::default()
            };

            let rarrival = rarrival + if read.num > 0 {
                Bound::new(read.length)
            } else {
                Bound::default()
            };

            out.total += rtotal + wtotal;
            out.arrival = out.arrival.max(rarrival + warrival);
        }

        out
    }

    fn post(&self, sys: &System<Rw>, out: &mut [ObliviousData]) {
        // add priority donation term
        for task in 0 .. sys.num_tasks() {
            out[task].total += (0 .. sys.num_tasks())
                               .filter(|i| *i != task
                                        && sys.task(*i).priority <= sys.task(task).priority)
                               .map(|i| out[i].arrival)
                               .max()
                               .unwrap_or_default();
        }
    }

    fn name(&self) -> &'static str { "CRW-OMLP" }
}
