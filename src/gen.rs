//! Generators for task-sets and requests.

use crate::{
    task::{Task, Time},
    rsrc::{System, RequestKind, Mutex, Rw, Request}
};

use rand::{
    distributions::{uniform::SampleRange, Bernoulli},
    seq::SliceRandom,
    Rng
};

use std::mem;

/// Generator that implements Stafford's RandFixedSum.
struct Rfs {
    t: Box<[Box<[Bernoulli]>]>,
    s: f64,
    k: usize
}

impl Rfs {
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss,
            clippy::cast_possible_truncation)]
    fn new(length: usize, s: f64) -> Self {
        if length == 0 {
            panic!("length must be nonzero");
        } else if s < 0.0 || s > length as f64 {
            panic!("s must be between 0 and length");
        }

        let k = (s as usize).clamp(0, length - 1); // must have 0 <= k <= length-1
        let s = s.clamp(k as f64, (k+1) as f64);   // must have k <= s <= k+1

        // construct the transition probability table, t.
        // t[i][j] will be utilized only in the region where j <= i + 1.

        let def = Bernoulli::from_ratio(1, 1).unwrap();

        let mut w = vec![0.0; length].into_boxed_slice();
        let mut t = (1 .. length).map(|l| vec![def; l + 1].into_boxed_slice())
                                 .collect::<Box<_>>();

        w[0] = f64::MAX;

        let delta = s - k as f64;

        for i in 1 .. length {
            let mut lastw = 0.0;

            for j in 0 .. i {
                let coe1 = (j as f64       + delta) / i as f64;
                let coe2 = ((i - j) as f64 - delta) / i as f64;

                let tmp1 = w[j]  * coe1;
                let tmp2 = lastw * coe2;

                lastw = mem::replace(&mut w[j], tmp1 + tmp2);

                t[i-1][j] = Bernoulli::new(if w[j] == 0.0 {
                    (coe1 >= 0.5).into()
                } else {
                    tmp2 / w[j]
                }).unwrap();
            }
        }
        
        Self { t, s, k }
    }

    #[allow(clippy::cast_precision_loss)]
    fn gen(&self) -> Box<[f64]> {
        let length = self.t.len() + 1;

        let mut out = vec![0.0; length].into_boxed_slice();

        // start with sum zero & product 1
        let mut sm = 0.0;
        let mut pr = 1.0;

        let mut j = self.k; // for indexing in the t table

        for i in (1 .. length).rev() { // work backwards in the t table
            let s = self.s - (self.k - j) as f64;
            let e = rand::thread_rng().sample(self.t[i-1][j]);       // choose a transition
            let sx = rand::random::<f64>().powf((i as f64).recip()); // compute next simplex coord.
            sm += (1.0 - sx) * pr * s / (i + 1) as f64;              // update sum
            pr *= sx;                                                // update product
            out[length - i] = f64::from(e).mul_add(pr, sm);          // calculate out using simplex coords.
            // transition adjustment
            j -= usize::from(e);
        }

        // compute the last value
        out[0] = (self.s - (self.k - j) as f64).mul_add(pr, sm);
        // randomly permute the order of out.
        out.shuffle(&mut rand::thread_rng());

        out
    }
}

/// Generator for tasks.
pub struct Tasks<R1, R2> {
    util: f64,
    num: R1,
    period: R2
}

impl<R1, R2> Tasks<R1, R2> {
    /// Constructs a new `Tasks` with the given parameters.
    ///
    /// The task-set to be generated will have normalized utilization
    /// `norm_util` for `num_cpus` CPUs. The number of tasks will be chosen
    /// uniformly at random from `num_tasks`, and their period also uniformly
    /// at random from `period`. Each task will have implicit deadline.
    ///
    /// The utilizations of each task are picked uniformly at random from the
    /// space described above; the algorithm used to do so is
    /// [Stafford's RandFixedSum](https://www.mathworks.com/matlabcentral/fileexchange/9700-random-vectors-with-fixed-sum).
    #[allow(clippy::cast_precision_loss)]
    pub fn new(num_cpus: usize, norm_util: f64, num_tasks: R1, period: R2) -> Self {
        Self {
            util: norm_util * num_cpus as f64,
            num: num_tasks,
            period
        }
    }
}

impl<R1, R2> Tasks<R1, R2> where R1: SampleRange<usize>,
                                 R2: SampleRange<Time> + Clone {
    /// Runs the generator.
    ///
    /// Returns a system as described in [`Tasks::new`].
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss,
            clippy::cast_possible_truncation)]
    pub fn gen(self) -> Box<[Task]> {
        let Self { util, num, period } = self;

        let num = rand::thread_rng().gen_range(num);
        let utils = Rfs::new(num, util).gen();

        IntoIterator::into_iter(utils).map(|u| {
            let period = rand::thread_rng().gen_range(period.clone());

            Task::new(
                (period as f64 * u).ceil() as Time,
                period
            ).edf()
        }).collect()
    }
}

/// Marks request kinds that can be generated by [`Requests`].
pub trait RequestGen: RequestKind {
    /// Type that parameterizes the final generation of each request.
    type Params: Copy;

    /// Adds request `req` by the task at index `task` for the resource at index `rsrc` to task
    /// system `sys`, under parameters given by `params`.
    /// 
    /// May and should make use of random decisions if needed.
    fn add_to(sys: &mut System<Self>, task: usize, rsrc: usize, req: Request, params: Self::Params);
}

impl RequestGen for Mutex {
    type Params = ();

    /// Simply adds the request as a mutex request.
    fn add_to(sys: &mut System<Self>, task: usize, rsrc: usize, req: Request, _: ()) {
        sys.add_req(task, rsrc, req);
    }
}

impl RequestGen for Rw {
    /// Probability that a request will be a write request.
    type Params = f64;

    /// Adds the request as a write request with probability `prob` and as a read request
    /// otherwise.
    fn add_to(sys: &mut System<Self>, task: usize, rsrc: usize, req: Request, prob: f64) {
        if rand::random::<f64>() < prob {
            sys.add_write(task, rsrc, req);
        } else {
            sys.add_read(task, rsrc, req);
        }
    }
}

/// Generator for requests.
pub struct Requests<R1, R2> {
    prob_acc: f64,
    num: R1,
    length: R2
}

impl<R1, R2> Requests<R1, R2> {
    /// Constructs a new `Requests` with the given parameters.
    ///
    /// Each task will access a resource with probability `prob_acc`.
    /// The number of actual requests and their length will be generated
    /// uniformly from the ranges in `num` and `length`, respectively.
    pub fn new(prob_acc: f64, num: R1, length: R2) -> Self {
        assert!((0.0 ..= 1.0).contains(&prob_acc));

        Self {
            prob_acc,
            num,
            length
        }
    }
}

impl<R1, R2> Requests<R1, R2> where R1: SampleRange<usize> + Clone,
                                    R2: SampleRange<Time> + Clone {
    /// Runs the generator on system `sys`.
    ///
    /// Returns a system filled with `num_rsrc` new resources and requests
    /// for the same, as described in [`Requests::new`] and conforming to
    /// the process of the appropriate [`RequestGen`]; the same `params` will be
    /// passed to each invocation of [`RequestGen::add_to`].
    ///
    /// If `sys` already contains resources and requests they will be added
    /// on top of the ones already present; no data will be overwritten.
    pub fn gen_with<K: RequestGen>(&self, sys: &mut System<K>, num_rsrc: usize, params: K::Params)
    where K::Base<Request>: Default {
        for _ in 0 .. num_rsrc {
            self.gen_one_with(sys, params);
        }
    }

    /// Variant of [`gen_with`](`Self::gen_with`) for use with [`RequestGen`]
    /// implementations with unit [`Params`](`RequestGen::Params`), that is, for
    /// generators that do not require additional parameters.
    pub fn gen<K: RequestGen<Params = ()>>(&self, sys: &mut System<K>, num_rsrc: usize)
    where K::Base<Request>: Default {
        for _ in 0 .. num_rsrc {
            self.gen_one_with(sys, ());
        }
    }

    fn gen_one_with<K: RequestGen>(
        &self,
        sys: &mut System<K>,
        params: K::Params
    ) where K::Base<Request>: Default {
        let Self { prob_acc, num, length } = self;

        let rsrc = sys.add_rsrc();

        for task in 0 .. sys.num_tasks() {
            if rand::random::<f64>() >= *prob_acc {
                continue;
            }

            // XXX this is not exactly how it's specified
            //     in the paper but it's how taskset gen
            //     is done in the codebase linked by it.

            let mut nonreq = sys.task(task).cost;

            for _ in 0 .. rand::thread_rng().gen_range(num.clone()) {
                let len = rand::thread_rng().gen_range(length.clone());
                
                let request = if let Some(new) = nonreq.checked_sub(len) {
                    nonreq = new;
                    Request { num: 1, length: len }
                } else {
                    let adjust = nonreq;
                    nonreq = 0;
                    Request { num: 1, length: len - adjust }
                };

                K::add_to(sys, task, rsrc, request, params);

                if nonreq == 0 {
                    break;
                }
            }
        }
    }
}
