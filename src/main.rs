#![warn(clippy::pedantic)]

use rtsched::{
    task::{Task, Time},
    rsrc::{System, Mutex, Rw},
    sharing::{ObliviousAnalyzer, ObliviousTask},
    proto::{
        GlobalOm, SingleClusterOm,
        OptimalFIFO, FlexibleMulti
    },
    gen, bound
};

use clap::{Parser, ValueEnum};
use rand::distributions::uniform::SampleRange;
use itertools::iproduct;

use std::{
    ops::RangeInclusive,
    fs::File,
    io::Write,
    thread, fmt
};

/// Periods (short, medium, long) in microseconds.
const PERIODS: [RangeInclusive<Time>; 3] = [
     3_000 ..=  33_000,
    10_000 ..= 100_000,
    50_000 ..= 500_000
];

/// Request lengths (short, medium, long) in microseconds.
const LENGTHS: [RangeInclusive<Time>; 3] = [
    1 ..= 15,
    1 ..= 100,
    5 ..= 1280
];

// normalized utility function
struct Nuf {
    range: RangeInclusive<usize>,
    gen: Box<dyn Fn(usize) -> f64 + Sync>
}

impl Nuf {
    #[allow(clippy::cast_precision_loss)]
    pub fn uniform() -> Self {
        Self {
            range: 2 ..= 9,
            gen: Box::new(|x| x as f64 / 10.0)
        }
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn log() -> Self {
        Self {
            range: 1 ..= 32,
            gen: Box::new(|x| -(x as f64 / -8.0).exp_m1())
        }
    }

    pub fn gen(&self) -> impl Iterator<Item = f64> + '_ {
        self.range.clone().map(&self.gen)
    }

    pub fn len(&self) -> usize {
        self.range.end() - self.range.start() + 1
    }
}

trait Statistic {
    type Output;

    fn new_result(&self) -> Self::Output;
    fn collect(&self, tasks: &[Task], res: &mut Self::Output);
    fn header(&self, w: &mut impl fmt::Write) -> fmt::Result;
    fn describe(&self, w: &mut impl fmt::Write, res: &Self::Output, count: usize) -> fmt::Result;
}

struct MutexS<'a, R1, R2> {
    protos: &'a [Box<dyn ObliviousAnalyzer<Mutex> + Sync>],
    num_cpus: usize,
    num_rsrc: usize,
    requests: &'a gen::Requests<R1, R2>
}

impl<R1, R2> Statistic for MutexS<'_, R1, R2> where R1: SampleRange<usize> + Clone,
                                                    R2: SampleRange<Time> + Clone {
    type Output = Box<[usize]>;

    fn new_result(&self) -> Self::Output {
        vec![0; self.protos.len()].into_boxed_slice()
    }

    fn collect(&self, tasks: &[Task], res: &mut Self::Output) {
        let mut sys = System::new(tasks);

        self.requests.gen(&mut sys, self.num_rsrc);

        for (i, proto) in self.protos.iter().enumerate() {
            let data = sys.run::<dyn ObliviousAnalyzer<Mutex> + Sync>(proto.as_ref());

            res[i] += usize::from(bound::soft(
                tasks.iter().zip(data.iter()).map(ObliviousTask::from),
                self.num_cpus
            ).unwrap());
        }
    }

    fn header(&self, w: &mut impl fmt::Write) -> fmt::Result {
        let Some((first, protos)) = self.protos.split_first() else {
            return Ok(());
        };

        write!(w, "{}", first.name())?;

        for proto in protos {
            write!(w, "\t{}", proto.name())?;
        }

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)]
    fn describe(&self, w: &mut impl fmt::Write, res: &Self::Output, count: usize) -> fmt::Result {
        let Some((first, res)) = res.split_first() else {
            return Ok(());
        };

        write!(w, "{}", *first as f64 / count as f64)?;

        for value in res {
            write!(w, "\t{}", *value as f64 / count as f64)?;
        }

        Ok(())
    }
}

struct RwS<'a, R1, R2> {
    mutex_protos: &'a [Box<dyn ObliviousAnalyzer<Mutex> + Sync>],
    protos:       &'a [Box<dyn ObliviousAnalyzer<Rw> + Sync>],
    num_cpus: usize,
    num_rsrc: usize,
    requests: &'a gen::Requests<R1, R2>,
    prob_write: f64
}

impl<R1, R2> Statistic for RwS<'_, R1, R2> where R1: SampleRange<usize> + Clone,
                                                 R2: SampleRange<Time> + Clone {
    type Output = Box<[usize]>;

    fn new_result(&self) -> Self::Output {
        vec![0; self.mutex_protos.len() + self.protos.len()].into_boxed_slice()
    }

    fn collect(&self, tasks: &[Task], res: &mut Self::Output) {
        let mut sys = System::new(tasks);

        self.requests.gen_with(&mut sys, self.num_rsrc, self.prob_write);

        for (i, proto) in self.protos.iter().enumerate() {
            let data = sys.run::<dyn ObliviousAnalyzer<Rw> + Sync>(proto.as_ref());

            res[self.mutex_protos.len() + i] += usize::from(bound::soft(
                tasks.iter().zip(data.iter()).map(ObliviousTask::from),
                self.num_cpus
            ).unwrap());
        }

        let sys = sys.as_mutex();

        for (i, proto) in self.mutex_protos.iter().enumerate() {
            let data = sys.run::<dyn ObliviousAnalyzer<Mutex> + Sync>(proto.as_ref());

            res[i] += usize::from(bound::soft(
                tasks.iter().zip(data.iter()).map(ObliviousTask::from),
                self.num_cpus
            ).unwrap());
        }
    }

    fn header(&self, w: &mut impl fmt::Write) -> fmt::Result {
        let protos = if let Some((first, protos)) = self.mutex_protos.split_first() {
            write!(w, "{}", first.name())?;

            for proto in protos {
                write!(w, "\t{}", proto.name())?;
            }

            &self.protos
        } else {
            let Some((first, protos)) = self.protos.split_first() else {
                return Ok(());
            };

            write!(w, "{}", first.name())?;
            protos
        };

        for proto in protos {
            write!(w, "\t{}", proto.name())?;
        }

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)]
    fn describe(&self, w: &mut impl fmt::Write, res: &Self::Output, count: usize) -> fmt::Result {
        let Some((first, res)) = res.split_first() else {
            return Ok(());
        };

        write!(w, "{}", *first as f64 / count as f64)?;

        for value in res {
            write!(w, "\t{}", *value as f64 / count as f64)?;
        }

        Ok(())
    }
}

struct StatRunner<'a, S> {
    stat: S,
    periods: RangeInclusive<Time>,
    num_cpus: usize,
    nuf: &'a Nuf
}

impl<S> fmt::Display for StatRunner<'_, S> where S: Statistic + Sync, S::Output: Send {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const PASSES: usize = 10_000;

        let mut results = (0 .. self.nuf.len())
                          .map(|_| self.stat.new_result())
                          .collect::<Box<_>>();

        thread::scope(|s| {
            let results = &mut results;

            for (res, util) in results.iter_mut().zip(self.nuf.gen()) {
                s.spawn(move || {
                    for _ in 0 .. PASSES {
                        let tasks = gen::Tasks::new(
                            self.num_cpus, util, 2 * self.num_cpus ..= 150, self.periods.clone()
                        ).gen();

                        self.stat.collect(&tasks, res);
                    }
                });
            }
        });

        write!(f, "norm_util\t")?;
        self.stat.header(f)?;

        for (util, pass) in self.nuf.gen().zip(IntoIterator::into_iter(results)) {
            writeln!(f)?;
            write!(f, "{util}\t")?;
            self.stat.describe(f, &pass, PASSES)?;
        }

        Ok(())
    }
}

#[derive(Parser)]
struct SingleRunArgs {
    #[arg(short = 'm')]
    /// Number of CPUs in system
    num_cpus: usize,
    #[arg(value_enum, short = 'p')]
    /// Task period length class
    periods: Length,
    #[arg(short = 'r')]
    /// Number of resources in system
    num_rsrc: usize,
    #[arg(short = 'a')]
    /// Probability of access (indep. for each task/resource pair)
    prob_acc: f64,
    #[arg(value_enum, short = 'l')]
    /// Resource access duration class
    lengths: Length,
    #[arg(short = 'u')]
    /// Generate log-scale utilizations instead of linear-scale
    log_nuf: bool
}

#[derive(Parser)]
#[command(version)]
enum TestSet {
    /// Test all protocols for mutex access
    Mutex {
        #[command(flatten)]
        args: SingleRunArgs
    },
    /// Test all protocols for read-write access
    Rw {
        #[command(flatten)]
        args: SingleRunArgs,
        #[arg(short = 'w')]
        /// Probability that an access will be counted as write
        prob_write: f64
    },
    /// Generate valid combinations for all mutex protocols in current directory
    MutexAll,
    /// Generate valid combinations for all read-write protocols in current directory
    RwAll
}

#[derive(Clone, Copy, ValueEnum)]
#[repr(usize)]
enum Length {
    Short,
    Medium,
    Long
}

impl fmt::Display for Length {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Length::Short  => write!(f, "short"),
            Length::Medium => write!(f, "medium"),
            Length::Long   => write!(f, "long")
        }
    }
}

fn mutex_list(num_cpus: usize) -> Box<[Box<dyn ObliviousAnalyzer<Mutex> + Sync>]> {
    Box::new([Box::new(GlobalOm::new(num_cpus)),
              Box::new(SingleClusterOm::new(num_cpus)),
              Box::new(OptimalFIFO::new(num_cpus)),
              Box::new(FlexibleMulti)])
}

fn rw_list_rw(num_cpus: usize) -> Box<[Box<dyn ObliviousAnalyzer<Rw> + Sync>]> {
    Box::new([Box::new(SingleClusterOm::new(num_cpus)),
              Box::new(OptimalFIFO::new(num_cpus))])
}

fn rw_list_mutex(num_cpus: usize) -> Box<[Box<dyn ObliviousAnalyzer<Mutex> + Sync>]> {
    Box::new([Box::new(OptimalFIFO::new(num_cpus))])
}

fn runner_from_args<S>(args: &SingleRunArgs, stat: S) where S: Statistic + Sync, S::Output: Send {
    let nuf = if args.log_nuf {
        Nuf::log()
    } else {
        Nuf::uniform()
    };

    let periods = PERIODS[args.periods as usize].clone();
    
    println!("{}", StatRunner {
        stat,
        periods,
        num_cpus: args.num_cpus,
        nuf: &nuf
    });
}

fn main() {
    const NUM_CPUS:    [usize; 3]  = [4, 8, 16];
    // also period
    const LENGTH:      [Length; 3] = [Length::Short, Length::Medium, Length::Long];
    const PROB_ACC:    [f64; 3]    = [0.1, 0.25, 0.5];
    const NUM_RSRC_L2: [i32; 4]    = [-2, -1, 0, 1];
    const PROB_WRITE:  [f64; 5]    = [0.1, 0.2, 0.3, 0.5, 0.7];

    let args = TestSet::parse();

    match args {
        TestSet::Mutex { args } => {
            let requests = gen::Requests::new(
                args.prob_acc,
                1 ..= 5,
                LENGTHS[args.lengths as usize].clone()
            );

            runner_from_args(&args, MutexS {
                protos: &mutex_list(args.num_cpus),
                num_cpus: args.num_cpus,
                num_rsrc: args.num_rsrc,
                requests: &requests
            });
        },

        TestSet::Rw { args, prob_write } => {
            let requests = gen::Requests::new(
                args.prob_acc,
                1 ..= 5,
                LENGTHS[args.lengths as usize].clone()
            );

            runner_from_args(&args, RwS {
                mutex_protos: &rw_list_mutex(args.num_cpus),
                protos: &rw_list_rw(args.num_cpus),
                num_cpus: args.num_cpus,
                num_rsrc: args.num_rsrc,
                requests: &requests,
                prob_write
            });
        },

        TestSet::MutexAll => {
            let nuf = Nuf::uniform();

            for num_cpus in NUM_CPUS {
                let protos = mutex_list(num_cpus);

                for (length, prob_acc) in iproduct!(LENGTH, PROB_ACC) {
                    let requests = gen::Requests::new(
                        prob_acc,
                        1 ..= 5,
                        LENGTHS[length as usize].clone()
                    );

                    for (period, l2) in iproduct!(LENGTH, NUM_RSRC_L2) {
                        let periods = PERIODS[period as usize].clone();

                        let num_rsrc = if l2 < 0 {
                            num_cpus >> -l2
                        } else {
                            num_cpus << l2
                        };

                        let stat = MutexS {
                            protos: &protos,
                            requests: &requests,
                            num_cpus, num_rsrc
                        };

                        let runner = StatRunner {
                            nuf: &nuf,
                            stat, periods, num_cpus
                        };

                        let out_name = format!(
                            "mutex-{num_cpus}-{period}-{length}-{prob_acc}-{num_rsrc}.dat"
                        );

                        let mut file = match File::create(&out_name) {
                            Ok(file) => file,
                            Err(e) => panic!("error while creating {out_name}: {e}")
                        };

                        if let Err(e) = write!(file, "{}", runner) {
                            panic!("error while writing {out_name}: {e}");
                        }
                    }
                }
            }
        },

        TestSet::RwAll => {
            let nuf = Nuf::uniform();

            for num_cpus in NUM_CPUS {
                let protos = rw_list_rw(num_cpus);
                let mutex_protos = rw_list_mutex(num_cpus);

                for (length, prob_acc) in iproduct!(LENGTH, PROB_ACC) {
                    let requests = gen::Requests::new(
                        prob_acc,
                        1 ..= 5,
                        LENGTHS[length as usize].clone()
                    );

                    for (period, l2, prob_write) in iproduct!(LENGTH, NUM_RSRC_L2, PROB_WRITE) {
                        let periods = PERIODS[period as usize].clone();

                        let num_rsrc = if l2 < 0 {
                            num_cpus >> -l2
                        } else {
                            num_cpus << l2
                        };

                        let stat = RwS {
                            mutex_protos: &mutex_protos,
                            protos: &protos,
                            requests: &requests,
                            num_cpus, num_rsrc, prob_write
                        };

                        let runner = StatRunner {
                            nuf: &nuf,
                            stat, periods, num_cpus
                        };

                        let out_name = format!(
                            "rw-{num_cpus}-{period}-{length}-{prob_acc}-{num_rsrc}-{prob_write}.dat"
                        );

                        let mut file = match File::create(&out_name) {
                            Ok(file) => file,
                            Err(e) => panic!("error while creating {out_name}: {e}")
                        };

                        if let Err(e) = write!(file, "{}", runner) {
                            panic!("error while writing {out_name}: {e}");
                        }
                    }
                }
            }
        }
    }
}
