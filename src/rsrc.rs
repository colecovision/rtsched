//! The resource model.

use crate::{
    task::{Task, Time},
    sharing::{ObliviousAnalyzer, ObliviousData}
};

use std::{
    borrow::Borrow,
    ops::DerefMut
};

/// Data for a set of requests from a single task to a single resource.
#[derive(Default, Clone, Copy)]
pub struct Request {
    /// Total number of requests.
    pub num: usize,
    /// Maximum length of a single request.
    pub length: Time
}

/// Reference to a set of requests from a given task to a single resource.
#[derive(Clone, Copy)]
pub struct TaskRequest<'a> {
    /// The task in question, as an index into some [`System`].
    ///
    /// To get that task, use [`System::task`].
    pub task: usize,
    /// Reference to request data.
    pub req: &'a Request
}

/// Any ordered collection of request sets.
pub trait RequestSet {
    /// Sorts the collection by the maximum length of each request set.
    fn sort_by_length(&mut self);
}

/// Arrays of [`Request`]s are collections of request sets.
impl RequestSet for [Request] {
    /// Sorts the collection by the maximum length of each request set.
    fn sort_by_length(&mut self) {
        self.sort_unstable_by(|r1, r2| r1.length.cmp(&r2.length).reverse());
    }
}

/// Arrays of [`TaskRequest`]s are collections of request sets, and behave as if
/// they were a collection of the underlying [`TaskRequest::req`]s.
impl<'a> RequestSet for [TaskRequest<'a>] {
    /// Sorts the collection by the maximum length of each request set reference.
    fn sort_by_length(&mut self) {
        self.sort_unstable_by(|r1, r2| r1.req.length.cmp(&r2.req.length).reverse());
    }
}

/// Arrays of [`RequestSet`]s are considered to be clusters and behave as independent
/// collections.
impl<T> RequestSet for [T] where T: RequestSet {
    /// Sorts _each [`RequestSet`]_ independently, and mantains outer order.
    fn sort_by_length(&mut self) {
        for x in self {
            x.sort_by_length();
        }
    }
}

impl<P> RequestSet for P where P: DerefMut<Target: RequestSet> {
    fn sort_by_length(&mut self) {
        self.deref_mut().sort_by_length();
    }
}

/// Constructor for read-write request pairs.
#[derive(Default, Clone, Copy)]
pub struct RwPair<T> {
    pub read: T,
    pub write: T
}

type Stored<K> = <K as RequestKind>::Base<Request>;

/// All uses of a given resource by each task.
///
/// This type alias is used to shorten the definition of [`RequestKind::transpose`]
/// and its main use as an argument to [`ObliviousAnalyzer::pass`].
pub type Usage<'a, K> = <K as RequestKind>::Base<Box<[TaskRequest<'a>]>>;

/// Marks request kinds.
pub trait RequestKind {
    /// Generic constructor for request containers. The type `T` represents a generic request.
    /// Request kinds that do not refer to multiple requests may set this type to `T`.
    ///
    /// As a general rule, in order for [`System::run`] to work properly `Base<T>` should
    /// implement [`RequestSet`] if `T` does.
    type Base<T>;

    /// Collects iterator-of-structs `it` into a struct-of-arrays.
    ///
    /// The output must have the same structure as the input, in the sense that
    /// elements of request type `T` in `Base<T>` must be collected into the same element
    /// in output --- in however way that elements of [`Base`](`Self::Base`) are defined.
    fn transpose<'a>(it: impl Iterator<Item = &'a Stored<Self>> + Clone) -> Usage<'a, Self>
        where Stored<Self>: 'a;
}

/// Marker struct for mutex-type requests.
pub struct Mutex;
/// Marker struct for read-write-type requests.
pub struct Rw;

macro_rules! collect_by_rsrc {
    ($it:expr$(, $access:tt)?) => {
        $it.enumerate()
           .map(|(task, set)| TaskRequest { task, req: &set$(.$access)? })
           .collect()
    }
}

macro_rules! group_by_rsrc_impl {
    ($name:ty) => {
        impl RequestKind for $name {
            type Base<T> = T;

            fn transpose<'a>(it: impl Iterator<Item = &'a Stored<Self>> + Clone) -> Usage<'a, Self> {
                collect_by_rsrc!(it)
            }
        }
    };

    ($name:ty, $cont:ident) => {
        impl RequestKind for $name {
            type Base<T> = $cont<T>;

            fn transpose<'a>(it: impl Iterator<Item = &'a Stored<Self>> + Clone) -> Usage<'a, Self> {
                $cont(collect_by_rsrc!(it, 0))
            }
        }

        impl<T: RequestSet> RequestSet for $cont<T> {
            fn sort_by_length(&mut self) {
                self.0.sort_by_length();
            }
        }
    };

    ($name:ty, $cont:ident { $($field:ident),+ }) => {
        impl RequestKind for $name {
            type Base<T> = $cont<T>;

            fn transpose<'a>(it: impl Iterator<Item = &'a Stored<Self>> + Clone) -> Usage<'a, Self> {
                $cont { $($field: collect_by_rsrc!(it.clone(), $field)),* }
            }
        }

        impl<T: RequestSet> RequestSet for $cont<T> {
            fn sort_by_length(&mut self) {
                $(self.$field.sort_by_length();)*
            }
        }
    }
}

group_by_rsrc_impl!(Mutex);
group_by_rsrc_impl!(Rw, RwPair { read, write });

/// Task system with a set of requests.
///
/// `K` is simply a marker and is not itself used in the structure.
pub struct System<'a, K: RequestKind + ?Sized> {
    tasks: &'a [Task],
    num_rsrc: usize,
    reqs: Box<[Vec<Stored<K>>]> // one array per task
}

impl<'a, K> System<'a, K> where K: RequestKind {
    /// Constructs a new `System` with the given task-set.
    ///
    /// The task-set cannot be modified. The returned system has
    /// no resources and therefore no requests.
    pub fn new(tasks: &'a [Task]) -> Self {
        Self {
            num_rsrc: 0,
            reqs: (0 .. tasks.len()).map(|_| Vec::new())
                                    .collect(),
            tasks
        }
    }

    /// Retrieves the task at index `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds for the task-set.
    pub fn task(&self, i: usize) -> &'a Task {
        &self.tasks[i]
    }
}

impl<K> System<'_, K> where K: RequestKind {
    /// Returns the number of tasks in the system.
    pub fn num_tasks(&self) -> usize {
        self.tasks.len()
    }

    /// Retrieves the list of requests made by task `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds for the task-set.
    pub fn reqs_by(&self, i: usize) -> &[Stored<K>] {
        &self.reqs[i]
    }

    /// Creates a new resource and returns its index.
    ///
    /// Each resource requires a number of `K::Base<Request>`s equal to
    /// the size of the task-set ([`System::num_tasks`]). Each such structure
    /// is allocated eagerly during the execution of this function.
    ///
    /// # Panics
    ///
    /// Panics if the number of resources would overflow a `usize`.
    pub fn add_rsrc(&mut self) -> usize where K::Base<Request>: Default {
        let id = self.num_rsrc;
        self.num_rsrc = self.num_rsrc.checked_add(1).unwrap();

        for rs in &mut self.reqs {
            rs.push(K::Base::default());
        }

        id
    }

    /// Returns uses of each resource grouped by their index.
    ///
    /// This differs from [`System::reqs_by`] in that the latter returns uses
    /// of each resource _from a given task_ as opposed to globally.
    pub fn by_rsrc(&self) -> Box<[Usage<K>]> {
        (0 .. self.num_rsrc).map(
            |rsrc| K::transpose(self.reqs.iter()
                                    .map(|rs| &rs[rsrc]))
        ).collect()
    }

    /// Runs analyzer `an` on the system and returns its results for each task.
    pub fn run<'x, A: ObliviousAnalyzer<K> + ?Sized>(&'x self, an: impl Borrow<A>)
    -> Box<[ObliviousData]> where Usage<'x, K>: RequestSet {
        let an = an.borrow();
        let mut by_rsrc = self.by_rsrc();
        by_rsrc.sort_by_length();

        let mut out = (0 .. self.tasks.len())
                      .map(|task| an.pass(task, self, &by_rsrc))
                      .collect::<Box<_>>();

        an.post(self, &mut out);
        out
    }
}

impl System<'_, Mutex> {
    /// Combines the set of requests `req` from the task at index `task` to
    /// the resource at index `rsrc` with the one already present in the system.
    ///
    /// # Panics
    ///
    /// Panics if `task` is out of bounds for the task-set, if `rsrc` refers to
    /// a non-existent resource, or if the total number of requests would overflow
    /// a `usize`.
    pub fn add_req(&mut self, task: usize, rsrc: usize, req: Request) {
        assert!(task < self.tasks.len());
        assert!(rsrc < self.num_rsrc);
        let slot = &mut self.reqs[task][rsrc];

        slot.num += req.num;
        slot.length = slot.length.max(req.length);
    }
}

impl<'a> System<'a, Rw> {
    /// Collapses the read-write request system into a mutex request system.
    ///
    /// The result system contains the same number of requests; every request is treated
    /// as mutually exclusive, whether originally read or write.
    pub fn as_mutex(&self) -> System<'a, Mutex> {
        let mut out = System::new(self.tasks);

        for _ in 0 .. self.num_rsrc {
            out.add_rsrc();
        }

        for (task, rset) in self.reqs.iter().enumerate() {
            for (rsrc, RwPair { read, write }) in rset.iter().enumerate() {
                out.add_req(task, rsrc, *read);
                out.add_req(task, rsrc, *write);
            }
        }

        out
    }
}

impl System<'_, Rw> {
    /// Combines the set of read requests `req` from the task at index `task` to
    /// the resource at index `rsrc` with the one already present in the system.
    ///
    /// # Panics
    ///
    /// Panics if `task` is out of bounds for the task-set, if `rsrc` refers to
    /// a non-existent resource, or if the total number of requests would overflow
    /// a `usize`.
    pub fn add_read(&mut self, task: usize, rsrc: usize, req: Request) {
        assert!(task < self.tasks.len());
        assert!(rsrc < self.num_rsrc);
        let slot = &mut self.reqs[task][rsrc];

        slot.read.num += req.num;
        slot.read.length = slot.read.length.max(req.length);
    }

    /// Combines the set of write requests `req` from the task at index `task` to
    /// the resource at index `rsrc` with the one already present in the system.
    ///
    /// # Panics
    ///
    /// Panics if `task` is out of bounds for the task-set, if `rsrc` refers to
    /// a non-existent resource, or if the total number of requests would overflow
    /// a `usize`.
    pub fn add_write(&mut self, task: usize, rsrc: usize, req: Request) {
        assert!(task < self.tasks.len());
        assert!(rsrc < self.num_rsrc);
        let slot = &mut self.reqs[task][rsrc];

        slot.write.num += req.num;
        slot.write.length = slot.write.length.max(req.length);
    }
}
