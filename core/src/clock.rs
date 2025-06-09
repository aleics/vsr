/// A timeout allows to define a maximal amount of ticks a certain task
/// can be running for. The replica's internal clock uses tick counts instead
/// of actual time.
#[derive(Debug, PartialEq)]
pub(crate) struct Timeout {
    /// An identifier associated to the timeout.
    id: String,

    /// Amount of ticks it has been running for.
    ticks: u64,

    /// Amount of maximal ticks it's allowed to run for.
    max: u64,

    /// A boolean expressing if the timeout is running or not.
    ticking: bool,
}

impl Timeout {
    /// Create a new timeout with a certain maximal amount of ticks
    pub(crate) fn new(id: &str, max: u64) -> Self {
        Timeout {
            id: id.to_string(),
            ticks: 0,
            max,
            ticking: false,
        }
    }

    /// Increase the timeout.
    pub(crate) fn increase(&mut self, max: u64) {
        self.max = max;
    }

    /// Start the timeout
    pub(crate) fn start(&mut self) {
        self.ticks = 0;
        self.ticking = true;
    }

    /// Execute a new tick iteration (in case the timeout is running).
    pub(crate) fn tick(&mut self) {
        if self.ticking {
            self.ticks += 1;
        }
    }

    /// Check if the timeout has fired or not.
    pub(crate) fn fired(&self) -> bool {
        if self.ticking && self.ticks >= self.max {
            assert!(self.ticks == self.max);

            true
        } else {
            false
        }
    }

    /// Reset the timeout. It expects to be ticking already.
    pub(crate) fn reset(&mut self) {
        assert!(self.ticking);
        self.ticks = 0;
    }
}
