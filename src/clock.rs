#[derive(Debug, PartialEq)]
pub(crate) struct Timeout {
    id: String,
    ticks: u64,
    after: u64,
    ticking: bool,
}

impl Timeout {
    pub(crate) fn new(id: &str, after: u64) -> Self {
        Timeout {
            id: id.to_string(),
            ticks: 0,
            after,
            ticking: false,
        }
    }

    pub(crate) fn increase(&mut self, after: u64) {
        self.after = after;
    }

    pub(crate) fn start(&mut self) {
        self.ticks = 0;
        self.ticking = true;
    }

    pub(crate) fn tick(&mut self) {
        if self.ticking {
            self.ticks += 1;
        }
    }

    pub(crate) fn fired(&self) -> bool {
        if self.ticking && self.ticks >= self.after {
            assert!(self.ticks == self.after);

            true
        } else {
            false
        }
    }

    pub(crate) fn reset(&mut self) {
        assert!(self.ticking);
        self.ticks = 0;
    }
}
