use std::cell::RefCell;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Clone)]
pub(crate) struct Env {
    pub(crate) seed: u64,
    pub(crate) rng: RefCell<ChaCha8Rng>,
}

impl Env {
    pub(crate) fn new(seed: u64) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(seed);
        Env {
            seed,
            rng: RefCell::new(rng),
        }
    }

    pub(crate) fn flip_coin(&self, probability: f64) -> bool {
        let mut rng = self.rng.borrow_mut();
        rng.random_bool(probability)
    }
}
