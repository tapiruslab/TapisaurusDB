//! Deterministic PRNG (`SplitMix64` + `Xoroshiro128++`) for bit-for-bit reproducible
//! simulation schedules across runs and platforms.

/// Deterministic 128-bit state PRNG seeded from a 64-bit simulation seed.
#[derive(Debug, Clone)]
pub struct SimRng {
    seed: u64,
    s0: u64,
    s1: u64,
    steps: u64,
}

impl SimRng {
    /// Initialize a deterministic PRNG from a 64-bit seed using SplitMix64 warm-up.
    pub fn from_seed(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let s0 = splitmix64(&mut z);
        let mut s1 = splitmix64(&mut z);
        if s0 == 0 && s1 == 0 {
            s1 = 0xD1B5_4A32_D192_ED03;
        }
        Self {
            seed,
            s0,
            s1,
            steps: 0,
        }
    }

    /// Return the original seed used to initialize this PRNG.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Return total PRNG draws executed so far.
    pub fn steps(&self) -> u64 {
        self.steps
    }

    /// Generate the next deterministic `u64` via Xoroshiro128++.
    pub fn next_u64(&mut self) -> u64 {
        let s0 = self.s0;
        let mut s1 = self.s1;
        let result = s0.wrapping_add(s1).rotate_left(17).wrapping_add(s0);

        s1 ^= s0;
        self.s0 = s0.rotate_left(49) ^ s1 ^ (s1 << 21);
        self.s1 = s1.rotate_left(28);
        self.steps += 1;
        result
    }

    /// Generate a uniform `usize` in `[0, upper)` (panics if `upper == 0`).
    pub fn gen_usize(&mut self, upper: usize) -> usize {
        assert!(upper > 0, "upper bound must be non-zero");
        (self.next_u64() % (upper as u64)) as usize
    }

    /// Generate a uniform `u64` in `[low, high]` inclusive.
    pub fn gen_range_u64(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        let span = high - low + 1;
        low + (self.next_u64() % span)
    }

    /// Generate a uniform `i64` in `[low, high]` inclusive.
    pub fn gen_range_i64(&mut self, low: i64, high: i64) -> i64 {
        if high <= low {
            return low;
        }
        let span = (high - low) as u64 + 1;
        low + (self.next_u64() % span) as i64
    }

    /// Return `true` with `probability` in `[0.0, 1.0]`.
    pub fn bernoulli(&mut self, probability: f64) -> bool {
        if probability <= 0.0 {
            return false;
        }
        if probability >= 1.0 {
            return true;
        }
        let unit = (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
        unit < probability
    }

    /// Fisher-Yates deterministic in-place shuffle.
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let j = self.gen_usize(i + 1);
            slice.swap(i, j);
        }
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
