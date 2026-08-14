//! SplitMix64: a tiny, seeded, deterministic PRNG (Vigna's reference
//! algorithm), hand-rolled so the torture harness needs no `rand` dependency
//! and a fault schedule is fully reproducible from its seed. Not
//! cryptographic; not for keys — for choosing which broker dies next.

pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish in [lo, hi). Modulo bias is < 2^-40 for the ranges the
    /// harness uses (spans well under 2^24); irrelevant for fault picking.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo < hi, "empty range");
        lo + self.next_u64() % (hi - lo)
    }

    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.range(0, xs.len() as u64) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_reference_vectors() {
        // First outputs for seed 0, from Vigna's splitmix64.c.
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn range_stays_in_bounds_and_hits_everything() {
        let mut rng = SplitMix64::new(7);
        let mut seen = [false; 5];
        for _ in 0..200 {
            let v = rng.range(10, 15);
            assert!((10..15).contains(&v));
            seen[(v - 10) as usize] = true;
        }
        assert!(seen.iter().all(|s| *s), "all values in a small range appear");
    }
}
