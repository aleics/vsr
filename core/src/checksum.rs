use aegis::aegis128l::Aegis128LMac;

const KEY: [u8; 16] = [0; 16];

/// ChecksumGenerator is a helper struct for generating checksums using the AEGIS-128L MAC algorithm.
struct ChecksumGenerator {
    seed: Aegis128LMac<16>,
}

impl ChecksumGenerator {
    fn new() -> Self {
        Self {
            seed: Aegis128LMac::new(&KEY),
        }
    }

    /// Adds data to the checksum generator.
    fn add(&mut self, data: &[u8]) {
        self.seed.update(data);
    }

    /// Finalizes the checksum generation and returns the result.
    fn checksum(self) -> u128 {
        let tag = self.seed.finalize();
        u128::from_le_bytes(tag)
    }
}

pub(crate) fn checksum(data: &[u8]) -> u128 {
    let mut generator = ChecksumGenerator::new();
    generator.add(data);
    generator.checksum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum() {
        let data = b"Hello, world!";
        let expected = checksum(data);
        assert_eq!(expected, checksum(data));
    }

    #[test]
    fn test_checksum_tampering() {
        let data = b"Hello, world!";

        // Modify the data to tamper with the checksum
        let mut tampered_data = data.to_vec();
        tampered_data[0] ^= 0xFF;

        assert!(checksum(data) != checksum(&tampered_data));
    }
}
