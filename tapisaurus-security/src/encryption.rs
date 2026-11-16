//! Zero-Trust Encryption Engine (AES-256-GCM) for data-at-rest and field-level security.

use ring::aead::{
    Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey, AES_256_GCM, NONCE_LEN,
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

/// Helper type for ring's HKDF output key material length
struct HkdfLen(usize);

impl ring::hkdf::KeyType for HkdfLen {
    fn len(&self) -> usize {
        self.0
    }
}

/// Custom single-nonce sequence for AEAD operations
struct OneNonce(Option<[u8; NONCE_LEN]>);

impl NonceSequence for OneNonce {
    fn advance(&mut self) -> Result<Nonce, ring::error::Unspecified> {
        let bytes = self.0.take().ok_or(ring::error::Unspecified)?;
        Nonce::try_assume_unique_for_key(&bytes)
    }
}

/// Encrypted payload with random 96-bit nonce prepended
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    /// Nonce (12 bytes) + Ciphertext + Auth Tag (16 bytes)
    pub ciphertext: Vec<u8>,
}

/// AES-256-GCM cipher manager
pub struct Cipher {
    key_bytes: [u8; 32],
    rng: SystemRandom,
}

impl Cipher {
    /// Create a cipher from a 32-byte (256-bit) secret key
    pub fn new(key_bytes: [u8; 32]) -> Self {
        Self {
            key_bytes,
            rng: SystemRandom::new(),
        }
    }

    /// Create a cipher by deriving a 256-bit key from a user passphrase and an explicit 16-byte salt
    /// using **Argon2id** (memory-hard key derivation) followed by HKDF-SHA256 domain separation.
    pub fn from_passphrase_with_salt(passphrase: &str, salt: &[u8; 16]) -> Self {
        let mut argon_out = [0u8; 32];
        if argon2::Argon2::default()
            .hash_password_into(passphrase.as_bytes(), salt, &mut argon_out)
            .is_ok()
        {
            let hkdf_salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, salt);
            let prk = hkdf_salt.extract(&argon_out);
            argon_out.fill(0);
            std::hint::black_box(&mut argon_out);

            let info = [b"tapisaurus-aes256gcm-data-at-rest".as_ref()];
            let okm = prk
                .expand(&info, HkdfLen(32))
                .expect("HKDF-SHA256 expand should not fail for 32-byte output");
            let mut key = [0u8; 32];
            okm.fill(&mut key)
                .expect("HKDF fill should not fail for matching length");
            return Self::new(key);
        }

        // Fallback if Argon2id parameter allocation fails
        let hkdf_salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, salt);
        let prk = hkdf_salt.extract(passphrase.as_bytes());
        let info = [b"tapisaurus-aes256gcm-data-at-rest".as_ref()];
        let okm = prk
            .expand(&info, HkdfLen(32))
            .expect("HKDF-SHA256 expand should not fail for 32-byte output");
        let mut key = [0u8; 32];
        okm.fill(&mut key)
            .expect("HKDF fill should not fail for matching length");
        Self::new(key)
    }

    /// Create a deterministic cipher from a user passphrase using **Argon2id** with a domain-separated salt
    /// derived from the passphrase and application label.
    /// For per-payload random-salt encryption, prefer `encrypt_with_passphrase` / `decrypt_with_passphrase`.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let domain_salt =
            ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, b"TapisaurusDB-Encryption-Salt-v2");
        let prk = domain_salt.extract(passphrase.as_bytes());
        let salt_info = [b"tapisaurus-argon2id-salt".as_ref()];
        let okm = prk
            .expand(&salt_info, HkdfLen(16))
            .expect("HKDF-SHA256 expand should not fail for 16-byte salt");
        let mut salt16 = [0u8; 16];
        okm.fill(&mut salt16)
            .expect("HKDF fill should not fail for 16-byte salt");
        Self::from_passphrase_with_salt(passphrase, &salt16)
    }

    /// Encrypt plaintext with a passphrase using a fresh random 16-byte Argon2id salt prepended to the ciphertext
    pub fn encrypt_with_passphrase(
        passphrase: &str,
        plaintext: &[u8],
    ) -> Result<EncryptedData, String> {
        let rng = SystemRandom::new();
        let mut salt = [0u8; 16];
        rng.fill(&mut salt)
            .map_err(|_| "Failed to generate random KDF salt".to_string())?;
        let cipher = Self::from_passphrase_with_salt(passphrase, &salt);
        let inner = cipher.encrypt(plaintext)?;
        let mut out = Vec::with_capacity(16 + inner.ciphertext.len());
        out.extend_from_slice(&salt);
        out.extend_from_slice(&inner.ciphertext);
        Ok(EncryptedData { ciphertext: out })
    }

    /// Decrypt payload produced by `encrypt_with_passphrase` (extracts 16-byte Argon2id salt prefix)
    pub fn decrypt_with_passphrase(
        passphrase: &str,
        encrypted: &EncryptedData,
    ) -> Result<Vec<u8>, String> {
        if encrypted.ciphertext.len() < 16 + NONCE_LEN + 16 {
            return Err("Salted ciphertext is too short".to_string());
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&encrypted.ciphertext[..16]);
        let cipher = Self::from_passphrase_with_salt(passphrase, &salt);
        let inner = EncryptedData {
            ciphertext: encrypted.ciphertext[16..].to_vec(),
        };
        cipher.decrypt(&inner)
    }

    /// Generate a cryptographically secure random 256-bit key
    pub fn generate_key() -> Result<[u8; 32], String> {
        let rng = SystemRandom::new();
        let mut key = [0u8; 32];
        rng.fill(&mut key)
            .map_err(|_| "Failed to generate random key".to_string())?;
        Ok(key)
    }

    /// Encrypt plaintext bytes using AES-256-GCM with a fresh unique nonce
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData, String> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|_| "Failed to generate nonce")?;

        let unbound_key = UnboundKey::new(&AES_256_GCM, &self.key_bytes)
            .map_err(|_| "Failed to create AEAD key")?;
        let mut sealing_key = SealingKey::new(unbound_key, OneNonce(Some(nonce_bytes)));

        let mut in_out = plaintext.to_vec();
        sealing_key
            .seal_in_place_append_tag(Aad::empty(), &mut in_out)
            .map_err(|_| "Encryption failed")?;

        // Output: [12 bytes nonce] + [ciphertext + tag]
        let mut full_output = Vec::with_capacity(NONCE_LEN + in_out.len());
        full_output.extend_from_slice(&nonce_bytes);
        full_output.extend_from_slice(&in_out);

        Ok(EncryptedData {
            ciphertext: full_output,
        })
    }

    /// Decrypt ciphertext produced by `encrypt`
    pub fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>, String> {
        if encrypted.ciphertext.len() < NONCE_LEN + 16 {
            return Err("Ciphertext is too short".to_string());
        }

        let (nonce_bytes, ciphertext_with_tag) = encrypted.ciphertext.split_at(NONCE_LEN);
        let mut nonce_arr = [0u8; NONCE_LEN];
        nonce_arr.copy_from_slice(nonce_bytes);

        let unbound_key = UnboundKey::new(&AES_256_GCM, &self.key_bytes)
            .map_err(|_| "Failed to create AEAD key")?;
        let mut opening_key = OpeningKey::new(unbound_key, OneNonce(Some(nonce_arr)));

        let mut in_out = ciphertext_with_tag.to_vec();
        let decrypted_slice = opening_key
            .open_in_place(Aad::empty(), &mut in_out)
            .map_err(|_| "Decryption failed or invalid key/tampered data")?;

        Ok(decrypted_slice.to_vec())
    }
}

impl Drop for Cipher {
    fn drop(&mut self) {
        self.key_bytes.fill(0);
        std::hint::black_box(&mut self.key_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argon2id_salted_passphrase_encrypt_decrypt() {
        let pass = "correct-horse-battery-staple-2026";
        let plaintext = b"Confidential TapisaurusDB WAL Page";
        let enc1 = Cipher::encrypt_with_passphrase(pass, plaintext).unwrap();
        let enc2 = Cipher::encrypt_with_passphrase(pass, plaintext).unwrap();
        // Random 16-byte salt + random 12-byte nonce ensures distinct ciphertexts
        assert_ne!(enc1.ciphertext, enc2.ciphertext);

        let dec1 = Cipher::decrypt_with_passphrase(pass, &enc1).unwrap();
        assert_eq!(dec1, plaintext);
        assert!(Cipher::decrypt_with_passphrase("wrong-passphrase", &enc1).is_err());
    }

    #[test]
    fn test_aes256_encrypt_decrypt() {
        let key = Cipher::generate_key().unwrap();
        let cipher = Cipher::new(key);

        let original = b"TapisaurusDB: World's fastest AI-native database";
        let encrypted = cipher.encrypt(original).unwrap();
        assert_ne!(encrypted.ciphertext, original);

        let decrypted = cipher.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn test_tamper_detection() {
        let key = Cipher::generate_key().unwrap();
        let cipher = Cipher::new(key);

        let original = b"Secret payload";
        let mut encrypted = cipher.encrypt(original).unwrap();

        // Tamper with a byte
        let len = encrypted.ciphertext.len();
        encrypted.ciphertext[len - 1] ^= 0x55;

        // Decryption must fail due to auth tag verification
        assert!(cipher.decrypt(&encrypted).is_err());
    }
}
