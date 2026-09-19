//! Fernet compatible symmetric encryption.
//!
//! The wire format matches the Python `cryptography` implementation, so a data directory
//! written by either service can be read by the other as long as the key is unchanged.
use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{ApiError, Result};
use crate::util::{random_bytes, unix_now};

type CbcDecryptor = cbc::Decryptor<aes::Aes128>;
type CbcEncryptor = cbc::Encryptor<aes::Aes128>;
type HmacSha256 = Hmac<Sha256>;

const VERSION: u8 = 0x80;
/// version byte + timestamp + IV + HMAC tag; the ciphertext itself adds at least one block.
const MIN_TOKEN: usize = 1 + 8 + 16 + 32;

#[derive(Clone)]
pub struct Fernet {
    signing: [u8; 16],
    encryption: [u8; 16],
}

impl Fernet {
    /// Build a cipher from a 32 byte urlsafe base64 key.
    pub fn from_key(key: &[u8; 32]) -> Self {
        let mut signing = [0u8; 16];
        let mut encryption = [0u8; 16];
        signing.copy_from_slice(&key[..16]);
        encryption.copy_from_slice(&key[16..]);
        Self {
            signing,
            encryption,
        }
    }

    /// Accept a regular environment secret as well as an already encoded Fernet key.
    pub fn from_secret(secret: &str) -> Self {
        let decoded = URL_SAFE
            .decode(secret.trim())
            .ok()
            .filter(|bytes| bytes.len() == 32);
        match decoded {
            Some(bytes) => {
                let mut key = [0u8; 32];
                key.copy_from_slice(&bytes);
                Self::from_key(&key)
            }
            None => {
                let digest = Sha256::digest(secret.as_bytes());
                let encoded = URL_SAFE.encode(digest);
                let mut key = [0u8; 32];
                key.copy_from_slice(
                    &URL_SAFE
                        .decode(&encoded)
                        .expect("sha256 digest is 32 bytes"),
                );
                Self::from_key(&key)
            }
        }
    }

    pub fn generate_key() -> String {
        URL_SAFE.encode(random_bytes(32))
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> String {
        let iv = random_bytes(16);
        let ciphertext = CbcEncryptor::new((&self.encryption).into(), iv.as_slice().into())
            .encrypt_padded_vec_mut::<Pkcs7>(plaintext);
        let mut body = Vec::with_capacity(MIN_TOKEN + ciphertext.len());
        body.push(VERSION);
        body.extend_from_slice(&unix_now().to_be_bytes());
        body.extend_from_slice(&iv);
        body.extend_from_slice(&ciphertext);
        let tag = self.tag(&body);
        body.extend_from_slice(&tag);
        URL_SAFE.encode(&body)
    }

    pub fn decrypt(&self, token: &str) -> Result<Vec<u8>> {
        let raw = URL_SAFE
            .decode(token.trim())
            .map_err(|_| ApiError::internal("stored secret is not valid base64"))?;
        if raw.len() < MIN_TOKEN || raw[0] != VERSION {
            return Err(ApiError::internal("stored secret is not a Fernet token"));
        }
        let (signed, tag) = raw.split_at(raw.len() - 32);
        if self.tag(signed).ct_eq(tag).unwrap_u8() != 1 {
            return Err(ApiError::internal("stored secret failed authentication"));
        }
        let iv = &signed[9..25];
        CbcDecryptor::new((&self.encryption).into(), iv.into())
            .decrypt_padded_vec_mut::<Pkcs7>(&signed[25..])
            .map_err(|_| ApiError::internal("stored secret could not be decrypted"))
    }

    pub fn decrypt_text(&self, token: &str) -> Result<String> {
        String::from_utf8(self.decrypt(token)?)
            .map_err(|_| ApiError::internal("stored secret is not UTF-8"))
    }

    fn tag(&self, signed: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.signing).expect("hmac accepts any key size");
        mac.update(signed);
        mac.finalize().into_bytes().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vectors produced by Python `cryptography.fernet.Fernet` so the two stay interchangeable.
    const PYTHON_KEY: &str = "e1mIKTjfURp_8_icI0T-I7wu0Tca55UdICUFF0Mm3z4=";
    const PYTHON_TOKEN: &str = "gAAAAABqrk2gtYcXviEn8f1bDNZy_aV11oo-s6fkIFvz2si7rwMM5XupgigTsvfVpX755IWHR4VMvpfc3OaJG0gmKFDriNEsQA==";
    const PYTHON_EMPTY_TOKEN: &str = "gAAAAABqrk2gpHfOb9WjAe8uY8L2R2oKdFF7QmPuVBsmDS36GGRa_-dBM6kYkhh_Hw6-4a-ACJTfXjDbKyMe43eqHag01JPCkA==";
    const PYTHON_DERIVED_TOKEN: &str = "gAAAAABqrk2g7p-gB9nCpTFB5Rp1pCwYs8lfMc7RXV5qsc0GFvT6Enau7m0Tbe1Q1oJrdI1_AssKgtFqElDPsSRPiPE5kXjkaQ==";

    #[test]
    fn round_trips_values() {
        let cipher = Fernet::from_key(&[7u8; 32]);
        for plain in ["", "sk-secret-value", "unicode ✓ 中文", &"x".repeat(4096)] {
            assert_eq!(
                cipher
                    .decrypt_text(&cipher.encrypt(plain.as_bytes()))
                    .unwrap(),
                plain
            );
        }
    }

    #[test]
    fn decrypts_tokens_written_by_python() {
        let cipher = Fernet::from_secret(PYTHON_KEY);
        assert_eq!(
            cipher.decrypt_text(PYTHON_TOKEN).unwrap(),
            "sk-secret-value"
        );
        assert_eq!(cipher.decrypt_text(PYTHON_EMPTY_TOKEN).unwrap(), "");
    }

    #[test]
    fn derives_the_python_key_from_a_plain_secret() {
        // Python derives sha256("a-long-local-secret") -> urlsafe base64 for the same token.
        let cipher = Fernet::from_secret("a-long-local-secret");
        assert_eq!(
            cipher.decrypt_text(PYTHON_DERIVED_TOKEN).unwrap(),
            "payload"
        );
        assert!(Fernet::from_secret(PYTHON_KEY)
            .decrypt_text(PYTHON_DERIVED_TOKEN)
            .is_err());
    }

    #[test]
    fn rejects_tampered_or_malformed_tokens() {
        let cipher = Fernet::from_key(&[3u8; 32]);
        let token = cipher.encrypt(b"secret");
        let mut characters: Vec<char> = token.chars().collect();
        let middle = characters.len() / 2;
        characters[middle] = if characters[middle] == 'A' { 'B' } else { 'A' };
        assert!(cipher
            .decrypt(&characters.into_iter().collect::<String>())
            .is_err());
        assert!(cipher.decrypt("not base64").is_err());
        assert!(cipher.decrypt("c2hvcnQ").is_err());
        assert!(Fernet::from_key(&[4u8; 32]).decrypt(&token).is_err());
    }

    #[test]
    fn generates_usable_keys() {
        let key = Fernet::generate_key();
        assert_eq!(URL_SAFE.decode(&key).unwrap().len(), 32);
        assert_eq!(
            Fernet::from_secret(&key)
                .decrypt_text(&Fernet::from_secret(&key).encrypt(b"value"))
                .unwrap(),
            "value"
        );
    }
}
