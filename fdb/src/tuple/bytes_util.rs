//! Utility functions for operating on [`Bytes`].
//!
//! Although built for FDB tuple layer, some functions may be useful
//! otherwise.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{FdbError, FdbResult, TUPLE_BYTES_UTIL_STRINC_ERROR};

/// Computes the key that would sort immediately after `key`.
///
/// # Panic
///
/// `key` must not be empty.
pub fn key_after(key: Bytes) -> Bytes {
    let mut res = BytesMut::new();
    res.put(key);
    res.put_u8(0x00);
    res.into()
}

/// Checks if `key` starts with `prefix`.
pub fn starts_with(key: Bytes, prefix: Bytes) -> bool {
    // Check to make sure `key` is atleast as long as
    // `prefix`. Otherwise the slice operator will panic.
    if key.len() < prefix.len() {
        false
    } else {
        &prefix[..] == &key[..prefix.len()]
    }
}

/// Computes the first key that would sort outside the range prefixed
/// by `prefix`.
///
/// The `prefix` must not be empty or contain only `0xFF` bytes. That is
/// `prefix` must contain at least one byte not equal to `0xFF`.
///
/// This resulting [`Bytes`] serves as the exclusive upper-bound for
/// all keys prefixed by the argument `prefix`. In other words, it is
/// the first key for which the argument `prefix` is not a prefix.
pub fn strinc(prefix: Bytes) -> FdbResult<Bytes> {
    if prefix.len() == 0 {
        return Err(FdbError::new(TUPLE_BYTES_UTIL_STRINC_ERROR));
    }

    // initially assume that the last byte is not `0xFF`
    let mut non_ff_byte_index = prefix.len() - 1;

    while non_ff_byte_index > 0 {
        if prefix[non_ff_byte_index] == 0xFF {
            non_ff_byte_index -= 1;
        } else {
            break;
        }
    }

    if non_ff_byte_index == 0x00 {
        // We have only one byte. If it is `0xFF` then it is an
        // error. Otherwise just increment the byte and return it.
        if prefix[non_ff_byte_index] == 0xFF {
            Err(FdbError::new(TUPLE_BYTES_UTIL_STRINC_ERROR))
        } else {
            let mut res = BytesMut::new();
            res.put_u8(prefix[non_ff_byte_index] + 1);
            Ok(res.into())
        }
    } else {
        // There are two bytes or more
        let mut res = BytesMut::new();
        res.put(&prefix[0..non_ff_byte_index]);
        res.put_u8(prefix[non_ff_byte_index] + 1);
        Ok(res.into())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::error::{FdbError, TUPLE_BYTES_UTIL_STRINC_ERROR};

    use super::{key_after, starts_with, strinc};

    #[test]
    fn test_key_after() {
        assert_eq!(key_after(Bytes::new()), Bytes::from_static(&b"\x00"[..]));
        assert_eq!(
            key_after(Bytes::from_static(&b"hello_world"[..])),
            Bytes::from_static(&b"hello_world\x00"[..])
        );
    }

    #[test]
    fn test_starts_with() {
        // length mismatch
        assert_eq!(
            starts_with(
                Bytes::from_static(&b"p"[..]),
                Bytes::from_static(&b"prefix"[..])
            ),
            false
        );

        assert_eq!(
            starts_with(
                Bytes::from_static(&b"wrong_prefix"[..]),
                Bytes::from_static(&b"prefix"[..])
            ),
            false
        );

        assert_eq!(
            starts_with(
                Bytes::from_static(&b"prefix_plus_something_else"[..]),
                Bytes::from_static(&b"prefix"[..])
            ),
            true
        );
    }

    #[test]
    fn test_strinc() {
        assert_eq!(
            strinc(Bytes::new()),
            Err(FdbError::new(TUPLE_BYTES_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFF"[..])),
            Err(FdbError::new(TUPLE_BYTES_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFF\xFF"[..])),
            Err(FdbError::new(TUPLE_BYTES_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\x00"[..])),
            Ok(Bytes::from_static(&b"\x01"[..]))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFE"[..])),
            Ok(Bytes::from_static(&b"\xFF"[..]))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"a\xFF"[..])),
            Ok(Bytes::from_static(&b"b"[..]))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1"[..])),
            Ok(Bytes::from_static(&b"hello2"[..]))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1\xFF"[..])),
            Ok(Bytes::from_static(&b"hello2"[..]))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1\xFF\xFF"[..])),
            Ok(Bytes::from_static(&b"hello2"[..]))
        );
    }
}
