use bytes::Bytes;
use num_bigint::BigInt;
use uuid::Uuid;

use crate::tuple::Versionstamp;

// The specifications for FDB Tuple layer typecodes is here.
// https://github.com/apple/foundationdb/blob/master/design/tuple.md

// todo: https://github.com/josephg/fdb-tuple/blob/master/lib/test.ts
// todo: https://github.com/apache/couchdb-erlfdb/blob/main/src/erlfdb_tuple.erl

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum TupleValue {
    NullValue,                                 // 0x00
    ByteString(Bytes),                         // 0x01
    UnicodeString(String),                     // 0x02
    NestedTuple(Vec<TupleValue>),              // 0x05
    NegativeArbitraryPrecisionInteger(BigInt), // 0x0b
    NegInt8(u64),                              // 0x0c
    NegInt7(u64),                              // 0x0d
    NegInt6(u64),                              // 0x0e
    NegInt5(u64),                              // 0x0f
    NegInt4(u32),                              // 0x10
    NegInt3(u32),                              // 0x11
    NegInt2(u16),                              // 0x12
    NegInt1(u8),                               // 0x13
    IntZero,                                   // 0x14
    PosInt1(u8),                               // 0x15
    PosInt2(u16),                              // 0x16
    PosInt3(u32),                              // 0x17
    PosInt4(u32),                              // 0x18
    PosInt5(u64),                              // 0x19
    PosInt6(u64),                              // 0x1a
    PosInt7(u64),                              // 0x1b
    PosInt8(u64),                              // 0x1c
    PositiveArbitraryPrecisionInteger(BigInt), // 0x1d
    IeeeBinaryFloatingPointFloat(f32),         // 0x20
    IeeeBinaryFloatingPointDouble(f64),        // 0x21
    FalseValue,                                // 0x26
    TrueValue,                                 // 0x27
    Rfc4122Uuid(Uuid),                         // 0x30
    Versionstamp96Bit(Versionstamp),           // 0x33
}

pub(self) mod parsers {
    use super::TupleValue;

    use bytes::Buf;
    use nom::{bytes as nom_bytes, combinator, multi, number, sequence, IResult};
    use num_bigint::{BigInt, Sign};

    fn neg_u8_slice_into_vec(i: &[u8]) -> Vec<u8> {
        let mut res = Vec::new();
        i.into_iter().for_each(|x| {
            res.push(!(*x));
        });
        res
    }

    // Understand how to use `and_then` if needed, and also read about
    // monads and `flat_map`, and see if can be applied here.
    //
    // TODO: Continue to work on this after you have a working python setup.
    fn negative_arbitrary_precision_integer(i: &[u8]) -> IResult<&[u8], TupleValue> {
	todo!();
    }

    fn neg_int_8(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0C"[..]),
            combinator::map(nom_bytes::complete::take(8u8), |x: &[u8]| {
                TupleValue::NegInt8((&neg_u8_slice_into_vec(x)[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_7(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0D"[..]),
            combinator::map(nom_bytes::complete::take(7u8), |x: &[u8]| {
                let mut val = vec![0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt7((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_6(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0E"[..]),
            combinator::map(nom_bytes::complete::take(6u8), |x: &[u8]| {
                let mut val = vec![0xFFu8, 0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt6((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_5(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0F"[..]),
            combinator::map(nom_bytes::complete::take(5u8), |x: &[u8]| {
                let mut val = vec![0xFFu8, 0xFFu8, 0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt5((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_4(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x10"[..]),
            combinator::map(nom_bytes::complete::take(4u8), |x: &[u8]| {
                TupleValue::NegInt4((&neg_u8_slice_into_vec(x)[..]).get_u32())
            }),
        )(i)
    }

    fn neg_int_3(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x11"[..]),
            combinator::map(nom_bytes::complete::take(3u8), |x: &[u8]| {
                let mut val = vec![0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt3((&neg_u8_slice_into_vec(&val[..])[..]).get_u32())
            }),
        )(i)
    }

    fn neg_int_2(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x12"[..]),
            combinator::map(nom_bytes::complete::take(2u8), |x: &[u8]| {
                TupleValue::NegInt2((&neg_u8_slice_into_vec(x)[..]).get_u16())
            }),
        )(i)
    }

    fn neg_int_1(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x13"[..]),
            combinator::map(nom_bytes::complete::take(1u8), |x: &[u8]| {
                TupleValue::NegInt1((&neg_u8_slice_into_vec(x)[..]).get_u8())
            }),
        )(i)
    }

    fn int_zero(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x14"[..]), |_| {
            TupleValue::IntZero
        })(i)
    }

    fn pos_int_1(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x15"[..]),
            combinator::map(nom_bytes::complete::take(1u8), |mut x: &[u8]| {
                TupleValue::PosInt1(x.get_u8())
            }),
        )(i)
    }

    fn pos_int_2(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x16"[..]),
            combinator::map(nom_bytes::complete::take(2u8), |mut x: &[u8]| {
                TupleValue::PosInt2(x.get_u16())
            }),
        )(i)
    }

    fn pos_int_3(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x17"[..]),
            combinator::map(nom_bytes::complete::take(3u8), |x: &[u8]| {
                let mut val = vec![0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt3((&val[..]).get_u32().into())
            }),
        )(i)
    }

    fn pos_int_4(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x18"[..]),
            combinator::map(nom_bytes::complete::take(4u8), |mut x: &[u8]| {
                TupleValue::PosInt4(x.get_u32())
            }),
        )(i)
    }

    fn pos_int_5(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x19"[..]),
            combinator::map(nom_bytes::complete::take(5u8), |x: &[u8]| {
                let mut val = vec![0u8, 0u8, 0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt5((&val[..]).get_u64().into())
            }),
        )(i)
    }

    fn pos_int_6(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1A"[..]),
            combinator::map(nom_bytes::complete::take(6u8), |x: &[u8]| {
                let mut val = vec![0u8, 0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt6((&val[..]).get_u64().into())
            }),
        )(i)
    }

    fn pos_int_7(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1B"[..]),
            combinator::map(nom_bytes::complete::take(7u8), |x: &[u8]| {
                let mut val = vec![0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt7((&val[..]).get_u64().into())
            }),
        )(i)
    }

    fn pos_int_8(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1C"[..]),
            combinator::map(nom_bytes::complete::take(8u8), |mut x: &[u8]| {
                TupleValue::PosInt8(x.get_u64())
            }),
        )(i)
    }

    fn positive_arbitrary_precision_integer(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1D"[..]),
            combinator::map(multi::length_data(number::complete::be_u8), |x| {
                TupleValue::PositiveArbitraryPrecisionInteger(BigInt::from_bytes_be(Sign::Plus, x))
            }),
        )(i)
    }

    pub(self) mod tuple_extractor {
        use super::TupleValue;
        use crate::{FdbError, FdbResult};
        use num_bigint::BigInt;
        use std::convert::TryInto;

        fn tuple_extractor_error() -> FdbError {
            FdbError::new(200)
        }

        pub(crate) fn neg_int_8_bigint(tv: TupleValue) -> FdbResult<BigInt> {
            if let TupleValue::NegInt8(i) = tv {
                Ok(Into::<BigInt>::into(i) * -1)
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_8_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::NegInt8(i) = tv {
                (Into::<i128>::into(i) * -1)
                    .try_into()
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_7_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::NegInt7(i) = tv {
                // Even though NegInt5's range
                // -72057594037927935..=-281474976710656 is well
                // within i64::MIN (-9223372036854775808), this
                // information is not known to `i: u64`. So we need to
                // use `try_into()`.
                i.try_into()
                    .map(|x: i64| x * -1)
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_6_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::NegInt6(i) = tv {
                // Even though NegInt5's range
                // -281474976710655..=-1099511627776 is well within
                // i64::MIN (-9223372036854775808), this information
                // is not known to `i: u64`. So we need to use
                // `try_into()`.
                i.try_into()
                    .map(|x: i64| x * -1)
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_5_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::NegInt5(i) = tv {
                // Even though NegInt5's range
                // -1099511627775..=-4294967296 is well within
                // i64::MIN (-9223372036854775808), this information
                // is not known to `i: u64`. So we need to use
                // `try_into()`.
                i.try_into()
                    .map(|x: i64| x * -1)
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_4_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::NegInt4(i) = tv {
                Ok(Into::<i64>::into(i) * -1)
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_4_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::NegInt4(i) = tv {
                (Into::<i64>::into(i) * -1)
                    .try_into()
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_3_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::NegInt3(i) = tv {
                // Even though NegInt3's range -16777215..=-65536 is
                // well within i32::MIN (-2147483648), this
                // information is not known to `i: u32`. So, we need
                // to use `try_into()`.
                i.try_into()
                    .map(|x: i32| x * -1)
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_2_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::NegInt2(i) = tv {
                Ok(Into::<i32>::into(i) * -1)
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_2_i16(tv: TupleValue) -> FdbResult<i16> {
            if let TupleValue::NegInt2(i) = tv {
                (Into::<i32>::into(i) * -1)
                    .try_into()
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_1_i16(tv: TupleValue) -> FdbResult<i16> {
            if let TupleValue::NegInt1(i) = tv {
                Ok(Into::<i16>::into(i) * -1)
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn neg_int_1_i8(tv: TupleValue) -> FdbResult<i8> {
            if let TupleValue::NegInt1(i) = tv {
                (Into::<i16>::into(i) * -1)
                    .try_into()
                    .map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn int_zero(tv: TupleValue) -> FdbResult<i8> {
            if let TupleValue::IntZero = tv {
                Ok(0)
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_1_i8(tv: TupleValue) -> FdbResult<i8> {
            if let TupleValue::PosInt1(i) = tv {
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_1_i16(tv: TupleValue) -> FdbResult<i16> {
            if let TupleValue::PosInt1(i) = tv {
                Ok(i.into())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_2_i16(tv: TupleValue) -> FdbResult<i16> {
            if let TupleValue::PosInt2(i) = tv {
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_2_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::PosInt2(i) = tv {
                Ok(i.into())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_3_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::PosInt3(i) = tv {
                // Even though PosInt3's range 65536..=16777215 is
                // within i32::MAX (2147483647), this information is
                // not known to `i: u32`. So we need to use
                // `try_into()`.
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_4_i32(tv: TupleValue) -> FdbResult<i32> {
            if let TupleValue::PosInt4(i) = tv {
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_4_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::PosInt4(i) = tv {
                Ok(i.into())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_5_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::PosInt5(i) = tv {
                // Even though PosInt5's range
                // 4294967296..=1099511627775 is within i64::MAX
                // (9223372036854775807), this information is not
                // known to `i: u64`. So we need to use `try_into()`.
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_6_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::PosInt6(i) = tv {
                // Even though PosInt6's range
                // 1099511627776..=281474976710655 is within i64::MAX
                // (9223372036854775807), this information is not
                // known to `i: u64`. So we need to use `try_into()`.
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_7_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::PosInt7(i) = tv {
                // Even though PosInt6's range
                // 281474976710656..=72057594037927935 is within
                // i64::MAX (9223372036854775807), this information is
                // not known to `i: u64`. So we need to use
                // `try_into()`.
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_8_i64(tv: TupleValue) -> FdbResult<i64> {
            if let TupleValue::PosInt8(i) = tv {
                i.try_into().map_err(|_| tuple_extractor_error())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn pos_int_8_bigint(tv: TupleValue) -> FdbResult<BigInt> {
            if let TupleValue::PosInt8(i) = tv {
                Ok(i.into())
            } else {
                Err(tuple_extractor_error())
            }
        }

        pub(crate) fn positive_arbitrary_precision_integer(tv: TupleValue) -> FdbResult<BigInt> {
            if let TupleValue::PositiveArbitraryPrecisionInteger(i) = tv {
                Ok(i)
            } else {
                Err(tuple_extractor_error())
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{
            int_zero, neg_int_1, neg_int_2, neg_int_3, neg_int_4, neg_int_5, neg_int_6, neg_int_7,
            neg_int_8, pos_int_1, pos_int_2, pos_int_3, pos_int_4, pos_int_5, pos_int_6, pos_int_7,
            pos_int_8, positive_arbitrary_precision_integer, tuple_extractor, TupleValue,
        };
        use nom::error::{Error, ErrorKind};
        use num_bigint::BigInt;
        use std::num::NonZeroUsize;

        #[test]
        fn test_neg_int_8() {
            assert_eq!(
                neg_int_8(&b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt8(18446744073709551615)))
            );
            assert_eq!(
                neg_int_8(&b"\x0C\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt8(72057594037927936)))
            );
            assert_eq!(
                neg_int_8(&b"no_neg_int_8"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_8"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_8_bigint(
                    neg_int_8(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFEmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap()
            );
            assert_eq!(
                tuple_extractor::neg_int_8_i64(
                    neg_int_8(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -9223372036854775808i64
            );
        }

        #[test]
        fn test_neg_int_7() {
            assert_eq!(
                neg_int_7(&b"\x0D\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt7(72057594037927935)))
            );
            assert_eq!(
                neg_int_7(&b"\x0D\xFE\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt7(281474976710656)))
            );
            assert_eq!(
                neg_int_7(&b"no_neg_int_7"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_7"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_7_i64(
                    neg_int_7(&b"\x0D\x00\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -72057594037927935i64
            );
            assert_eq!(
                tuple_extractor::neg_int_7_i64(
                    neg_int_7(&b"\x0D\xFE\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -281474976710656i64
            );
        }

        #[test]
        fn test_neg_int_6() {
            assert_eq!(
                neg_int_6(&b"\x0E\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt6(281474976710655)))
            );
            assert_eq!(
                neg_int_6(&b"\x0E\xFE\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt6(1099511627776)))
            );
            assert_eq!(
                neg_int_6(&b"no_neg_int_6"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_6"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_6_i64(
                    neg_int_6(&b"\x0E\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -281474976710655i64
            );
            assert_eq!(
                tuple_extractor::neg_int_6_i64(
                    neg_int_6(&b"\x0E\xFE\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -1099511627776i64
            );
        }

        #[test]
        fn test_neg_int_5() {
            assert_eq!(
                neg_int_5(&b"\x0F\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt5(1099511627775)))
            );
            assert_eq!(
                neg_int_5(&b"\x0F\xFE\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt5(4294967296)))
            );
            assert_eq!(
                neg_int_5(&b"no_neg_int_5"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_5"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_5_i64(
                    neg_int_5(&b"\x0F\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -1099511627775i64
            );
            assert_eq!(
                tuple_extractor::neg_int_5_i64(
                    neg_int_5(&b"\x0F\xFE\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -4294967296i64
            );
        }

        #[test]
        fn test_neg_int_4() {
            assert_eq!(
                neg_int_4(&b"\x10\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt4(4294967295)))
            );
            assert_eq!(
                neg_int_4(&b"\x10\xFE\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt4(16777216)))
            );
            assert_eq!(
                neg_int_4(&b"no_neg_int_4"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_4"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_4_i64(
                    neg_int_4(&b"\x10\x7F\xFF\xFF\xFEmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -2147483649i64
            );
            assert_eq!(
                tuple_extractor::neg_int_4_i32(
                    neg_int_4(&b"\x10\x7F\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -2147483648i32
            );
        }

        #[test]
        fn test_neg_int_3() {
            assert_eq!(
                neg_int_3(&b"\x11\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt3(16777215)))
            );
            assert_eq!(
                neg_int_3(&b"\x11\xFE\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt3(65536)))
            );
            assert_eq!(
                neg_int_3(&b"no_neg_int_3"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_3"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_3_i32(
                    neg_int_3(&b"\x11\x00\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                -16777215i32
            );
            assert_eq!(
                tuple_extractor::neg_int_3_i32(
                    neg_int_3(&b"\x11\xFE\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -65536i32
            );
        }

        #[test]
        fn test_neg_int_2() {
            assert_eq!(
                neg_int_2(&b"\x12\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt2(65535)))
            );
            assert_eq!(
                neg_int_2(&b"\x12\xFE\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt2(256)))
            );
            assert_eq!(
                neg_int_2(&b"no_neg_int_2"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_2"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_2_i32(neg_int_2(&b"\x12\x7F\xFEmoredata"[..]).unwrap().1)
                    .unwrap(),
                -32769i32
            );
            assert_eq!(
                tuple_extractor::neg_int_2_i16(neg_int_2(&b"\x12\x7F\xFFmoredata"[..]).unwrap().1)
                    .unwrap(),
                -32768i16
            );
        }

        #[test]
        fn test_neg_int_1() {
            assert_eq!(
                neg_int_1(&b"\x13\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt1(255)))
            );
            assert_eq!(
                neg_int_1(&b"\x13\xFEmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt1(1)))
            );
            assert_eq!(
                neg_int_1(&b"no_neg_int_1"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_1"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_1_i16(neg_int_1(&b"\x13\x7Emoredata"[..]).unwrap().1)
                    .unwrap(),
                -129i16
            );
            assert_eq!(
                tuple_extractor::neg_int_1_i8(neg_int_1(&b"\x13\x7Fmoredata"[..]).unwrap().1)
                    .unwrap(),
                -128i8
            );
        }

        #[test]
        fn test_int_zero() {
            assert_eq!(
                int_zero(&b"\x14moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::IntZero))
            );
            assert_eq!(
                int_zero(&b"no_zero"[..]),
                Err(nom::Err::Error(Error::new(&b"no_zero"[..], ErrorKind::Tag)))
            );

            assert_eq!(
                tuple_extractor::int_zero(int_zero(&b"\x14moredata"[..]).unwrap().1).unwrap(),
                0
            );
        }

        #[test]
        fn test_pos_int_1() {
            assert_eq!(
                pos_int_1(&b"\x15\x01moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt1(1)))
            );
            assert_eq!(
                pos_int_1(&b"\x15\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt1(255)))
            );
            assert_eq!(
                pos_int_1(&b"no_pos_int_1"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_1"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_1_i8(pos_int_1(&b"\x15\x7Fmoredata"[..]).unwrap().1)
                    .unwrap(),
                127i8
            );
            assert_eq!(
                tuple_extractor::pos_int_1_i16(pos_int_1(&b"\x15\x80moredata"[..]).unwrap().1)
                    .unwrap(),
                128i16
            );
        }

        #[test]
        fn test_pos_int_2() {
            assert_eq!(
                pos_int_2(&b"\x16\x01\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt2(256)))
            );
            assert_eq!(
                pos_int_2(&b"\x16\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt2(65535)))
            );
            assert_eq!(
                pos_int_2(&b"no_pos_int_2"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_2"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_2_i16(pos_int_2(&b"\x16\x7F\xFFmoredata"[..]).unwrap().1)
                    .unwrap(),
                32767i16
            );
            assert_eq!(
                tuple_extractor::pos_int_2_i32(pos_int_2(&b"\x16\x80\x00moredata"[..]).unwrap().1)
                    .unwrap(),
                32768i32
            );
        }

        #[test]
        fn test_pos_int_3() {
            assert_eq!(
                pos_int_3(&b"\x17\x01\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt3(65536)))
            );
            assert_eq!(
                pos_int_3(&b"\x17\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt3(16777215)))
            );
            assert_eq!(
                pos_int_3(&b"no_pos_int_3"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_3"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_3_i32(
                    pos_int_3(&b"\x17\x01\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                65536i32
            );
            assert_eq!(
                tuple_extractor::pos_int_3_i32(
                    pos_int_3(&b"\x17\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                16777215i32,
            );
        }

        #[test]
        fn test_pos_int_4() {
            assert_eq!(
                pos_int_4(&b"\x18\x01\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt4(16777216)))
            );
            assert_eq!(
                pos_int_4(&b"\x18\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt4(4294967295)))
            );
            assert_eq!(
                pos_int_4(&b"no_pos_int_4"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_4"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_4_i32(
                    pos_int_4(&b"\x18\x7F\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                2147483647i32
            );
            assert_eq!(
                tuple_extractor::pos_int_4_i64(
                    pos_int_4(&b"\x18\x80\x00\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                2147483648i64
            );
        }

        #[test]
        fn test_pos_int_5() {
            assert_eq!(
                pos_int_5(&b"\x19\x01\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt5(4294967296)))
            );
            assert_eq!(
                pos_int_5(&b"\x19\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt5(1099511627775)))
            );
            assert_eq!(
                pos_int_5(&b"no_pos_int_5"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_5"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_5_i64(
                    pos_int_5(&b"\x19\x01\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                4294967296i64
            );
            assert_eq!(
                tuple_extractor::pos_int_5_i64(
                    pos_int_5(&b"\x19\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                1099511627775i64
            );
        }

        #[test]
        fn test_pos_int_6() {
            assert_eq!(
                pos_int_6(&b"\x1A\x01\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt6(1099511627776)))
            );
            assert_eq!(
                pos_int_6(&b"\x1A\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt6(281474976710655)))
            );
            assert_eq!(
                pos_int_6(&b"no_pos_int_6"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_6"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_6_i64(
                    pos_int_6(&b"\x1A\x01\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                1099511627776i64
            );
            assert_eq!(
                tuple_extractor::pos_int_6_i64(
                    pos_int_6(&b"\x1A\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                281474976710655i64
            );
        }

        #[test]
        fn test_pos_int_7() {
            assert_eq!(
                pos_int_7(&b"\x1B\x01\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt7(281474976710656)))
            );
            assert_eq!(
                pos_int_7(&b"\x1B\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt7(72057594037927935)))
            );
            assert_eq!(
                pos_int_7(&b"no_pos_int_7"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_7"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_7_i64(
                    pos_int_7(&b"\x1B\x01\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                281474976710656i64
            );
            assert_eq!(
                tuple_extractor::pos_int_7_i64(
                    pos_int_7(&b"\x1B\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                72057594037927935i64
            );
        }

        #[test]
        fn test_pos_int_8() {
            assert_eq!(
                pos_int_8(&b"\x1C\x01\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt8(72057594037927936)))
            );
            assert_eq!(
                pos_int_8(&b"\x1C\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt8(18446744073709551615)))
            );
            assert_eq!(
                pos_int_8(&b"no_pos_int_8"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_8"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_8_i64(
                    pos_int_8(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                9223372036854775807i64
            );
            assert_eq!(
                tuple_extractor::pos_int_8_bigint(
                    pos_int_8(&b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"9223372036854775808", 10).unwrap()
            );
        }

        #[test]
        fn test_positive_arbitrary_precision_integer() {
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::PositiveArbitraryPrecisionInteger(
                        BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                    )
                ))
            );
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"no_positive_arbitrary_precision_integer"[..]
                ),
                Err(nom::Err::Error(Error::new(
                    &b"no_positive_arbitrary_precision_integer"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00"[..]
                ),
                Err(nom::Err::Incomplete(nom::Needed::Size(
                    NonZeroUsize::new(1).unwrap()
                )))
            );
            assert_eq!(
                tuple_extractor::positive_arbitrary_precision_integer(
                    positive_arbitrary_precision_integer(
                        &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]
                    )
                    .unwrap()
                    .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
            );
        }

        // #[test]
        // fn test_size_limits_pos_int() {
        //     let size_limits_pos: Vec<i128> = vec![
        //         // (1 << (0 * 8)) - 1,
        //         // (1 << (1 * 8)) - 1,
        //         // (1 << (2 * 8)) - 1,
        //         // (1 << (3 * 8)) - 1,
        //         // (1 << (4 * 8)) - 1,
        //         // (1 << (5 * 8)) - 1,
        //         // (1 << (6 * 8)) - 1,
        //         // (1 << (7 * 8)) - 1,
        //         // (1 << (8 * 8)) - 1,
        //     ];
        //     println!("size_limits_pos_int: {:?}", size_limits_pos);
        // }

        // #[test]
        // fn test_size_limits_neg_int() {
        //     let size_limits_neg: Vec<i128> = vec![
        //         // ((1 << (0 * 8)) * -1) + 1,
        //         // ((1 << (1 * 8)) * -1) + 1,
        //         // ((1 << (2 * 8)) * -1) + 1,
        //         // ((1 << (3 * 8)) * -1) + 1,
        //         // ((1 << (4 * 8)) * -1) + 1,
        //         // ((1 << (5 * 8)) * -1) + 1,
        //         // ((1 << (6 * 8)) * -1) + 1,
        //         // ((1 << (7 * 8)) * -1) + 1,
        //         // ((1 << (8 * 8)) * -1) + 1,
        //     ];
        //     println!("size_limits_neg_int: {:?}", size_limits_neg);
        // }
    }
}
