use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use super::{Result, Error};

use util::codec::bytes;

pub fn encode_key(key: &[u8], version: u64) -> Vec<u8> {
    let mut v = bytes::encode_bytes(key);
    v.write_u64::<BigEndian>(version).unwrap();
    v
}

pub fn decode_key(data: &[u8]) -> Result<(Vec<u8>, u64)> {
    let (key, read_size) = try!(bytes::decode_bytes(data));
    match data[read_size..].as_ref().read_u64::<BigEndian>() {
        Ok(ver) => Ok((key, ver)),
        Err(_) => Err(Error::KeyVersion),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_key, decode_key};

    #[test]
    fn test_encode_key() {
        let pairs: Vec<(&[u8], u64)> = vec![
          (b"abc", 0),
          (b"\x00\x00", 100),
        ];

        for (x, y) in pairs {
            let data = encode_key(x, y);
            let (k, ver) = decode_key(&data).unwrap();
            assert_eq!((x, y), (&k as &[u8], ver));
        }
    }

    #[test]
    fn test_key_compare() {
        let a1 = encode_key(b"A", 1);
        let a2 = encode_key(b"A", 2);
        let b1 = encode_key(b"B", 1);
        assert!(a2 > a1);
        assert!(a2 < b1);
    }
}
