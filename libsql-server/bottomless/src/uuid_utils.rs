// Copy-pasted from uuid crate to avoid their uuid_unstable flag guard.
// Once uuid v7 is standardized and stabilized, we can go back to using uuid::new_v7() directly.

use uuid::{NoContext, Timestamp, Uuid};

fn bytes() -> [u8; 16] {
    rand::random()
}

pub(crate) const fn encode_unix_timestamp_millis(millis: u64, random_bytes: &[u8; 10]) -> Uuid {
    let millis_high = ((millis >> 16) & 0xFFFF_FFFF) as u32;
    let millis_low = (millis & 0xFFFF) as u16;

    let random_and_version =
        (random_bytes[1] as u16 | ((random_bytes[0] as u16) << 8) & 0x0FFF) | (0x7 << 12);

    let mut d4 = [0; 8];

    d4[0] = (random_bytes[2] & 0x3F) | 0x80;
    d4[1] = random_bytes[3];
    d4[2] = random_bytes[4];
    d4[3] = random_bytes[5];
    d4[4] = random_bytes[6];
    d4[5] = random_bytes[7];
    d4[6] = random_bytes[8];
    d4[7] = random_bytes[9];

    Uuid::from_fields(millis_high, millis_low, random_and_version, &d4)
}

pub fn new_v7(ts: Timestamp) -> Uuid {
    let (secs, nanos) = ts.to_unix();
    let millis = (secs * 1000).saturating_add(nanos as u64 / 1_000_000);

    encode_unix_timestamp_millis(millis, &bytes()[..10].try_into().unwrap())
}

pub(crate) fn decode_unix_timestamp(uuid: &Uuid) -> Timestamp {
    // taken from uuid crate (unsafe features)
    let bytes = uuid.as_bytes();

    let millis: u64 = (bytes[0] as u64) << 40
        | (bytes[1] as u64) << 32
        | (bytes[2] as u64) << 24
        | (bytes[3] as u64) << 16
        | (bytes[4] as u64) << 8
        | (bytes[5] as u64);

    let seconds = millis / 1000;
    let nanos = ((millis % 1000) * 1_000_000) as u32;
    Timestamp::from_unix(NoContext, seconds, nanos)
}

#[cfg(test)]
mod test {
    use crate::uuid_utils::{decode_unix_timestamp, new_v7};
    use uuid::{NoContext, Timestamp};

    #[test]
    fn timestamp_uuid_conversion() {
        let ts = Timestamp::now(NoContext);
        let uuid = new_v7(ts);
        let actual = decode_unix_timestamp(&uuid);
        //TODO: information loss on encoding?
        let (s1, _) = actual.to_unix();
        let (s2, _) = ts.to_unix();
        assert_eq!(s1, s2);
    }
}
