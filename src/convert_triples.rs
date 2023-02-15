use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use futures::stream::TryStreamExt;
use terminus_store::{storage::FileLoad, structure::{logarray_file_get_length_and_width, bitarray_stream_bits, logarray_stream_entries, LogArrayBufBuilder}};

use std::io;

pub async fn convert_sp_o_nums<F: FileLoad + 'static>(
    bits: F,
    nums: F,
    mapping: &HashMap<u64, u64>,
) -> io::Result<Bytes> {
    let (_len, width) = logarray_file_get_length_and_width(nums.clone()).await?;
    let mut bits_stream = bitarray_stream_bits(bits).await?;
    let mut nums_stream = logarray_stream_entries(nums).await?;

    let mut buf = BytesMut::new();
    let mut builder = LogArrayBufBuilder::new(&mut buf, width);

    let mut tally = 0;
    while let Some(b) = bits_stream.try_next().await? {
        tally += 1;
        if b {
            // we hit a boundary, read just as many nums and that is our group slice
            let mut v = Vec::with_capacity(tally);
            for _ in 0..tally {
                let unmapped = nums_stream.try_next().await?.unwrap();
                let mapped = mapping.get(&unmapped).cloned().unwrap_or(unmapped);
                v.push(mapped);
            }
            v.sort();

            builder.push_vec(v);
            tally = 0;
        }
    }

    builder.finalize();

    Ok(buf.freeze())
}
