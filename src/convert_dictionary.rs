use std::{io, cmp::Ordering, path::PathBuf, time::SystemTime, collections::HashMap};

use bytes::BytesMut;
use terminus_store::{storage::{PersistentLayerStore, archive::ArchiveLayerStore, consts::{self, LayerFileEnum, FILENAME_ENUM_MAP}, FileLoad, string_to_name}, structure::{TypedDict, Datatype, TypedDictBufBuilder, TdbDataType, LangString, TypedDictEntry}};

use crate::dataconversion::prolog_string_to_string;

pub async fn convert_value_dict(in_store: &ArchiveLayerStore, out_store: &ArchiveLayerStore, id: [u32;5], offset: u64) -> io::Result<(HashMap<u64, u64>, u64)> {
    let types_present_file = in_store.get_file(id, consts::FILENAMES.value_dictionary_types_present).await?;
    let type_offsets_file = in_store.get_file(id, consts::FILENAMES.value_dictionary_type_offsets).await?;
    let blocks_file = in_store.get_file(id, consts::FILENAMES.value_dictionary_blocks).await?;
    let offsets_file = in_store.get_file(id, consts::FILENAMES.value_dictionary_offsets).await?;

    let types_present_map = types_present_file.map().await?;
    let type_offsets_map = type_offsets_file.map().await?;
    let blocks_map = blocks_file.map().await?;
    let offsets_map = offsets_file.map().await?;

    let dict = TypedDict::from_parts(types_present_map,
                                     type_offsets_map,
                                     offsets_map,
                                     blocks_map);

    let mut new_entries: Vec<(TypedDictEntry, u64)> = Vec::with_capacity(dict.num_entries());
    let mut reorder = false;
    for (ix, entry) in dict.into_iter().enumerate().map(|(ix,e)|(ix as u64+offset,e)) {
        let next_entry;
        match entry.datatype() {
            Datatype::String|
            Datatype::NCName|
            Datatype::Name|
            Datatype::Token|
            Datatype::NMToken|
            Datatype::NormalizedString|
            Datatype::Language|
            Datatype::AnyURI|
            Datatype::Notation|
            Datatype::QName|
            Datatype::ID|
            Datatype::IDRef|
            Datatype::Entity|
            Datatype::AnySimpleType => {
                let s = entry.as_val::<String, String>();
                let converted = prolog_string_to_string(&s);
                next_entry = String::make_entry(&converted);
            },Datatype::LangString => {
                let s: String = entry.as_val::<LangString, String>();
                let pos = s.find('@').expect("no @ found in langstring");

                let mut lang = &s[..pos];
                if &lang[0..1] == "\'" || &lang[0..1] == "\"" {
                    lang = &lang[1..lang.len()-1];
                }

                let val = &s[pos+2..s.len()-1];
                let string_converted = prolog_string_to_string(val);

                let mut converted = String::with_capacity(s.len());
                converted.push_str(lang);
                converted.push('@');
                converted.push_str(&string_converted);

                next_entry = LangString::make_entry(&converted);
            },
            _ => {
                next_entry = entry;
            }
        }

        if let Some((last,_)) = new_entries.last() {
            match last.cmp(&next_entry) {
                Ordering::Equal => panic!("after normalizing prolog strings, two entries were equal"),
                Ordering::Greater => {
                    reorder = true},
                _ => {}
            }
        }
        new_entries.push((next_entry, ix));
    }


    let reordered_ids: HashMap<u64, u64>;
    if reorder {
        // yikes, the order changed, we'll have to do a lot of work
        eprintln!(" reordering..");
        new_entries.sort();

        reordered_ids = new_entries.iter().enumerate().map(|(new_id, (_, old_id))| (*old_id + offset, new_id as u64)).collect();
    } else {
        reordered_ids = (offset..(offset+new_entries.len() as u64)).map(|ix|(ix,ix)).collect();
    }

    let new_offset = offset + new_entries.len() as u64;

    let mut new_types_present_map = BytesMut::new();
    let mut new_type_offsets_map = BytesMut::new();
    let mut new_offsets_map = BytesMut::new();
    let mut new_blocks_map = BytesMut::new();

    let mut builder =TypedDictBufBuilder::new(&mut new_types_present_map, &mut new_type_offsets_map, &mut new_offsets_map, &mut new_blocks_map);
    builder.add_all(new_entries.into_iter().map(|(e,_)|e));

    builder.finalize();
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryTypesPresent, new_types_present_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryTypeOffsets, new_type_offsets_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryOffsets, new_offsets_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryBlocks, new_blocks_map.freeze());

    Ok((reordered_ids, new_offset))
}
