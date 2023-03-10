mod reachable;
mod conversion_consts;
mod convert_triples;
mod convert_layer;
pub mod convert_store;
mod convert_dictionary;
mod dataconversion;

/*
pub async fn convert_store(in_store_path: PathBuf, out_store_path: PathBuf, conversion_datetime: DateTime<Local>) -> io::Result<()> {
    let in_store = ArchiveLayerStore::new(&in_store_path);
    let out_store = ArchiveLayerStore::new(&out_store_path);

    let conversion_systemtime: SystemTime = conversion_datetime.into();

    let mut entries = tokio::fs::read_dir(&in_store_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }

        let name = entry.file_name().into_string();
        if name.is_err() {
            continue;
        }

        let name = name.unwrap();
        if name.len() != 3 || name.find(|c| !is_hex_char(c)).is_some() {
            continue;
        }

        let mut inner_path = in_store_path.clone();
        inner_path.push(name);
        let mut inner_entries = tokio::fs::read_dir(inner_path).await?;

        while let Some(inner_entry) = inner_entries.next_entry().await? {
            let layer_file_path = inner_entry.path();
            if !layer_file_path.extension().map(|e|e == "larch").unwrap_or(false) {
                continue;
            }

            let layer_name = layer_file_path.file_stem().unwrap().to_str().unwrap();
            let layer = string_to_name(layer_name).unwrap();

            let metadata = tokio::fs::metadata(&layer_file_path).await?;
            let creation_timestamp = metadata.created()?;

            if creation_timestamp <= conversion_systemtime {
                eprintln!("converting layer {layer_name}");
                convert_layer(&in_store, &out_store, layer).await?;
            }
            else {
                eprintln!("copying layer {layer_name}");
                let mut destination_file_path = out_store_path.clone();
                destination_file_path.push(&layer_name[..3]);
                destination_file_path.push(format!("{}.larch", layer_name));
                tokio::fs::copy(layer_file_path, destination_file_path).await?;
            }
        }
    }

    let mut entries = tokio::fs::read_dir(in_store_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let name = entry.file_name().into_string();
            if name.is_err() {
                // our label files are all ascii so this is not a label file
                continue;
            }

            let name = name.unwrap();

            if name.ends_with(".label") {
                let mut destination_file_path = out_store_path.clone();
                destination_file_path.push(name);
                tokio::fs::copy(entry.path(), destination_file_path).await?;
            }
        }
    }

    // TODO write storage version

    Ok(())
}

fn is_hex_char(c: char) -> bool {
    if c >= '0' && c <= '9' {
        true
    } else if c >= 'a' && c <= 'f' {
        true
    } else {
        false
    }
}

pub async fn convert_layer(in_store: &ArchiveLayerStore, out_store: &ArchiveLayerStore, id: [u32;5]) -> io::Result<()> {
    out_store.create_named_directory(id).await?;

    let reorder = convert_value_dict(in_store, out_store, id).await?;
    if reorder {
        // yikes, ordering changed, we gotta do a lot of work
        todo!("Normalizing prolog strings in layer caused a reordering. This is currently unsupported");
    }
    else {
        copy_unchanged_files(in_store, out_store, id).await?;
    }

    out_store.finalize(id).await?;

    Ok(())
}

async fn convert_value_dict(in_store: &ArchiveLayerStore, out_store: &ArchiveLayerStore, id: [u32;5]) -> io::Result<bool> {
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

    let mut new_entries: Vec<TypedDictEntry> = Vec::with_capacity(dict.num_entries());
    let mut reorder = false;
    for entry in dict.into_iter() {
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

        if let Some(last) = new_entries.last() {
            match last.cmp(&next_entry) {
                Ordering::Equal => panic!("after normalizing prolog strings, two entries were equal"),
                Ordering::Greater => {
                    panic!("encountered elements in the wrong order");
                    reorder = true},
                _ => {}
            }
        }
        new_entries.push(next_entry);
    }

    if reorder {
        // yikes, the order changed, we'll have to do a lot of work
        new_entries.sort();
    }
    else {
    }

    let mut new_types_present_map = BytesMut::new();
    let mut new_type_offsets_map = BytesMut::new();
    let mut new_offsets_map = BytesMut::new();
    let mut new_blocks_map = BytesMut::new();

    let mut builder =TypedDictBufBuilder::new(&mut new_types_present_map, &mut new_type_offsets_map, &mut new_offsets_map, &mut new_blocks_map);
    builder.add_all(new_entries.into_iter());

    builder.finalize();
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryTypesPresent, new_types_present_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryTypeOffsets, new_type_offsets_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryOffsets, new_offsets_map.freeze());
    out_store.write_bytes(id, LayerFileEnum::ValueDictionaryBlocks, new_blocks_map.freeze());

    Ok(reorder)
}

async fn copy_unchanged_files(in_store: &ArchiveLayerStore, out_store: &ArchiveLayerStore, id: [u32;5]) -> io::Result<()> {
    let unchanged_files = vec![
        "node_dictionary_blocks.tfc",
        "node_dictionary_offsets.logarray",
        "predicate_dictionary_blocks.tfc",
        "predicate_dictionary_offsets.logarray",
        "node_value_idmap_bits.bitarray",
        "node_value_idmap_bit_index_blocks.bitarray",
        "node_value_idmap_bit_index_sblocks.bitarray",
        "predicate_idmap_bits.bitarray",
        "predicate_idmap_bit_index_blocks.bitarray",
        "predicate_idmap_bit_index_sblocks.bitarray",
        "child_pos_subjects.logarray",
        "child_pos_objects.logarray",
        "child_neg_subjects.logarray",
        "child_neg_objects.logarray",
        "pos_s_p_adjacency_list_nums.logarray",
        "pos_s_p_adjacency_list_bits.bitarray",
        "pos_s_p_adjacency_list_bit_index_blocks.logarray",
        "pos_s_p_adjacency_list_bit_index_sblocks.logarray",
        "pos_sp_o_adjacency_list_nums.logarray",
        "pos_sp_o_adjacency_list_bits.bitarray",
        "pos_sp_o_adjacency_list_bit_index_blocks.logarray",
        "pos_sp_o_adjacency_list_bit_index_sblocks.logarray",
        "pos_o_ps_adjacency_list_nums.logarray",
        "pos_o_ps_adjacency_list_bits.bitarray",
        "pos_o_ps_adjacency_list_bit_index_blocks.logarray",
        "pos_o_ps_adjacency_list_bit_index_sblocks.logarray",
        "pos_predicate_wavelet_tree_bits.bitarray",
        "pos_predicate_wavelet_tree_bit_index_blocks.logarray",
        "pos_predicate_wavelet_tree_bit_index_sblocks.logarray",
        "neg_s_p_adjacency_list_nums.logarray",
        "neg_s_p_adjacency_list_bits.bitarray",
        "neg_s_p_adjacency_list_bit_index_blocks.logarray",
        "neg_s_p_adjacency_list_bit_index_sblocks.logarray",
        "neg_sp_o_adjacency_list_nums.logarray",
        "neg_sp_o_adjacency_list_bits.bitarray",
        "neg_sp_o_adjacency_list_bit_index_blocks.logarray",
        "neg_sp_o_adjacency_list_bit_index_sblocks.logarray",
        "neg_o_ps_adjacency_list_nums.logarray",
        "neg_o_ps_adjacency_list_bits.bitarray",
        "neg_o_ps_adjacency_list_bit_index_blocks.logarray",
        "neg_o_ps_adjacency_list_bit_index_sblocks.logarray",
        "neg_predicate_wavelet_tree_bits.bitarray",
        "neg_predicate_wavelet_tree_bit_index_blocks.logarray",
        "neg_predicate_wavelet_tree_bit_index_sblocks.logarray",
        "parent.hex"
    ];

    for unchanged in unchanged_files {
        if let Some(map) = in_store.get_file(id, unchanged).await?.map_if_exists().await? {
            let enum_val = FILENAME_ENUM_MAP[unchanged];
            out_store.write_bytes(id, enum_val, map);
        }
    }

    Ok(())
}
*/
