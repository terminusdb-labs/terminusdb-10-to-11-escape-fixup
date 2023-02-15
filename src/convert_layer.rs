use terminus_store::layer::builder::build_object_index;
use terminus_store::storage::AdjacencyListFiles;
use terminus_store::storage::BitIndexFiles;
use terminus_store::storage::FileLoad;
use terminus_store::storage::LayerStore;
use terminus_store::storage::PersistentLayerStore;
use terminus_store::storage::archive::ArchiveLayerStore;
use terminus_store::storage::consts::FILENAMES;
use terminus_store::storage::consts::FILENAME_ENUM_MAP;
use terminus_store::storage::name_to_string;
use terminus_store::storage::string_to_name;
use tokio::io::AsyncReadExt;

use crate::conversion_consts::BASE_INDEX_FILES;
use crate::conversion_consts::CHILD_INDEX_FILES;
use crate::conversion_consts::UNCHANGED_FILES;
use crate::convert_dictionary::convert_value_dict;
use crate::convert_triples::*;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use bytes::Bytes;

use serde::{Deserialize, Serialize};

use tokio::io::AsyncWriteExt;

use thiserror::Error;

pub async fn convert_layer(
    from: &str,
    to: &str,
    work: &str,
    verbose: bool,
    id_string: &str,
) -> Result<(), LayerConversionError> {
    let from_store = ArchiveLayerStore::new(from);
    let to_store = ArchiveLayerStore::new(to);
    let id = string_to_name(id_string).unwrap();

    convert_layer_with_stores(&from_store, &to_store, work, verbose, id).await
}

#[derive(Debug, Error)]
pub enum InnerLayerConversionError {
    //#[error(transparent)]
    //DictionaryConversion(#[from] DictionaryConversionError),
    #[error("layer was already converted")]
    LayerAlreadyConverted,

    #[error("failed to copy {name}: {source}")]
    FileCopyError { name: String, source: io::Error },

    #[error(transparent)]
    ParentMapError(#[from] ParentMapError),

    #[error("failed to convert triple map: {0}")]
    TripleConversionError(io::Error),

    #[error("failed to rebuild indexes: {0}")]
    RebuildIndexError(io::Error),

    #[error("failed to finalize layer: {0}")]
    FinalizationError(io::Error),

    #[allow(unused)]
    #[error("failed to copy rollup file: {0}")]
    RollupFileCopyError(io::Error),

    #[error("failed to write the parent map: {0}")]
    ParentMapWriteError(io::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("a nodevalue remap file exists but was not expected")]
    NodeValueRemapExists,
}

#[derive(Debug, Error)]
#[error("Failed to convert layer {}: {source}", name_to_string(self.layer))]
pub struct LayerConversionError {
    layer: [u32; 5],
    source: InnerLayerConversionError,
}

impl LayerConversionError {
    fn new<E: Into<InnerLayerConversionError>>(layer: [u32; 5], source: E) -> Self {
        Self {
            layer,
            source: source.into(),
        }
    }
}

pub async fn convert_layer_with_stores(
    from_store: &ArchiveLayerStore,
    to_store: &ArchiveLayerStore,
    work: &str,
    verbose: bool,
    id: [u32; 5],
) -> Result<(), LayerConversionError> {
    println!("converting layer {}", name_to_string(id));
    let is_child = PersistentLayerStore::layer_has_parent(from_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;

    if PersistentLayerStore::directory_exists(to_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?
    {
        return Err(LayerConversionError::new(
            id,
            InnerLayerConversionError::LayerAlreadyConverted,
        ));
    }

    PersistentLayerStore::create_named_directory(to_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;

    assert_no_remap_exists(from_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;

    let map;
    let offset;
    let (mut mapping, offset_1) = get_mapping_and_offset(work, from_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;
    if verbose {
        println!("parent mappings retrieved");
    }
    let (mapping_addition, offset_1) = convert_value_dict(from_store, to_store, id, offset_1)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;
    mapping.extend(mapping_addition);
    if verbose {
        println!("dictionaries converted");
    }
    convert_triples(from_store, to_store, id, is_child, &mapping)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::TripleConversionError(e))
        })?;
    if verbose {
        println!("triples converted");
    }
    copy_unchanged_files(from_store, to_store, id).await?;
    if verbose {
        println!("files copied");
    }
    rebuild_indexes(to_store, id, is_child)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::RebuildIndexError(e))
        })?;
    if verbose {
        println!("indexes rebuilt");
    }
    map = Some(mapping);
    offset = Some(offset_1);

    PersistentLayerStore::finalize(to_store, id)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::FinalizationError(e))
        })?;

    /*
    // we copy the rollup only after finalizing, as rollups are not
    // part of a layer under construction
    copy_rollup_file(v10_store, v11_store, id)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::RollupFileCopyError(e))
        })?;
    */

    write_parent_map(&work, id, map.unwrap(), offset.unwrap())
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::ParentMapWriteError(e))
        })?;
    if verbose {
        println!("written parent map to workdir");
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum InnerParentMapError {
    #[error("not found")]
    ParentMapNotFound,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Deserialization(#[from] postcard::Error),
}

#[derive(Error, Debug)]
pub enum ParentMapError {
    #[error(transparent)]
    Io(io::Error),
    #[error("couldn't load parent map {}: {source}", name_to_string(*parent))]
    Other {
        parent: [u32; 5],
        source: InnerParentMapError,
    },
}

impl ParentMapError {
    fn new<E: Into<InnerParentMapError>>(parent: [u32; 5], source: E) -> Self {
        Self::Other {
            parent,
            source: source.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ParentMap {
    offset: u64,
    mapping: Vec<(u64, u64)>,
}

fn path_for_parent_map(workdir: &str, parent: [u32; 5]) -> PathBuf {
    let parent_string = name_to_string(parent);
    let prefix = &parent_string[..3];
    let mut pathbuf = PathBuf::from(workdir);
    pathbuf.push(prefix);
    pathbuf.push(format!("{parent_string}.postcard"));

    pathbuf
}

async fn get_mapping_and_offset_from_parent(
    workdir: &str,
    parent: [u32; 5],
) -> Result<(HashMap<u64, u64>, u64), ParentMapError> {
    let pathbuf = path_for_parent_map(workdir, parent);
    let file = tokio::fs::File::open(pathbuf).await;
    if file.is_err() && file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
        return Err(ParentMapError::new(
            parent,
            InnerParentMapError::ParentMapNotFound,
        ));
    }
    let mut file = file.map_err(|e| ParentMapError::new(parent, e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .await
        .map_err(|e| ParentMapError::new(parent, e))?;
    let ParentMap {
        offset,
        mapping: mapping_vec,
    } = postcard::from_bytes(&bytes).map_err(|e| ParentMapError::new(parent, e))?;
    let mut mapping = HashMap::with_capacity(mapping_vec.len());
    mapping.extend(mapping_vec);

    Ok((mapping, offset))
}

async fn get_mapping_and_offset(
    workdir: &str,
    store: &ArchiveLayerStore,
    id: [u32; 5],
) -> Result<(HashMap<u64, u64>, u64), ParentMapError> {
    // look up parent id if applicable
    if let Some(parent) = LayerStore::get_layer_parent_name(store, id)
        .await
        .map_err(|e| ParentMapError::Io(e))?
    {
        get_mapping_and_offset_from_parent(workdir, parent).await
    } else {
        Ok((HashMap::with_capacity(0), 0))
    }
}

async fn convert_triples(
    from_store: &ArchiveLayerStore,
    to_store: &ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
    mapping: &HashMap<u64, u64>,
) -> io::Result<()> {
    if is_child {
        let pos_bits = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.pos_sp_o_adjacency_list_bits,
        )
        .await?;
        let pos_nums = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.pos_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(pos_bits, pos_nums, mapping).await?;
        write_bytes_to_file(
            to_store,
            id,
            FILENAMES.pos_sp_o_adjacency_list_nums,
            output_nums,
        );

        let neg_bits = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_bits,
        )
        .await?;
        let neg_nums = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(neg_bits, neg_nums, mapping).await?;
        write_bytes_to_file(
            to_store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_nums,
            output_nums,
        );
    } else {
        let base_bits = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.base_sp_o_adjacency_list_bits,
        )
        .await?;
        let base_nums = PersistentLayerStore::get_file(
            from_store,
            id,
            FILENAMES.base_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(base_bits, base_nums, mapping).await?;
        write_bytes_to_file(
            to_store,
            id,
            FILENAMES.base_sp_o_adjacency_list_nums,
            output_nums,
        );
    }

    Ok(())
}

async fn copy_unchanged_files(
    from: &ArchiveLayerStore,
    to: &ArchiveLayerStore,
    id: [u32; 5],
) -> Result<(), LayerConversionError> {
    for filename in UNCHANGED_FILES.iter() {
        copy_file(from, to, id, filename).await?;
    }

    Ok(())
}

async fn copy_indexes(
    from: &ArchiveLayerStore,
    to: &ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
) -> Result<(), LayerConversionError> {
    let iter = if is_child {
        CHILD_INDEX_FILES.iter()
    } else {
        BASE_INDEX_FILES.iter()
    };
    for filename in iter {
        copy_file(from, to, id, filename).await?;
    }

    Ok(())
}

async fn rebuild_indexes(
    store: &ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
) -> io::Result<()> {
    let pos_objects_file = if is_child {
        Some(
            PersistentLayerStore::get_file(store, id, FILENAMES.pos_objects)
                .await?,
        )
    } else {
        None
    };

    let pos_sp_o_nums = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_sp_o_adjacency_list_nums,
    )
    .await?;
    let pos_sp_o_bits = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_sp_o_adjacency_list_bits,
    )
    .await?;
    let pos_sp_o_bit_index_blocks = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
    )
    .await?;
    let pos_sp_o_bit_index_sblocks = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
    )
    .await?;

    let pos_sp_o_files = AdjacencyListFiles {
        bitindex_files: BitIndexFiles {
            bits_file: pos_sp_o_bits,
            blocks_file: pos_sp_o_bit_index_blocks,
            sblocks_file: pos_sp_o_bit_index_sblocks,
        },
        nums_file: pos_sp_o_nums,
    };

    let pos_o_ps_nums = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_o_ps_adjacency_list_nums,
    )
    .await?;
    let pos_o_ps_bits = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_o_ps_adjacency_list_bits,
    )
    .await?;
    let pos_o_ps_bit_index_blocks = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_o_ps_adjacency_list_bit_index_blocks,
    )
    .await?;
    let pos_o_ps_bit_index_sblocks = PersistentLayerStore::get_file(
        store,
        id,
        FILENAMES.pos_o_ps_adjacency_list_bit_index_sblocks,
    )
    .await?;

    let pos_o_ps_files = AdjacencyListFiles {
        bitindex_files: BitIndexFiles {
            bits_file: pos_o_ps_bits,
            blocks_file: pos_o_ps_bit_index_blocks,
            sblocks_file: pos_o_ps_bit_index_sblocks,
        },
        nums_file: pos_o_ps_nums,
    };

    build_object_index(pos_sp_o_files, pos_o_ps_files, pos_objects_file).await?;

    if is_child {
        let neg_objects_file = Some(
            PersistentLayerStore::get_file(store, id, FILENAMES.neg_objects)
                .await?,
        );

        let neg_sp_o_nums = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_nums,
        )
        .await?;
        let neg_sp_o_bits = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_bits,
        )
        .await?;
        let neg_sp_o_bit_index_blocks = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
        )
        .await?;
        let neg_sp_o_bit_index_sblocks = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
        )
        .await?;

        let neg_sp_o_files = AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: neg_sp_o_bits,
                blocks_file: neg_sp_o_bit_index_blocks,
                sblocks_file: neg_sp_o_bit_index_sblocks,
            },
            nums_file: neg_sp_o_nums,
        };

        let neg_o_ps_nums = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_o_ps_adjacency_list_nums,
        )
        .await?;
        let neg_o_ps_bits = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_o_ps_adjacency_list_bits,
        )
        .await?;
        let neg_o_ps_bit_index_blocks = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_o_ps_adjacency_list_bit_index_blocks,
        )
        .await?;
        let neg_o_ps_bit_index_sblocks = PersistentLayerStore::get_file(
            store,
            id,
            FILENAMES.neg_o_ps_adjacency_list_bit_index_sblocks,
        )
        .await?;

        let neg_o_ps_files = AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: neg_o_ps_bits,
                blocks_file: neg_o_ps_bit_index_blocks,
                sblocks_file: neg_o_ps_bit_index_sblocks,
            },
            nums_file: neg_o_ps_nums,
        };

        build_object_index(neg_sp_o_files, neg_o_ps_files, neg_objects_file).await?;
    }

    Ok(())
}

async fn write_parent_map(
    workdir: &str,
    id: [u32; 5],
    mapping: HashMap<u64, u64>,
    offset: u64,
) -> io::Result<()> {
    let pathbuf = path_for_parent_map(workdir, id);
    tokio::fs::create_dir_all(pathbuf.parent().unwrap()).await?;

    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);

    let mut file = options.open(pathbuf).await?;

    let mut map_vec: Vec<_> = mapping.into_iter().collect();
    map_vec.sort();

    let parent_map = ParentMap {
        mapping: map_vec,
        offset,
    };

    let v = postcard::to_allocvec(&parent_map).unwrap();
    file.write_all(&v).await?;
    file.flush().await
}

fn write_bytes_to_file(
    store: &ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
    bytes: Bytes,
) {
    let file = FILENAME_ENUM_MAP[file];
    store.write_bytes(id, file, bytes);
}

async fn copy_file(
    from: &ArchiveLayerStore,
    to: &ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
) -> Result<(), LayerConversionError> {
    inner_copy_file(from, to, id, file).await.map_err(|e| {
        LayerConversionError::new(
            id,
            InnerLayerConversionError::FileCopyError {
                name: file.to_string(),
                source: e,
            },
        )
    })
}
async fn inner_copy_file(
    from: &ArchiveLayerStore,
    to: &ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
) -> io::Result<()> {
    // this assumes that the file name is the same in from and to,
    // which should be correct for everythning that is not a
    // dictionary. At this point, we've already copied over the
    // dictionaries.
    let input = PersistentLayerStore::get_file(from, id, file).await?;
    if let Some(map) = FileLoad::map_if_exists(&input).await? {
        write_bytes_to_file(to, id, file, map);
    }

    Ok(())
}

async fn assert_no_remap_exists(
    store: &ArchiveLayerStore,
    id: [u32; 5],
) -> Result<(), InnerLayerConversionError> {
    if PersistentLayerStore::file_exists(
        store,
        id,
        FILENAMES.node_value_idmap_bit_index_blocks,
    )
    .await?
    {
        Err(InnerLayerConversionError::NodeValueRemapExists)
    } else {
        Ok(())
    }
}
