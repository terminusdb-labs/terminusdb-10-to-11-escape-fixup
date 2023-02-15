use itertools::*;
use terminus_store::Layer;
use terminus_store::storage::{LabelStore, LayerStore, string_to_name};
use terminus_store::storage::archive::ArchiveLayerStore;
use terminus_store::storage::directory::DirectoryLabelStore;
use terminus_store::structure::TypedDictEntry;

use std::collections::{HashMap, HashSet};
use std::io;
use std::io::Write;

pub async fn find_reachable_layers(
    layer_store: &ArchiveLayerStore,
    label_store: &DirectoryLabelStore,
    verbose: bool,
) -> io::Result<HashMap<Option<[u32; 5]>, Vec<[u32; 5]>>> {
    let special_labels: HashSet<&'static str> = HashSet::from([
        "http%3a%2f%2fterminusdb.com%2fschema%2fref",
        "http%3a%2f%2fterminusdb.com%2fschema%2frepository",
        "http%3a%2f%2fterminusdb.com%2fschema%2fwoql",
        "terminusdb%3a%2f%2f%2fsystem%2fdata",
        "terminusdb%3a%2f%2f%2fsystem%2fschema",
    ]);

    if verbose {
        println!("starting label retrieval");
    }
    let labels = LabelStore::labels(label_store).await?;
    let special_layers: Vec<[u32; 5]> = labels
        .iter()
        .filter(|l| special_labels.contains(l.name.as_str()))
        .map(|l| *l.layer.as_ref().unwrap())
        .collect();
    let mut data_product_layers: Vec<[u32; 5]> = labels
        .into_iter()
        .filter(|l| !special_labels.contains(l.name.as_str()))
        .map(|l| l.layer.unwrap())
        .collect();
    if verbose {
        println!("labels retrieved");
    }
    data_product_layers.sort();
    data_product_layers.dedup();
    let mut layers = data_product_layers.clone();
    layers.extend(special_layers.clone());

    // now we need to go into the system graph (?)
    // discover all data products in use, treat their labels as metadata graphs.
    // .. (or should we actually consider all labels, minus a blacklist, to be metadata graphs?)
    //
    // The metadata graphs will tell us where all the commit graphs are.
    // we need to traverse those commit graphs to find the actual data and schema layers.
    let mut commit_layers = HashSet::new();
    for data_product in data_product_layers.iter().cloned() {
        let commit_layers_for_data_product =
            discover_layers_in_meta_graph(layer_store, data_product).await?;
        commit_layers.extend(commit_layers_for_data_product.clone());
        layers.extend(commit_layers_for_data_product);
    }

    for commit in commit_layers {
        layers.extend(discover_layers_in_meta_graph(layer_store, commit).await?);
    }

    layers.sort();
    layers.dedup();

    let mut discovered: HashSet<_> =
        HashSet::with_capacity(data_product_layers.len() + special_layers.len());
    discovered.extend(layers.clone());

    let mut final_list = Vec::with_capacity(layers.len());
    while let Some(layer) = layers.pop() {
        if verbose {
            print!(".");
            io::stdout().flush()?;
        }
        if let Some(parent) =
            LayerStore::get_layer_parent_name(layer_store, layer).await?
        {
            final_list.push((Some(parent), layer));
            if discovered.insert(parent) {
                layers.push(parent);
            }
        } else {
            final_list.push((None, layer));
        }

        /* We have not forgotten, but we just aren't doing it.
        // we musn't forget about the rollup
        if PersistentLayerStore::layer_has_rollup(layer_store, layer).await? {
            let rollup =
                PersistentLayerStore::read_rollup_file(layer_store, layer).await?;
            if discovered.insert(rollup) {
                layers.push(rollup);
            }
        }
        */
    }

    if verbose {
        println!("reachable layers retrieved");
    }
    final_list.sort();
    let group_iter = final_list
        .into_iter()
        .group_by(|(parent, _)| parent.clone());
    let final_map: HashMap<Option<[u32; 5]>, Vec<[u32; 5]>> = group_iter
        .into_iter()
        .map(|(k, g)| {
            let mut children: Vec<_> = g.map(|(_, v)| v).collect();
            children.sort();
            children.dedup();
            (k, children)
        })
        .collect();

    if verbose {
        println!("reachable layers sorted");
    }

    Ok(final_map)
}

async fn discover_layers_in_meta_graph(
    store: &ArchiveLayerStore,
    id: [u32; 5],
) -> io::Result<Vec<[u32; 5]>> {
    let meta_layer = LayerStore::get_layer(store, id)
        .await?
        .expect("layer should have existed but did not");
    let predicate_id = Layer::predicate_id(
        &*meta_layer,
        "http://terminusdb.com/schema/layer#identifier",
    );
    if predicate_id.is_none() {
        // somehow this is data product has no layers. weird, but whatever.
        return Ok(Vec::with_capacity(0));
    }
    let predicate_id = predicate_id.unwrap();
    let mut result: Vec<_> = Layer::triples_p(&*meta_layer, predicate_id)
        .filter_map(|t| Layer::id_object_value(&*meta_layer, t.object))
        .map(|v| layer_id_value_to_id(&v))
        .collect();

    result.sort();
    result.dedup();

    Ok(result)
}

const STRING_SUFFIX: &'static str = "\"^^'http://www.w3.org/2001/XMLSchema#string'";
fn layer_id_value_to_id(val: &TypedDictEntry) -> [u32; 5] {
    let id = val.as_val::<String,String>();
    string_to_name(&id).unwrap()
}
