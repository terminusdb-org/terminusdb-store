//! Throwaway: open a real store directory, and for every layer that holds
//! `xdd:dateTimeInterval` values compare the index candidates against a
//! brute-force scan for `during` against a reference interval.
//!
//! cargo run --release --example live_interval_probe -- /path/to/storage/db "2025-01-01T00:00:00Z/2025-10-01T00:00:00Z"
use std::collections::HashSet;

use tdb_succinct::tfc::lexical_interval::{
    decode_lexical_entry, interval_relation, literal_as_interval, AllenRelation,
};
use tdb_succinct::Datatype;
use terminus_store::layer::ObjectType;
use terminus_store::storage::{name_to_string, string_to_name};
use terminus_store::store::sync::*;
use terminus_store::Layer;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("store path");
    let reference = literal_as_interval(&args.next().expect("reference interval")).unwrap();
    let store = open_sync_archive_store(&path, 64);

    // walk every layer directory: <db>/<3 hex>/<29 hex>
    let mut names = Vec::new();
    for prefix in std::fs::read_dir(&path).unwrap().flatten() {
        if !prefix.file_type().unwrap().is_dir() {
            continue;
        }
        for d in std::fs::read_dir(prefix.path()).unwrap().flatten() {
            let f = d.file_name().to_string_lossy().to_string();
            if let Some(stem) = f.strip_suffix(".larch") {
                if let Ok(n) = string_to_name(stem) {
                    names.push(n);
                }
            }
        }
    }
    {
        let label = "";
        for name in names {
            let Some(layer) = store.get_layer_from_id(name).unwrap() else {
                continue;
            };
            // brute force over every triple with an interval object
            let mut truth: HashSet<u64> = HashSet::new();
            let mut n_intervals = 0usize;
            for t in layer.triples() {
                if let Some(ObjectType::Value(e)) = layer.id_object(t.object) {
                    if e.datatype() == Datatype::DateTimeInterval {
                        n_intervals += 1;
                        let iv = decode_lexical_entry(&e.to_bytes()).unwrap();
                        if interval_relation(&iv, AllenRelation::During, &reference) {
                            truth.insert(t.object);
                        }
                    }
                }
            }
            eprintln!(
                "layer {} triples={} intervals={}",
                name_to_string(name),
                layer.triple_count(),
                n_intervals
            );
            if n_intervals == 0 {
                continue;
            }
            let cands: HashSet<u64> = layer
                .interval_relation_object_ids(AllenRelation::During, &reference)
                .into_iter()
                .collect();
            let via_triples: HashSet<u64> = layer
                .triples_interval_relation(AllenRelation::During, &reference)
                .map(|t| t.object)
                .collect();
            let missing: Vec<_> = truth.difference(&cands).collect();
            println!(
                "layer {} label {} intervals={} truth={:?} cands={:?} via_triples={:?} MISSING={:?}",
                name_to_string(name),
                label,
                n_intervals,
                truth,
                cands,
                via_triples,
                missing
            );
            if !missing.is_empty() {
                for id in &truth {
                    let e = layer.id_object_value(*id).unwrap();
                    let iv = decode_lexical_entry(&e.to_bytes()).unwrap();
                    println!("  truth id {} = {:?} bytes={:02x?}", id, iv, e.to_bytes());
                }
                // where the ids sit in the layer stack
                println!("  node_and_value_count={}", layer.node_and_value_count());
            }
        }
    }
}
