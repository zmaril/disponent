//! Print disponent's `catalog.json` (the derive front end's replacement for
//! `node emit.mjs disponent.tsp`). `scripts/gen.sh` writes stdout to
//! `schema/catalog.json`, then hands it to `fluessig-gen`.

fn main() {
    print!("{}", disponent_schema::fluessig_catalog::to_json());
}
