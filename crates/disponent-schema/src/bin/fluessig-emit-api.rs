//! Print disponent's `api.json` op surface (the derive front end's replacement for
//! the TypeSpec emitter's api layer). `scripts/gen.sh` writes stdout to
//! `schema/api.json`, then hands it to `fluessig-gen`.

fn main() {
    print!("{}", disponent_schema::fluessig_catalog::api_to_json());
}
