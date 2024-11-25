use anyhow::{anyhow, Result};
use atrium_api::types::string::RecordKey;
use regex::Regex;
use surrealdb::RecordId;

pub fn extract_dt(
    dt: &Option<atrium_api::types::string::Datetime>,
) -> Result<Option<surrealdb::Datetime>> {
    match dt {
        Some(dt) => {
            let res = chrono::DateTime::parse_from_rfc3339(dt.as_str())?.to_utc();
            Ok(Some(res.into()))
        }
        None => Ok(None),
    }
}

pub fn extract_self_labels(
    labels: &Option<
        atrium_api::types::Union<atrium_api::app::bsky::actor::profile::RecordLabelsRefs>,
    >,
) -> Option<Vec<String>> {
    match labels {
        Some(l) => match l{
            atrium_api::types::Union::Refs(refs)=> match refs{
                atrium_api::app::bsky::actor::profile::RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(labels)=>
                Some(labels.values.iter().map(|l| l.val.clone()).collect())
            },
            atrium_api::types::Union::Unknown(_)=>None
        }
        None => None,
    }
}

pub fn did_to_key(did: &str) -> Result<String> {
    did_to_key_impl(did, false)
}

pub fn did_to_key_impl(did: &str, full: bool) -> Result<String> {
    let val: String;
    // did:plc covers 99.99% of all DIDs
    if did.starts_with("did:plc:") {
        val = format!("plc_{}", &did[8..]);
    } else if did.starts_with("did:web:") {
        val = format!("web_{}", &did[8..].replace('.', "_").replace('-', "__"));
    } else {
        return Err(anyhow!("Invalid DID {}", did));
    }

    if !VALID_DID_KEY_REGEX.is_match(&val) {
        return Err(anyhow!("Found invalid DID: {} {} {}", did, full, val));
    }

    if full {
        Ok(format!("did:{}", val))
    } else {
        Ok(val)
    }
}

pub fn strong_ref_to_record_id(
    sr: &Option<atrium_api::com::atproto::repo::strong_ref::Main>,
) -> Result<Option<RecordId>> {
    match sr {
        Some(sr) => {
            let res = at_uri_to_record_id(&sr.uri)?;
            Ok(Some(res))
        }
        None => Ok(None),
    }
}

lazy_static::lazy_static! {
    static ref VALID_DID_KEY_REGEX: Regex = Regex::new(r"^(plc|web)_[a-z0-9_]+$").unwrap();
}

pub fn at_uri_to_record_id(uri: &str) -> Result<RecordId> {
    let u: Vec<&str> = uri.split('/').collect();
    let u_hostname = u.get(2).unwrap().to_string();
    let u_collection = u.get(3).unwrap().to_string();
    let u_rkey = u.get(4).unwrap().to_string();

    let table: &str;
    if u_collection == "app.bsky.feed.post" {
        table = "post";
    } else if u_collection == "app.bsky.graph.list" {
        table = "list";
    } else if u_collection == "app.bsky.graph.starterpack" {
        table = "starterpack";
    } else {
        return Err(anyhow!("Unsupported URI {}", uri));
    }

    let mut did = did_to_key(&u_hostname)?;
    if did.starts_with("plc_did:plc:") {
        did = format!("plc_{}", &did[12..]);
    }
    ensure_valid_rkey(u_rkey.to_string()).unwrap();

    Ok(RecordId::from_table_key(
        table,
        format!("{}_{}", u_rkey, did),
    ))
}

pub fn ensure_valid_rkey(rkey: String) -> Result<()> {
    RecordKey::new(rkey).unwrap();
    Ok(())
}
