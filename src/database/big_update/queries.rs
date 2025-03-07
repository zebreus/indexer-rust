use anyhow::Result;
use sqlx::PgTransaction;

use super::types::{BskyFollow, BskyLatestBackfill, BskyPost, WithId};

macro_rules! get_column {
    ($thing:expr, $field:ident) => {
        $thing.iter().map(|x| x.$field.clone()).collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident, nullable_record) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map(|x| x.map(|x| x.key().to_string()))
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident, nullable_timestamp) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map(|x| x.map(|x| x.naive_utc()))
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident, timestamp) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map(|x| x.naive_utc())
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident , record) => {
        $thing
            .iter()
            .map(|x| x.$field.clone())
            .map(|x| x.key().to_string())
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident, record) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map(|x| x.key().to_string())
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map(|x| x.key().to_string())
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident, $transform:expr) => {
        $thing
            .iter()
            .map(|x| x.$field.clone())
            .map($transform)
            .collect::<Vec<_>>()
    };
    ($thing:expr, $field:ident.$field2:ident, $transform:expr) => {
        $thing
            .iter()
            .map(|x| x.$field.$field2.clone())
            .map($transform)
            .collect::<Vec<_>>()
    };
}
macro_rules! get_columns {
    ($thing:expr, $field:ident.$field2:ident) => {{
        let values = $thing
            .iter()
            .flat_map(|x| x.$field.$field2.to_owned().unwrap_or_default().into_iter())
            .collect::<Vec<_>>();
        let ids = $thing
            .iter()
            .flat_map(|x| {
                let id = x.id.to_string();
                x.$field
                    .$field2
                    .to_owned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |_| id.clone())
            })
            .collect::<Vec<_>>();
        (ids, values)
    }};
}

// /// Insert a single value. Works but is slow
// pub async fn insert_latest_backfill(
//     update: &WithId<BskyLatestBackfill>,
//     database: impl PgExecutor<'_>,
// ) -> Result<u64> {
//     let rows_affected = sqlx::query!(
//         r"
// INSERT INTO latest_backfill (
//     id,
//     of_did_id,
//     at
// ) VALUES (
//     $1,
//     $2,
//     $3
// ) ON CONFLICT DO NOTHING",
//         update.id,
//         update.data.of.key().to_string(),
//         update.data.at
//     )
//     .execute(database)
//     .await?
//     .rows_affected();

//     return Ok(rows_affected);
// }

// /// Inserting as json allows bulk insertion of array while being faster than single INSERTs. However it is slower than normal bulk insertion.
// pub async fn insert_latest_backfills_json(
//     update: &Vec<WithId<BskyLatestBackfill>>,
//     database: impl PgExecutor<'_>,
// ) -> Result<u64> {
//     let json = serde_json::to_value(update)?;

//     let rows_affected = sqlx::query!(
//         r"
// INSERT INTO latest_backfill (
//     id,
//     of_did_id,
//     at
// ) SELECT * FROM json_to_recordset($1::json) AS e (id text, of text, at TIMESTAMP) ON CONFLICT DO NOTHING",
//         json
//     )
//     .execute(database)
//     .await?
//     .rows_affected();

//     return Ok(rows_affected);
// }

pub async fn insert_latest_backfills(
    update: &Vec<WithId<BskyLatestBackfill>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    let ids = get_column!(update, id);
    let of_did_ids = get_column!(update, data.of, record);
    let timestamps = get_column!(update, data.at, nullable_timestamp);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO latest_backfill (
    id,
    of_did_id,
    at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TIMESTAMP[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        of_did_ids.as_slice(),
        timestamps.as_slice() as _
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_posts<'a>(
    update: &Vec<WithId<BskyPost>>,
    database: &mut PgTransaction<'a>,
) -> Result<u64> {
    let ids = get_column!(update, id);
    let authors = get_column!(update, data.author, record);
    let bridgys = get_column!(update, data.bridgy_original_url);
    let created_ats = get_column!(update, data.created_at);
    let parents = get_column!(update, data.parent, nullable_record);
    let records = get_column!(update, data.record, nullable_record);
    let roots = get_column!(update, data.root, nullable_record);
    let texts = get_column!(update, data.text);
    let vias = get_column!(update, data.via);
    let videos = get_column!(update, data.video, |x| serde_json::to_value(x).unwrap());
    let extra_data = get_column!(update, data.extra_data);

    let (tag_post_ids, tag_values) = get_columns!(update, data.tags);
    let (lang_post_ids, lang_values) = get_columns!(update, data.langs);
    let (link_post_ids, link_values) = get_columns!(update, data.links);
    let (label_post_ids, label_values) = get_columns!(update, data.labels);

    let (images_post_ids, images_unprocessed) = get_columns!(update, data.images);
    let images_alt = get_column!(images_unprocessed, alt);
    let images_blobs = get_column!(images_unprocessed, blob, record);
    let images_aspectratios = get_column!(images_unprocessed, aspect_ratio);
    let images_aspectratios_widths = images_aspectratios
        .iter()
        .map(|x| x.clone().map(|x| x.width as i64))
        .collect::<Vec<_>>();
    let images_aspectratios_heights = images_aspectratios
        .iter()
        .map(|x| x.clone().map(|x| x.height as i64))
        .collect::<Vec<_>>();

    sqlx::query!(
        r"
INSERT INTO post (
id,
author,
bridgy_original_url,
created_at,
parent,
record,
root,
text,
via,
video,
extra_data
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TEXT[],
    $4::TIMESTAMP[],
    $5::TEXT[],
    $6::TEXT[],
    $7::TEXT[],
    $8::TEXT[],
    $9::TEXT[],
    $10::JSONB[],
    $11::TEXT[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        authors.as_slice(),
        bridgys.as_slice() as _,
        created_ats.as_slice() as _,
        parents.as_slice() as _,
        records.as_slice() as _,
        roots.as_slice() as _,
        texts.as_slice(),
        vias.as_slice() as _,
        videos.as_slice(),
        extra_data.as_slice() as _
    )
    .execute(&mut **database)
    .await
    .unwrap();

    sqlx::query!(
        r"
INSERT INTO post_label (
post_id,
label
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        label_post_ids.as_slice(),
        label_values.as_slice()
    )
    .execute(&mut **database)
    .await
    .unwrap();

    sqlx::query!(
        r"
INSERT INTO post_lang (
post_id,
lang
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        lang_post_ids.as_slice(),
        lang_values.as_slice()
    )
    .execute(&mut **database)
    .await
    .unwrap();

    sqlx::query!(
        r"
INSERT INTO post_link (
post_id,
link
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        link_post_ids.as_slice(),
        link_values.as_slice()
    )
    .execute(&mut **database)
    .await
    .unwrap();

    sqlx::query!(
        r"
INSERT INTO post_tag (
post_id,
tag
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        tag_post_ids.as_slice(),
        tag_values.as_slice()
    )
    .execute(&mut **database)
    .await
    .unwrap();

    sqlx::query!(
        r"
    INSERT INTO post_image (
    post_id,
    alt,
    blob_id,
    aspect_ratio_width,
    aspect_ratio_height
    ) SELECT * FROM UNNEST(
        $1::TEXT[],
        $2::TEXT[],
        $3::TEXT[],
        $4::INT[],
        $5::INT[]
    ) ON CONFLICT DO NOTHING",
        images_post_ids.as_slice(),
        images_alt.as_slice(),
        images_blobs.as_slice(),
        images_aspectratios_widths.as_slice() as _,
        images_aspectratios_heights.as_slice() as _
    )
    .execute(&mut **database)
    .await
    .unwrap();

    return Ok(0);
}

pub async fn insert_follows(
    update: &Vec<WithId<BskyFollow>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    let ids = get_column!(update, id);
    let follower_did_ids = get_column!(update, data.from, record);
    let followed_did_ids = get_column!(update, data.to, record);
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO follow (
    id,
    follower_did_id,
    followed_did_id,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TEXT[],
    $4::TIMESTAMP[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        follower_did_ids.as_slice(),
        followed_did_ids.as_slice(),
        created_ats.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}
