use anyhow::Result;
use sqlx::PgExecutor;

use super::types::{BskyDid, BskyFollow, BskyLatestBackfill, BskyPost, WithId};

fn timestamp_to_chrono(
    timestamp: surrealdb::sql::Datetime,
) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(timestamp.timestamp(), timestamp.timestamp_subsec_nanos())
        .ok_or(anyhow::anyhow!("Invalid timestamp"))
}

pub async fn insert_latest_backfills(
    update: &Vec<WithId<BskyLatestBackfill>>,
    database: impl PgExecutor<'_>,
) -> Result<u64> {
    let ids = update.iter().map(|x| x.id.clone()).collect::<Vec<_>>();
    let of_did_ids = update
        .iter()
        .map(|x| x.data.of.key().to_string())
        .collect::<Vec<_>>();
    let timestamps = update
        .iter()
        .map(|x| {
            x.data
                .at
                .clone()
                .map(|x| {
                    chrono::DateTime::from_timestamp(x.timestamp(), x.timestamp_subsec_nanos())
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();

    let rows_affected = sqlx::query!(
        r"
INSERT INTO latest_backfill (
    id,
    of,
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
    .execute(database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_latest_backfills_json(
    update: &Vec<WithId<BskyLatestBackfill>>,
    database: impl PgExecutor<'_>,
) -> Result<u64> {
    let json = serde_json::to_value(update)?;

    let rows_affected = sqlx::query!(
        r"
INSERT INTO latest_backfill (
    id,
    of,
    at
) SELECT * FROM json_to_recordset($1::json) AS e (id text, of text, at TIMESTAMP) ON CONFLICT DO NOTHING",
        json
    )
    .execute(database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_follows(
    update: &Vec<WithId<BskyFollow>>,
    database: impl PgExecutor<'_>,
) -> Result<u64> {
    let ids = update
        .iter()
        .map(|follow| follow.id.clone())
        .collect::<Vec<_>>();
    let follower_did_ids = update
        .iter()
        .map(|follow| follow.data.from.key().to_string())
        .collect::<Vec<_>>();
    let followed_did_ids = update
        .iter()
        .map(|follow| follow.data.to.key().to_string())
        .collect::<Vec<_>>();
    let created_ats = update
        .iter()
        .map(|follow| follow.data.created_at.naive_utc())
        .collect::<Vec<_>>();

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
    .execute(database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

// ...existing code...

pub async fn bulk_insert_dids(
    db: &sqlx::PgPool,
    updates: &[WithId<BskyDid>],
) -> Result<u64, sqlx::Error> {
    //     let ids = updates.iter().map(|r| r.id.clone()).collect::<Vec<_>>();
    //     let display_names = updates
    //         .iter()
    //         .map(|r| r.data.display_name.clone())
    //         .collect::<Vec<_>>();
    //     let descriptions = updates
    //         .iter()
    //         .map(|r| r.data.description.clone())
    //         .collect::<Vec<_>>();
    //     let avatars = updates
    //         .iter()
    //         .map(|r| {
    //             r.data
    //                 .avatar
    //                 .as_ref()
    //                 .map(|x| x.to_string())
    //                 .unwrap_or_default()
    //         })
    //         .collect::<Vec<_>>();
    //     let banners = updates
    //         .iter()
    //         .map(|r| {
    //             r.data
    //                 .banner
    //                 .as_ref()
    //                 .map(|x| x.to_string())
    //                 .unwrap_or_default()
    //         })
    //         .collect::<Vec<_>>();
    //     let labels = updates
    //         .iter()
    //         .map(|r| r.data.labels.clone())
    //         .collect::<Vec<Vec<_>>>();
    //     let joined_vias = updates
    //         .iter()
    //         .map(|r| {
    //             r.data
    //                 .joined_via_starter_pack
    //                 .as_ref()
    //                 .map(|x| x.to_string())
    //                 .unwrap_or_default()
    //         })
    //         .collect::<Vec<_>>();
    //     let pinned_posts = updates
    //         .iter()
    //         .map(|r| {
    //             r.data
    //                 .pinned_post
    //                 .as_ref()
    //                 .map(|x| x.to_string())
    //                 .unwrap_or_default()
    //         })
    //         .collect::<Vec<_>>();
    //     let created_ats = updates
    //         .iter()
    //         .map(|r| r.data.created_at.map(|x| x.naive_utc()).unwrap_or_default())
    //         .collect::<Vec<_>>();
    //     let seen_ats = updates
    //         .iter()
    //         .map(|r| r.data.seen_at.naive_utc())
    //         .collect::<Vec<_>>();
    //     let extra_data = updates
    //         .iter()
    //         .map(|r| r.data.extra_data.clone())
    //         .collect::<Vec<_>>();
    //     // let labelsrr = &labels.iter().map(|x| x.iter()

    //     let rows_affected = sqlx::query!(
    //         r#"
    // INSERT INTO did (
    //     id,
    //     display_name,
    //     description,
    //     avatar,
    //     banner,
    //     joined_via_starter_pack,
    //     pinned_post,
    //     created_at,
    //     seen_at,
    //     labels,
    //     extra_data
    // ) SELECT * FROM UNNEST(
    //     $1::TEXT[],
    //     $2::TEXT[],
    //     $3::TEXT[],
    //     $4::TEXT[],
    //     $5::TEXT[],
    //     $6::TEXT[],
    //     $7::TEXT[],
    //     $8::TIMESTAMP[],
    //     $9::TIMESTAMP[],
    //     $10::TEXT[][],
    //     $11::TEXT[]
    // ) ON CONFLICT DO NOTHING
    //         "#,
    //         ids.as_slice(),
    //         display_names.as_slice() as _,
    //         descriptions.as_slice() as _,
    //         avatars.as_slice(),
    //         banners.as_slice(),
    //         joined_vias.as_slice(),
    //         pinned_posts.as_slice(),
    //         created_ats.as_slice(),
    //         seen_ats.as_slice(),
    //         labels.as_slice() as &[&[String]],
    //         extra_data.as_slice() as _
    //     )
    //     .execute(db)
    //     .await?
    //     .rows_affected();

    Ok(0)
}

// ...existing code...

// pub async fn bulk_insert_posts(
//     db: &sqlx::PgPool,
//     updates: &[WithId<BskyPost>],
// ) -> Result<u64, sqlx::Error> {
//     let ids: Vec<_> = updates.iter().map(|r| r.id.clone()).collect();
//     let authors: Vec<_> = updates.iter().map(|r| r.data.author.to_string()).collect();
//     let bridgys: Vec<_> = updates
//         .iter()
//         .map(|r| r.data.bridgy_original_url.clone().unwrap_or_default())
//         .collect();
//     let created_ats: Vec<_> = updates
//         .iter()
//         .map(|r| r.data.created_at.naive_utc())
//         .collect();
//     let labels: Vec<Vec<String>> = updates
//         .iter()
//         .map(|r| r.data.labels.clone().unwrap_or_default())
//         .collect();
//     let langs: Vec<Vec<String>> = updates
//         .iter()
//         .map(|r| r.data.langs.clone().unwrap_or_default())
//         .collect();
//     let links: Vec<Vec<String>> = updates
//         .iter()
//         .map(|r| r.data.links.clone().unwrap_or_default())
//         .collect();
//     let parents: Vec<_> = updates
//         .iter()
//         .map(|r| {
//             r.data
//                 .parent
//                 .as_ref()
//                 .map(|x| x.to_string())
//                 .unwrap_or_default()
//         })
//         .collect();
//     let records: Vec<_> = updates
//         .iter()
//         .map(|r| {
//             r.data
//                 .record
//                 .as_ref()
//                 .map(|x| x.to_string())
//                 .unwrap_or_default()
//         })
//         .collect();
//     let roots: Vec<_> = updates
//         .iter()
//         .map(|r| {
//             r.data
//                 .root
//                 .as_ref()
//                 .map(|x| x.to_string())
//                 .unwrap_or_default()
//         })
//         .collect();
//     let tags: Vec<Vec<String>> = updates
//         .iter()
//         .map(|r| r.data.tags.clone().unwrap_or_default())
//         .collect();
//     let texts: Vec<_> = updates.iter().map(|r| r.data.text.clone()).collect();
//     let vias: Vec<_> = updates
//         .iter()
//         .map(|r| r.data.via.clone().unwrap_or_default())
//         .collect();
//     let videos: Vec<_> = updates
//         .iter()
//         .map(|r| serde_json::to_string(&r.data.video).unwrap_or_default())
//         .collect();
//     let extra_data: Vec<_> = updates
//         .iter()
//         .map(|r| r.data.extra_data.clone().unwrap_or_default())
//         .collect();

//     let rows_affected = sqlx::query!(
//         r#"
// INSERT INTO post (
//     id,
//     author,
//     bridgy_original_url,
//     created_at,
//     labels,
//     langs,
//     links,
//     parent,
//     record,
//     root,
//     tags,
//     text,
//     via,
//     video,
//     extra_data
// ) SELECT * FROM UNNEST(
//     $1::TEXT[],
//     $2::TEXT[],
//     $3::TEXT[],
//     $4::TIMESTAMP[],
//     $5::TEXT[][],
//     $6::TEXT[][],
//     $7::TEXT[][],
//     $8::TEXT[],
//     $9::TEXT[],
//     $10::TEXT[],
//     $11::TEXT[][],
//     $12::TEXT[],
//     $13::TEXT[],
//     $14::TEXT[],
//     $15::TEXT[]
// ) ON CONFLICT DO NOTHING
//         "#,
//         &ids,
//         &authors,
//         &bridgys,
//         &created_ats,
//         &labels,
//         &langs,
//         &links,
//         &parents,
//         &records,
//         &roots,
//         &tags,
//         &texts,
//         &vias,
//         &videos,
//         &extra_data
//     )
//     .execute(db)
//     .await?
//     .rows_affected();

//     Ok(rows_affected)
// }

// ...existing code...

// Repeat similar patterns for blob, feed, list, block, like, listitem, post_image,
// post_mention, posts_relation, replies_relation, quotes_relation, replyto_relation,
// repost, and latest_backfill as needed.
