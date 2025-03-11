use anyhow::Result;
use sqlx::PgTransaction;

use super::types::{
    BskyBlock, BskyDid, BskyFeed, BskyFollow, BskyLatestBackfill, BskyLike, BskyList,
    BskyListBlock, BskyListItem, BskyPost, BskyPostsRelation, BskyQuote, BskyRepliesRelation,
    BskyReplyToRelation, BskyRepost, JetstreamIdentityEvent, WithId,
};

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
    ($thing:expr, $field:ident.$field2:ident, notnull) => {{
        let values = $thing
            .iter()
            .flat_map(|x| x.$field.$field2.to_owned().into_iter())
            .collect::<Vec<_>>();
        let ids = $thing
            .iter()
            .flat_map(|x| {
                let id = x.id.to_string();
                x.$field
                    .$field2
                    .to_owned()
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
    if update.len() == 0 {
        return Ok(0);
    }

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

pub async fn upsert_latest_backfills(
    update: &Vec<WithId<BskyLatestBackfill>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

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
) ON CONFLICT (id) DO UPDATE SET at = EXCLUDED.at",
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
    if update.len() == 0 {
        return Ok(0);
    }

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
    if update.len() == 0 {
        return Ok(0);
    }

    let follower_did_ids = get_column!(update, data.from, record);
    let followed_did_ids = get_column!(update, data.to, record);
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO follow (
    follower_did_id,
    followed_did_id,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TIMESTAMP[]
) ON CONFLICT DO NOTHING",
        follower_did_ids.as_slice(),
        followed_did_ids.as_slice(),
        created_ats.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

#[derive(sqlx::Type, Debug)]
#[sqlx(rename_all = "lowercase", type_name = "like_target")]
enum LikeTarget {
    Post,
    Feed,
    List,
    Starterpack,
    Labeler,
}

impl From<&str> for LikeTarget {
    fn from(s: &str) -> Self {
        match s {
            "post" => LikeTarget::Post,
            "feed" => LikeTarget::Feed,
            "list" => LikeTarget::List,
            "starterpack" => LikeTarget::Starterpack,
            "labeler" => LikeTarget::Labeler,
            _ => panic!("Invalid like target"),
        }
    }
}

pub async fn insert_likes(
    update: &Vec<WithId<BskyLike>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let liker_did_ids = get_column!(update, data.from, record);
    let liked_ids = get_column!(update, data.to, record);
    let liked_types: Vec<LikeTarget> = get_column!(update, data.to, |r| r.table().into());
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query(
        r#"
INSERT INTO "like" (
    user_id,
    target_id,
    target_type,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::LIKE_TARGET[],
    $4::TIMESTAMP[]
) ON CONFLICT DO NOTHING"#,
    )
    .bind(liker_did_ids.as_slice())
    .bind(liked_ids.as_slice())
    .bind(liked_types.as_slice())
    .bind(created_ats.as_slice())
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_listblocks(
    update: &Vec<WithId<BskyListBlock>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let blocker_did_ids = get_column!(update, data.from, record);
    let target_ids = get_column!(update, data.to, record);
    let target_types: Vec<LikeTarget> = get_column!(update, data.to, |r| r.table().into());
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query(
        r#"
INSERT INTO listblock (
    blocker_did_id,
    target_id,
    target_type,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::LIKE_TARGET[],
    $4::TIMESTAMP[]
) ON CONFLICT DO NOTHING"#,
    )
    .bind(blocker_did_ids.as_slice())
    .bind(target_ids.as_slice())
    .bind(target_types.as_slice())
    .bind(created_ats.as_slice())
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_listitems(
    update: &Vec<WithId<BskyListItem>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let list_ids = get_column!(update, data.from, record);
    let did_ids = get_column!(update, data.to, record);
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO listitem (
    list_id,
    did_id,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TIMESTAMP[]
) ON CONFLICT DO NOTHING",
        list_ids.as_slice(),
        did_ids.as_slice(),
        created_ats.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_reposts(
    update: &Vec<WithId<BskyRepost>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let reposter_did_ids = get_column!(update, data.from, record);
    let reposted_ids = get_column!(update, data.to, record);
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO repost (
    did_id,
    post_id,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TIMESTAMP[]
) ON CONFLICT DO NOTHING",
        reposter_did_ids.as_slice(),
        reposted_ids.as_slice(),
        created_ats.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_blocks(
    update: &Vec<WithId<BskyBlock>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let blocker_ids = get_column!(update, data.from, record);
    let blocked_ids = get_column!(update, data.to, record);
    let created_ats = get_column!(update, data.created_at, timestamp);

    let rows_affected = sqlx::query!(
        r#"
INSERT INTO "block" (
    blocker_did_id,
    blocked_did_id,
    created_at
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TIMESTAMP[]
) ON CONFLICT DO NOTHING"#,
        blocker_ids.as_slice(),
        blocked_ids.as_slice(),
        created_ats.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_profiles(
    update: &Vec<WithId<BskyDid>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let ids = get_column!(update, id);
    let display_names = get_column!(update, data.display_name);
    let descriptions = get_column!(update, data.description);
    let avatars = get_column!(update, data.avatar, nullable_record);
    let banners = get_column!(update, data.banner, nullable_record);
    let created_ats = get_column!(update, data.created_at, nullable_timestamp);
    let seen_ats = get_column!(update, data.seen_at, timestamp);
    let joined_via_starter_packs =
        get_column!(update, data.joined_via_starter_pack, nullable_record);
    let pinned_posts = get_column!(update, data.pinned_post, nullable_record);
    let extra_datas = get_column!(update, data.extra_data);

    let (label_profile_ids, label_values) = get_columns!(update, data.labels, notnull);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO did (
    id,
    display_name,
    description,
    avatar,
    banner,
    joined_via_starter_pack,
    created_at,
    seen_at,
    pinned_post,
    extra_data
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TEXT[],
    $4::TEXT[],
    $5::TEXT[],
    $6::TEXT[],
    $7::TIMESTAMP[],
    $8::TIMESTAMP[],
    $9::TEXT[],
    $10::TEXT[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        display_names.as_slice() as _,
        descriptions.as_slice() as _,
        avatars.as_slice() as _,
        banners.as_slice() as _,
        joined_via_starter_packs.as_slice() as _,
        created_ats.as_slice() as _,
        seen_ats.as_slice(),
        pinned_posts.as_slice() as _,
        extra_datas.as_slice() as _
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    sqlx::query!(
        r"
INSERT INTO did_label (
did_id,
label
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        label_profile_ids.as_slice(),
        label_values.as_slice()
    )
    .execute(&mut **database)
    .await?;

    return Ok(rows_affected);
}

pub async fn insert_replies_relations(
    update: &Vec<WithId<BskyRepliesRelation>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let from_did_ids = get_column!(update, data.from, record);
    let to_post_ids = get_column!(update, data.to, record);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO replies_relation (
    did_id,
    post_id
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        from_did_ids.as_slice(),
        to_post_ids.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_posts_relations(
    update: &Vec<WithId<BskyPostsRelation>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let from_did_ids = get_column!(update, data.from, record);
    let to_post_ids = get_column!(update, data.to, record);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO posts_relation (
    did_id,
    post_id
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        from_did_ids.as_slice(),
        to_post_ids.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_quotes_relations(
    update: &Vec<WithId<BskyQuote>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let from_did_ids = get_column!(update, data.from, record);
    let to_post_ids = get_column!(update, data.to, record);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO quotes_relation (
    source_post_id,
    target_post_id
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        from_did_ids.as_slice(),
        to_post_ids.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_reply_to_relations(
    update: &Vec<WithId<BskyReplyToRelation>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }

    let from_did_ids = get_column!(update, data.from, record);
    let to_post_ids = get_column!(update, data.to, record);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO replyto_relation (
    source_post_id,
    target_post_id
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[]
) ON CONFLICT DO NOTHING",
        from_did_ids.as_slice(),
        to_post_ids.as_slice()
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_feeds(
    update: &Vec<WithId<BskyFeed>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }
    let ids = get_column!(update, id);
    let uris = get_column!(update, data.uri);
    let authors = get_column!(update, data.author, record);
    let rkeys = get_column!(update, data.rkey);
    let dids = get_column!(update, data.did);
    let display_names = get_column!(update, data.display_name);
    let descriptions = get_column!(update, data.description);
    let avatars = get_column!(update, data.avatar, nullable_record);
    let created_ats = get_column!(update, data.created_at, timestamp);
    let extra_datas = get_column!(update, data.extra_data);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO feed (
id,
uri,
author,
rkey,
did,
display_name,
description,
avatar,
created_at,
extra_data
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TEXT[],
    $4::TEXT[],
    $5::TEXT[],
    $6::TEXT[],
    $7::TEXT[],
    $8::TEXT[],
    $9::TIMESTAMP[],
    $10::TEXT[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        uris.as_slice(),
        authors.as_slice(),
        rkeys.as_slice(),
        dids.as_slice(),
        display_names.as_slice(),
        descriptions.as_slice() as _,
        avatars.as_slice() as _,
        created_ats.as_slice(),
        extra_datas.as_slice() as _
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

pub async fn insert_lists(
    update: &Vec<WithId<BskyList>>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    if update.len() == 0 {
        return Ok(0);
    }
    let ids = get_column!(update, id);
    let names = get_column!(update, data.name);
    let purposes = get_column!(update, data.purpose);
    let created_ats = get_column!(update, data.created_at, timestamp);
    let descriptions = get_column!(update, data.description);
    let avatars = get_column!(update, data.avatar, nullable_record);
    let extra_datas = get_column!(update, data.extra_data);

    let rows_affected = sqlx::query!(
        r"
INSERT INTO list (
id,
name,
purpose,
created_at,
description,
avatar,
extra_data
) SELECT * FROM UNNEST(
    $1::TEXT[],
    $2::TEXT[],
    $3::TEXT[],
    $4::TIMESTAMP[],
    $5::TEXT[],
    $6::TEXT[],
    $7::TEXT[]
) ON CONFLICT DO NOTHING",
        ids.as_slice(),
        names.as_slice(),
        purposes.as_slice(),
        created_ats.as_slice(),
        descriptions.as_slice() as _,
        avatars.as_slice() as _,
        extra_datas.as_slice() as _
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}

// CREATE TABLE IF NOT EXISTS jetstream_account_event (
//     id TEXT PRIMARY KEY NOT NULL,
//     time_us TIMESTAMP WITH TIME ZONE NOT NULL,
//     active BOOLEAN NOT NULL,
//     seq INT NOT NULL,
//     time TEXT NOT NULL
// );

// CREATE TABLE IF NOT EXISTS jetstream_identity_event (
//     id TEXT PRIMARY KEY NOT NULL,
//     time_us TIMESTAMP WITH TIME ZONE NOT NULL,
//     active BOOLEAN NOT NULL,
//     seq INT NOT NULL,
//     time TEXT NOT NULL
// );

pub async fn upsert_jetstream_identity_event(
    update: &WithId<JetstreamIdentityEvent>,
    database: &mut PgTransaction<'_>,
) -> Result<u64> {
    let rows_affected = sqlx::query!(
        r"
INSERT INTO jetstream_identity_event (
    id,
    time_us,
    handle,
    seq,
    time
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5
) ON CONFLICT (id) DO UPDATE SET
    time_us = EXCLUDED.time_us,
    handle = EXCLUDED.handle,
    seq = EXCLUDED.seq,
    time = EXCLUDED.time",
        update.id,
        update.data.time_us,
        update.data.handle,
        update.data.seq,
        update.data.time
    )
    .execute(&mut **database)
    .await?
    .rows_affected();

    return Ok(rows_affected);
}
