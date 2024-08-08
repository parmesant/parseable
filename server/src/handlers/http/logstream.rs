/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use self::error::{CreateStreamError, StreamError};
use super::base_path_without_preceding_slash;
use super::cluster::utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats};
use super::cluster::{
    fetch_daily_stats_from_ingestors, fetch_stats_from_ingestors, sync_streams_with_ingestors,
    INTERNAL_STREAM_NAME,
};
use super::ingest::create_stream_if_not_exists;
use crate::alerts::Alerts;
use crate::handlers::{
    CUSTOM_PARTITION_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY, TIME_PARTITION_KEY,
    TIME_PARTITION_LIMIT_KEY, UPDATE_STREAM_KEY,
};
use crate::hottier::{HotTierManager, StreamHotTier};
use crate::metadata::STREAM_INFO;
use crate::metrics::{EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE};
use crate::option::validation::bytes_to_human_size;
use crate::option::{Mode, CONFIG};
use crate::static_schema::{convert_static_schema_to_arrow_schema, StaticSchema};
use crate::stats::{event_labels_date, storage_size_labels_date, Stats};
use crate::storage::StreamType;
use crate::storage::{retention::Retention, LogStream, StorageDir, StreamInfo};
use crate::{
    catalog::{self, remove_manifest_from_snapshot},
    event, stats,
};

use crate::{metadata, validator};
use actix_web::http::header::{self, HeaderMap};
use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, Responder};
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderName, HeaderValue};
use itertools::Itertools;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;

#[utoipa::path(
    delete,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Deleted stream", body = Vec<String>),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }
    match CONFIG.parseable.mode {
        Mode::Query | Mode::All => {
            let objectstore = CONFIG.storage().get_object_store();

            objectstore.delete_stream(&stream_name).await?;
            let stream_dir = StorageDir::new(&stream_name);
            if fs::remove_dir_all(&stream_dir.data_path).is_err() {
                log::warn!(
                    "failed to delete local data for stream {}. Clean {} manually",
                    stream_name,
                    stream_dir.data_path.to_string_lossy()
                )
            }

            let ingestor_metadata = super::cluster::get_ingestor_info().await.map_err(|err| {
                log::error!("Fatal: failed to get ingestor info: {:?}", err);
                StreamError::from(err)
            })?;

            for ingestor in ingestor_metadata {
                let url = format!(
                    "{}{}/logstream/{}",
                    ingestor.domain_name,
                    base_path_without_preceding_slash(),
                    stream_name
                );

                // delete the stream
                super::cluster::send_stream_delete_request(&url, ingestor.clone()).await?;
            }
        }
        _ => {}
    }

    metadata::STREAM_INFO.delete_stream(&stream_name);
    event::STREAM_WRITERS.delete_stream(&stream_name);
    stats::delete_stats(&stream_name, "json").unwrap_or_else(|e| {
        log::warn!("failed to delete stats for stream {}: {:?}", stream_name, e)
    });

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn retention_cleanup(
    req: HttpRequest,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        log::error!("Stream {} not found", stream_name.clone());
        return Err(StreamError::StreamNotFound(stream_name.clone()));
    }
    let date_list: Vec<String> = serde_json::from_slice(&body).unwrap();
    let res = remove_manifest_from_snapshot(storage.clone(), &stream_name, date_list).await;
    let mut first_event_at: Option<String> = None;
    if let Err(err) = res {
        log::error!("Failed to update manifest list in the snapshot {err:?}")
    } else {
        first_event_at = res.unwrap();
    }

    Ok((first_event_at, StatusCode::OK))
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream",
    responses(
        (status = 200, description = "Fetched all streams in the system", body = Vec<String>),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn list(_: HttpRequest) -> impl Responder {
    let res: Vec<LogStream> = STREAM_INFO
        .list_streams()
        .into_iter()
        .map(|stream| LogStream { name: stream })
        .collect();

    web::Json(res)
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/schema",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Fetched schema for stream", body = Object),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn schema(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let schema = STREAM_INFO.schema(&stream_name)?;
    Ok((web::Json(schema), StatusCode::OK))
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/alert",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Fetched alert for stream", body = Object),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_alert(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let alerts = metadata::STREAM_INFO
        .read()
        .expect(metadata::LOCK_EXPECT)
        .get(&stream_name)
        .map(|metadata| {
            serde_json::to_value(&metadata.alerts).expect("alerts can serialize to valid json")
        });

    let mut alerts = match alerts {
        Some(alerts) => alerts,
        None => {
            let alerts = CONFIG
                .storage()
                .get_object_store()
                .get_alerts(&stream_name)
                .await?;

            if alerts.alerts.is_empty() {
                return Err(StreamError::NoAlertsSet);
            }

            serde_json::to_value(alerts).expect("alerts can serialize to valid json")
        }
    };

    remove_id_from_alerts(&mut alerts);

    Ok((web::Json(alerts), StatusCode::OK))
}

#[utoipa::path(
    put,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Created new stream", body = Vec<String>),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if CONFIG.parseable.mode == Mode::Query {
        let headers = create_update_stream(&req, &body, &stream_name).await?;
        sync_streams_with_ingestors(headers, body, &stream_name).await?;
    } else {
        create_update_stream(&req, &body, &stream_name).await?;
    }

    Ok(("Log stream created", StatusCode::OK))
}

fn fetch_headers_from_put_stream_request(
    req: &HttpRequest,
) -> (String, String, String, String, String, String) {
    let mut time_partition = String::default();
    let mut time_partition_limit = String::default();
    let mut custom_partition = String::default();
    let mut static_schema_flag = String::default();
    let mut update_stream = String::default();
    let mut stream_type = StreamType::UserDefined.to_string();
    req.headers().iter().for_each(|(key, value)| {
        if key == TIME_PARTITION_KEY {
            time_partition = value.to_str().unwrap().to_string();
        }
        if key == TIME_PARTITION_LIMIT_KEY {
            time_partition_limit = value.to_str().unwrap().to_string();
        }
        if key == CUSTOM_PARTITION_KEY {
            custom_partition = value.to_str().unwrap().to_string();
        }
        if key == STATIC_SCHEMA_FLAG {
            static_schema_flag = value.to_str().unwrap().to_string();
        }
        if key == UPDATE_STREAM_KEY {
            update_stream = value.to_str().unwrap().to_string();
        }
        if key == STREAM_TYPE_KEY {
            stream_type = value.to_str().unwrap().to_string();
        }
    });

    (
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        update_stream,
        stream_type,
    )
}

fn validate_time_partition_limit(time_partition_limit: &str) -> Result<&str, CreateStreamError> {
    if !time_partition_limit.ends_with('d') {
        return Err(CreateStreamError::Custom {
            msg: "Missing 'd' suffix for duration value".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    let days = &time_partition_limit[0..time_partition_limit.len() - 1];
    if days.parse::<NonZeroU32>().is_err() {
        return Err(CreateStreamError::Custom {
            msg: "Could not convert duration to an unsigned number".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    Ok(days)
}

fn validate_custom_partition(custom_partition: &str) -> Result<(), CreateStreamError> {
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
    if custom_partition_list.len() > 3 {
        return Err(CreateStreamError::Custom {
            msg: "Maximum 3 custom partition keys are supported".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    Ok(())
}

fn validate_time_with_custom_partition(
    time_partition: &str,
    custom_partition: &str,
) -> Result<(), CreateStreamError> {
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
    if custom_partition_list.contains(&time_partition) {
        return Err(CreateStreamError::Custom {
            msg: format!(
                "time partition {} cannot be set as custom partition",
                time_partition
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }
    Ok(())
}

fn validate_static_schema(
    body: &Bytes,
    stream_name: &str,
    time_partition: &str,
    custom_partition: &str,
    static_schema_flag: &str,
) -> Result<Arc<Schema>, CreateStreamError> {
    if static_schema_flag == "true" {
        if body.is_empty() {
            return Err(CreateStreamError::Custom {
                msg: format!(
                    "Please provide schema in the request body for static schema logstream {stream_name}"
                ),
                status: StatusCode::BAD_REQUEST,
            });
        }

        let static_schema: StaticSchema = serde_json::from_slice(body)?;
        let parsed_schema =
            convert_static_schema_to_arrow_schema(static_schema, time_partition, custom_partition)
                .map_err(|_| CreateStreamError::Custom {
                    msg: format!(
                        "Unable to commit static schema, logstream {stream_name} not created"
                    ),
                    status: StatusCode::BAD_REQUEST,
                })?;

        return Ok(parsed_schema);
    }

    Ok(Arc::new(Schema::empty()))
}

async fn create_update_stream(
    req: &HttpRequest,
    body: &Bytes,
    stream_name: &str,
) -> Result<HeaderMap, StreamError> {
    let (
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        update_stream,
        stream_type,
    ) = fetch_headers_from_put_stream_request(req);

    if metadata::STREAM_INFO.stream_exists(stream_name) && update_stream != "true" {
        return Err(StreamError::Custom {
            msg: format!(
                "Logstream {stream_name} already exists, please create a new log stream with unique name"
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }

    if update_stream == "true" {
        if !STREAM_INFO.stream_exists(stream_name) {
            return Err(StreamError::StreamNotFound(stream_name.to_string()));
        }
        if !time_partition.is_empty() {
            return Err(StreamError::Custom {
                msg: "Altering the time partition of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !static_schema_flag.is_empty() {
            return Err(StreamError::Custom {
                msg: "Altering the schema of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !time_partition_limit.is_empty() {
            let time_partition_days = validate_time_partition_limit(&time_partition_limit)?;
            update_time_partition_limit_in_stream(stream_name.to_string(), time_partition_days)
                .await?;
            return Ok(req.headers().clone());
        }

        if !custom_partition.is_empty() {
            validate_custom_partition(&custom_partition)?;
            update_custom_partition_in_stream(stream_name.to_string(), &custom_partition).await?;
        } else {
            update_custom_partition_in_stream(stream_name.to_string(), "").await?;
        }
        return Ok(req.headers().clone());
    }
    let mut time_partition_in_days = "";
    if !time_partition_limit.is_empty() {
        time_partition_in_days = validate_time_partition_limit(&time_partition_limit)?;
    }
    if !custom_partition.is_empty() {
        validate_custom_partition(&custom_partition)?;
    }

    if !time_partition.is_empty() && !custom_partition.is_empty() {
        validate_time_with_custom_partition(&time_partition, &custom_partition)?;
    }

    let schema = validate_static_schema(
        body,
        stream_name,
        &time_partition,
        &custom_partition,
        &static_schema_flag,
    )?;

    create_stream(
        stream_name.to_string(),
        &time_partition,
        time_partition_in_days,
        &custom_partition,
        &static_schema_flag,
        schema,
        &stream_type,
    )
    .await?;

    Ok(req.headers().clone())
}

#[utoipa::path(
    put,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/alert",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    request_body(
        content =  Alerts, description = "Alert to be set"
    ),
    responses(
        (status = 200, description = "Put alert for stream", body = Vec<String>),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Log stream not initialized", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_alert(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let mut body = body.into_inner();
    remove_id_from_alerts(&mut body);

    let alerts: Alerts = match serde_json::from_value(body) {
        Ok(alerts) => alerts,
        Err(err) => {
            return Err(StreamError::BadAlertJson {
                stream: stream_name,
                err,
            })
        }
    };

    validator::alert(&alerts)?;

    if !STREAM_INFO.stream_initialized(&stream_name)? {
        return Err(StreamError::UninitializedLogstream);
    }

    let schema = STREAM_INFO.schema(&stream_name)?;
    for alert in &alerts.alerts {
        for column in alert.message.extract_column_names() {
            let is_valid = alert.message.valid(&schema, column);
            if !is_valid {
                return Err(StreamError::InvalidAlertMessage(
                    alert.name.to_owned(),
                    column.to_string(),
                ));
            }
            if !alert.rule.valid_for_schema(&schema) {
                return Err(StreamError::InvalidAlert(alert.name.to_owned()));
            }
        }
    }

    CONFIG
        .storage()
        .get_object_store()
        .put_alerts(&stream_name, &alerts)
        .await?;

    metadata::STREAM_INFO
        .set_alert(&stream_name, alerts)
        .expect("alerts set on existing stream");

    Ok((
        format!("set alert configuration for log stream {stream_name}"),
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/retention",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Fetched retention for stream", body = Retention),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_retention(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name.to_string()));
    }
    let retention = STREAM_INFO.get_retention(&stream_name);

    match retention {
        Ok(retention) => {
            if let Some(retention) = retention {
                Ok((web::Json(retention), StatusCode::OK))
            } else {
                Ok((web::Json(Retention::default()), StatusCode::OK))
            }
        }
        Err(err) => Err(StreamError::from(err)),
    }
}

#[utoipa::path(
    put,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/retention",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    request_body(
        content = Retention, description = "Retention details"
    ),
    responses(
        (status = 200, description = "Put retention for stream", body = Retention),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_retention(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let body = body.into_inner();

    let retention: Retention = match serde_json::from_value(body) {
        Ok(retention) => retention,
        Err(err) => return Err(StreamError::InvalidRetentionConfig(err)),
    };

    CONFIG
        .storage()
        .get_object_store()
        .put_retention(&stream_name, &retention)
        .await?;

    metadata::STREAM_INFO
        .set_retention(&stream_name, retention)
        .expect("retention set on existing stream");

    Ok((
        format!("set retention configuration for log stream {stream_name}"),
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/cache",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, body = bool),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_cache_enabled(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    match CONFIG.parseable.mode {
        Mode::Ingest | Mode::All => {
            if CONFIG.parseable.local_cache_path.is_none() {
                return Err(StreamError::CacheNotEnabled(stream_name));
            }
        }
        _ => {}
    }

    let cache_enabled = STREAM_INFO.get_cache_enabled(&stream_name)?;
    Ok((web::Json(cache_enabled), StatusCode::OK))
}

#[utoipa::path(
    put,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/cache",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Enabled cache for stream"),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_enable_cache(
    req: HttpRequest,
    body: web::Json<bool>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();

    match CONFIG.parseable.mode {
        Mode::Query => {
            if !metadata::STREAM_INFO.stream_exists(&stream_name) {
                return Err(StreamError::StreamNotFound(stream_name));
            }
            let ingestor_metadata = super::cluster::get_ingestor_info().await.map_err(|err| {
                log::error!("Fatal: failed to get ingestor info: {:?}", err);
                StreamError::from(err)
            })?;
            for ingestor in ingestor_metadata {
                let url = format!(
                    "{}{}/logstream/{}/cache",
                    ingestor.domain_name,
                    base_path_without_preceding_slash(),
                    stream_name
                );

                super::cluster::sync_cache_with_ingestors(&url, ingestor.clone(), *body).await?;
            }
        }
        Mode::Ingest => {
            if CONFIG.parseable.local_cache_path.is_none() {
                return Err(StreamError::CacheNotEnabled(stream_name));
            }
            // here the ingest server has not found the stream
            // so it should check if the stream exists in storage
            let check = storage
                .list_streams()
                .await?
                .iter()
                .map(|stream| stream.name.clone())
                .contains(&stream_name);

            if !check {
                log::error!("Stream {} not found", stream_name.clone());
                return Err(StreamError::StreamNotFound(stream_name.clone()));
            }
            metadata::STREAM_INFO
                .upsert_stream_info(
                    &*storage,
                    LogStream {
                        name: stream_name.clone().to_owned(),
                    },
                )
                .await
                .map_err(|_| StreamError::StreamNotFound(stream_name.clone()))?;
        }
        Mode::All => {
            if !metadata::STREAM_INFO.stream_exists(&stream_name) {
                return Err(StreamError::StreamNotFound(stream_name));
            }
            if CONFIG.parseable.local_cache_path.is_none() {
                return Err(StreamError::CacheNotEnabled(stream_name));
            }
        }
    }
    let enable_cache = body.into_inner();
    let mut stream_metadata = storage.get_object_store_format(&stream_name).await?;
    stream_metadata.cache_enabled = enable_cache;
    storage
        .put_stream_manifest(&stream_name, &stream_metadata)
        .await?;

    STREAM_INFO.set_cache_enabled(&stream_name, enable_cache)?;
    Ok((
        format!("Cache set to {enable_cache} for log stream {stream_name}"),
        StatusCode::OK,
    ))
}
pub async fn get_stats_date(stream_name: &str, date: &str) -> Result<Stats, StreamError> {
    let event_labels = event_labels_date(stream_name, "json", date);
    let storage_size_labels = storage_size_labels_date(stream_name, date);
    let events_ingested = EVENTS_INGESTED_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let ingestion_size = EVENTS_INGESTED_SIZE_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let storage_size = EVENTS_STORAGE_SIZE_DATE
        .get_metric_with_label_values(&storage_size_labels)
        .unwrap()
        .get() as u64;

    let stats = Stats {
        events: events_ingested,
        ingestion: ingestion_size,
        storage: storage_size,
    };
    Ok(stats)
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/stats",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Fetched stats for stream", body = QueriedStats),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    let query_string = req.query_string();
    if !query_string.is_empty() {
        let date_key = query_string.split('=').collect::<Vec<&str>>()[0];
        let date_value = query_string.split('=').collect::<Vec<&str>>()[1];
        if date_key != "date" {
            return Err(StreamError::Custom {
                msg: "Invalid query parameter".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !date_value.is_empty() {
            if CONFIG.parseable.mode == Mode::Query {
                let querier_stats = get_stats_date(&stream_name, date_value).await?;
                let ingestor_stats =
                    fetch_daily_stats_from_ingestors(&stream_name, date_value).await?;
                let total_stats = Stats {
                    events: querier_stats.events + ingestor_stats.events,
                    ingestion: querier_stats.ingestion + ingestor_stats.ingestion,
                    storage: querier_stats.storage + ingestor_stats.storage,
                };
                let stats = serde_json::to_value(total_stats)?;

                return Ok((web::Json(stats), StatusCode::OK));
            } else {
                let stats = get_stats_date(&stream_name, date_value).await?;
                let stats = serde_json::to_value(stats)?;

                return Ok((web::Json(stats), StatusCode::OK));
            }
        }
    }

    let stats = stats::get_current_stats(&stream_name, "json")
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let ingestor_stats = if CONFIG.parseable.mode == Mode::Query
        && STREAM_INFO.stream_type(&stream_name).unwrap() == StreamType::UserDefined.to_string()
    {
        Some(fetch_stats_from_ingestors(&stream_name).await?)
    } else {
        None
    };

    let hash_map = STREAM_INFO.read().expect("Readable");
    let stream_meta = &hash_map
        .get(&stream_name)
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let time = Utc::now();

    let stats = match &stream_meta.first_event_at {
        Some(_) => {
            let ingestion_stats = IngestionStats::new(
                stats.current_stats.events,
                format!("{} {}", stats.current_stats.ingestion, "Bytes"),
                stats.lifetime_stats.events,
                format!("{} {}", stats.lifetime_stats.ingestion, "Bytes"),
                stats.deleted_stats.events,
                format!("{} {}", stats.deleted_stats.ingestion, "Bytes"),
                "json",
            );
            let storage_stats = StorageStats::new(
                format!("{} {}", stats.current_stats.storage, "Bytes"),
                format!("{} {}", stats.lifetime_stats.storage, "Bytes"),
                format!("{} {}", stats.deleted_stats.storage, "Bytes"),
                "parquet",
            );

            QueriedStats::new(&stream_name, time, ingestion_stats, storage_stats)
        }

        None => {
            let ingestion_stats = IngestionStats::new(
                stats.current_stats.events,
                format!("{} {}", stats.current_stats.ingestion, "Bytes"),
                stats.lifetime_stats.events,
                format!("{} {}", stats.lifetime_stats.ingestion, "Bytes"),
                stats.deleted_stats.events,
                format!("{} {}", stats.deleted_stats.ingestion, "Bytes"),
                "json",
            );
            let storage_stats = StorageStats::new(
                format!("{} {}", stats.current_stats.storage, "Bytes"),
                format!("{} {}", stats.lifetime_stats.storage, "Bytes"),
                format!("{} {}", stats.deleted_stats.storage, "Bytes"),
                "parquet",
            );

            QueriedStats::new(&stream_name, time, ingestion_stats, storage_stats)
        }
    };
    let stats = if let Some(mut ingestor_stats) = ingestor_stats {
        ingestor_stats.push(stats);
        merge_quried_stats(ingestor_stats)
    } else {
        stats
    };

    let stats = serde_json::to_value(stats)?;

    Ok((web::Json(stats), StatusCode::OK))
}

// Check if the first_event_at is empty
#[allow(dead_code)]
pub fn first_event_at_empty(stream_name: &str) -> bool {
    let hash_map = STREAM_INFO.read().unwrap();
    if let Some(stream_info) = hash_map.get(stream_name) {
        if let Some(first_event_at) = &stream_info.first_event_at {
            return first_event_at.is_empty();
        }
    }
    true
}

fn remove_id_from_alerts(value: &mut Value) {
    if let Some(Value::Array(alerts)) = value.get_mut("alerts") {
        alerts
            .iter_mut()
            .map_while(|alert| alert.as_object_mut())
            .for_each(|map| {
                map.remove("id");
            });
    }
}

pub async fn update_time_partition_limit_in_stream(
    stream_name: String,
    time_partition_limit: &str,
) -> Result<(), CreateStreamError> {
    let storage = CONFIG.storage().get_object_store();
    if let Err(err) = storage
        .update_time_partition_limit_in_stream(&stream_name, time_partition_limit)
        .await
    {
        return Err(CreateStreamError::Storage { stream_name, err });
    }

    if metadata::STREAM_INFO
        .update_time_partition_limit(&stream_name, time_partition_limit.to_string())
        .is_err()
    {
        return Err(CreateStreamError::Custom {
            msg: "failed to update time partition limit in metadata".to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        });
    }

    Ok(())
}

pub async fn update_custom_partition_in_stream(
    stream_name: String,
    custom_partition: &str,
) -> Result<(), CreateStreamError> {
    let static_schema_flag = STREAM_INFO.get_static_schema_flag(&stream_name).unwrap();
    let time_partition = STREAM_INFO.get_time_partition(&stream_name).unwrap();
    if static_schema_flag.is_some() {
        let schema = STREAM_INFO.schema(&stream_name).unwrap();

        if !custom_partition.is_empty() {
            let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
            let custom_partition_exists: HashMap<_, _> = custom_partition_list
                .iter()
                .map(|&partition| {
                    (
                        partition.to_string(),
                        schema
                            .fields()
                            .iter()
                            .any(|field| field.name() == partition),
                    )
                })
                .collect();

            for partition in &custom_partition_list {
                if !custom_partition_exists[*partition] {
                    return Err(CreateStreamError::Custom {
                        msg: format!("custom partition field {} does not exist in the schema for the stream {}", partition, stream_name),
                        status: StatusCode::BAD_REQUEST,
                    });
                }

                if let Some(time_partition) = time_partition.clone() {
                    if time_partition == *partition {
                        return Err(CreateStreamError::Custom {
                            msg: format!(
                                "time partition {} cannot be set as custom partition",
                                partition
                            ),
                            status: StatusCode::BAD_REQUEST,
                        });
                    }
                }
            }
        }
    }

    let storage = CONFIG.storage().get_object_store();
    if let Err(err) = storage
        .update_custom_partition_in_stream(&stream_name, custom_partition)
        .await
    {
        return Err(CreateStreamError::Storage { stream_name, err });
    }

    if metadata::STREAM_INFO
        .update_custom_partition(&stream_name, custom_partition.to_string())
        .is_err()
    {
        return Err(CreateStreamError::Custom {
            msg: "failed to update custom partition in metadata".to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        });
    }

    Ok(())
}

pub async fn create_stream(
    stream_name: String,
    time_partition: &str,
    time_partition_limit: &str,
    custom_partition: &str,
    static_schema_flag: &str,
    schema: Arc<Schema>,
    stream_type: &str,
) -> Result<(), CreateStreamError> {
    // fail to proceed if invalid stream name
    if stream_type != StreamType::Internal.to_string() {
        validator::stream_name(&stream_name, stream_type)?;
    }
    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();

    match storage
        .create_stream(
            &stream_name,
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            schema.clone(),
            stream_type,
        )
        .await
    {
        Ok(created_at) => {
            let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

            for (field_name, field) in schema
                .fields()
                .iter()
                .map(|field| (field.name().to_string(), field.clone()))
            {
                static_schema.insert(field_name, field);
            }

            metadata::STREAM_INFO.add_stream(
                stream_name.to_string(),
                created_at,
                time_partition.to_string(),
                time_partition_limit.to_string(),
                custom_partition.to_string(),
                static_schema_flag.to_string(),
                static_schema,
                stream_type,
            );
        }
        Err(err) => {
            return Err(CreateStreamError::Storage { stream_name, err });
        }
    }
    Ok(())
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/info",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Stream info", body = StreamInfo),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_stream_info(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    let store = CONFIG.storage().get_object_store();
    let dates: Vec<String> = Vec::new();
    if let Ok(Some(first_event_at)) = catalog::get_first_event(store, &stream_name, dates).await {
        if let Err(err) =
            metadata::STREAM_INFO.set_first_event_at(&stream_name, Some(first_event_at))
        {
            log::error!(
                "Failed to update first_event_at in streaminfo for stream {:?} {err:?}",
                stream_name
            );
        }
    }

    let hash_map = STREAM_INFO.read().unwrap();
    let stream_meta = &hash_map
        .get(&stream_name)
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let stream_info: StreamInfo = StreamInfo {
        stream_type: stream_meta.stream_type.clone(),
        created_at: stream_meta.created_at.clone(),
        first_event_at: stream_meta.first_event_at.clone(),
        time_partition: stream_meta.time_partition.clone(),
        time_partition_limit: stream_meta.time_partition_limit.clone(),
        custom_partition: stream_meta.custom_partition.clone(),
        cache_enabled: stream_meta.cache_enabled,
        static_schema_flag: stream_meta.static_schema_flag.clone(),
    };

    // get the other info from

    Ok((web::Json(stream_info), StatusCode::OK))
}

#[utoipa::path(
    put,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/hottier",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Enabled hottier for stream"),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_stream_hot_tier(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    if CONFIG.parseable.mode != Mode::Query {
        return Err(StreamError::Custom {
            msg: "Hot tier can only be enabled in query mode".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if STREAM_INFO.stream_type(&stream_name).unwrap() == StreamType::Internal.to_string() {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be updated for internal stream".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    if STREAM_INFO
        .get_time_partition(&stream_name)
        .unwrap()
        .is_some()
    {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be enabled for stream with time partition".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    let body = body.into_inner();
    let mut hottier: StreamHotTier = match serde_json::from_value(body) {
        Ok(hottier) => hottier,
        Err(err) => return Err(StreamError::InvalidHotTierConfig(err)),
    };

    validator::hot_tier(&hottier.size.to_string())?;

    STREAM_INFO.set_hot_tier(&stream_name, true)?;
    if let Some(hot_tier_manager) = HotTierManager::global() {
        let existing_hot_tier_used_size = hot_tier_manager
            .validate_hot_tier_size(&stream_name, &hottier.size)
            .await?;
        hottier.used_size = Some(bytes_to_human_size(existing_hot_tier_used_size));
        hottier.available_size = Some(hottier.size.clone());
        hot_tier_manager
            .put_hot_tier(&stream_name, &mut hottier)
            .await?;
        let storage = CONFIG.storage().get_object_store();
        let mut stream_metadata = storage.get_object_store_format(&stream_name).await?;
        stream_metadata.hot_tier_enabled = Some(true);
        storage
            .put_stream_manifest(&stream_name, &stream_metadata)
            .await?;
    }

    Ok((
        format!("hot tier set for stream {stream_name}"),
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/hottier",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Fetched hottier for stream", body = StreamHotTier),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_stream_hot_tier(req: HttpRequest) -> Result<impl Responder, StreamError> {
    if CONFIG.parseable.mode != Mode::Query {
        return Err(StreamError::Custom {
            msg: "Hot tier can only be enabled in query mode".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        let hot_tier = hot_tier_manager.get_hot_tier(&stream_name).await?;
        Ok((web::Json(hot_tier), StatusCode::OK))
    } else {
        Err(StreamError::Custom {
            msg: format!("hot tier not initialised for stream {}", stream_name),
            status: (StatusCode::BAD_REQUEST),
        })
    }
}

#[utoipa::path(
    delete,
    tag = "logstream",
    context_path = "/api/v1",
    path = "/logstream/{logstream}/hottier",
    params(
        ("logstream" = String, Path, description = "Name of stream")
    ),
    responses(
        (status = 200, description = "Deleted hottier for stream"),
        (status = 400, description = "Error", body = HttpResponse),
        (status = 500, description = "Failure", body = HttpResponse),
        (status = 404, description = "Stream not found", body = HttpResponse),
        (status = 405, description = "Method not found", body = HttpResponse),
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn delete_stream_hot_tier(req: HttpRequest) -> Result<impl Responder, StreamError> {
    if CONFIG.parseable.mode != Mode::Query {
        return Err(StreamError::Custom {
            msg: "Hot tier can only be enabled in query mode".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        hot_tier_manager.delete_hot_tier(&stream_name).await?;
    }
    Ok((
        format!("hot tier deleted for stream {stream_name}"),
        StatusCode::OK,
    ))
}

pub async fn create_internal_stream_if_not_exists() -> Result<(), StreamError> {
    if create_stream_if_not_exists(INTERNAL_STREAM_NAME, &StreamType::Internal.to_string())
        .await
        .is_ok()
    {
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str(STREAM_TYPE_KEY).unwrap(),
            HeaderValue::from_str(&StreamType::Internal.to_string()).unwrap(),
        );
        header_map.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        sync_streams_with_ingestors(header_map, Bytes::new(), INTERNAL_STREAM_NAME).await?;
    }
    Ok(())
}
#[allow(unused)]
fn classify_json_error(kind: serde_json::error::Category) -> StatusCode {
    match kind {
        serde_json::error::Category::Io => StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::error::Category::Syntax => StatusCode::BAD_REQUEST,
        serde_json::error::Category::Data => StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::error::Category::Eof => StatusCode::BAD_REQUEST,
    }
}

pub mod error {

    use actix_web::http::header::ContentType;
    use http::StatusCode;

    use crate::{
        hottier::HotTierError,
        metadata::error::stream_info::MetadataError,
        storage::ObjectStorageError,
        validator::error::{
            AlertValidationError, HotTierValidationError, StreamNameValidationError,
        },
    };

    #[allow(unused)]
    use super::classify_json_error;

    #[derive(Debug, thiserror::Error)]
    pub enum CreateStreamError {
        #[error("Stream name validation failed: {0}")]
        StreamNameValidation(#[from] StreamNameValidationError),
        #[error("failed to create log stream {stream_name} due to err: {err}")]
        Storage {
            stream_name: String,
            err: ObjectStorageError,
        },
        #[error("{msg}")]
        Custom { msg: String, status: StatusCode },
        #[error("Could not deserialize into JSON object, {0}")]
        SerdeError(#[from] serde_json::Error),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum StreamError {
        #[error("{0}")]
        CreateStream(#[from] CreateStreamError),
        #[error("Log stream {0} does not exist")]
        StreamNotFound(String),
        #[error(
            "Caching not enabled at Parseable server config. Can't enable cache for stream {0}"
        )]
        CacheNotEnabled(String),
        #[error("Log stream is not initialized, send an event to this logstream and try again")]
        UninitializedLogstream,
        #[error("Storage Error {0}")]
        Storage(#[from] ObjectStorageError),
        #[error("No alerts configured for this stream")]
        NoAlertsSet,
        #[error("failed to set alert configuration for log stream {stream} due to err: {err}")]
        BadAlertJson {
            stream: String,
            err: serde_json::Error,
        },
        #[error("Alert validation failed due to {0}")]
        AlertValidation(#[from] AlertValidationError),
        #[error("alert - \"{0}\" is invalid, please check if alert is valid according to this stream's schema and try again")]
        InvalidAlert(String),
        #[error(
            "alert - \"{0}\" is invalid, column \"{1}\" does not exist in this stream's schema"
        )]
        InvalidAlertMessage(String, String),
        #[error("failed to set retention configuration due to err: {0}")]
        InvalidRetentionConfig(serde_json::Error),
        #[error("{msg}")]
        Custom { msg: String, status: StatusCode },
        #[error("Error: {0}")]
        Anyhow(#[from] anyhow::Error),
        #[error("Network Error: {0}")]
        Network(#[from] reqwest::Error),
        #[error("Could not deserialize into JSON object, {0}")]
        SerdeError(#[from] serde_json::Error),
        #[error(
            "Hot tier is not enabled at the server config, cannot enable hot tier for stream {0}"
        )]
        HotTierNotEnabled(String),
        #[error("failed to enable hottier due to err: {0}")]
        InvalidHotTierConfig(serde_json::Error),
        #[error("Hot tier validation failed due to {0}")]
        HotTierValidation(#[from] HotTierValidationError),
        #[error("{0}")]
        HotTierError(#[from] HotTierError),
    }

    impl actix_web::ResponseError for StreamError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                StreamError::CreateStream(CreateStreamError::StreamNameValidation(_)) => {
                    StatusCode::BAD_REQUEST
                }
                StreamError::CreateStream(CreateStreamError::Storage { .. }) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                StreamError::CreateStream(CreateStreamError::Custom { .. }) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                StreamError::CreateStream(CreateStreamError::SerdeError(_)) => {
                    StatusCode::BAD_REQUEST
                }
                StreamError::CacheNotEnabled(_) => StatusCode::BAD_REQUEST,
                StreamError::StreamNotFound(_) => StatusCode::NOT_FOUND,
                StreamError::Custom { status, .. } => *status,
                StreamError::UninitializedLogstream => StatusCode::METHOD_NOT_ALLOWED,
                StreamError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::NoAlertsSet => StatusCode::NOT_FOUND,
                StreamError::BadAlertJson { .. } => StatusCode::BAD_REQUEST,
                StreamError::AlertValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidAlert(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidAlertMessage(_, _) => StatusCode::BAD_REQUEST,
                StreamError::InvalidRetentionConfig(_) => StatusCode::BAD_REQUEST,
                StreamError::SerdeError(_) => StatusCode::BAD_REQUEST,
                StreamError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::Network(err) => {
                    err.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                }
                StreamError::HotTierNotEnabled(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidHotTierConfig(_) => StatusCode::BAD_REQUEST,
                StreamError::HotTierValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::HotTierError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }

    impl From<MetadataError> for StreamError {
        fn from(value: MetadataError) -> Self {
            match value {
                MetadataError::StreamMetaNotFound(s) => StreamError::StreamNotFound(s),
                MetadataError::StandaloneWithDistributed(s) => StreamError::Custom {
                    msg: s,
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::http::logstream::error::StreamError;
    use crate::handlers::http::logstream::get_stats;
    use actix_web::test::TestRequest;
    use anyhow::bail;

    #[actix_web::test]
    #[should_panic]
    async fn get_stats_panics_without_logstream() {
        let req = TestRequest::default().to_http_request();
        let _ = get_stats(req).await;
    }

    #[actix_web::test]
    async fn get_stats_stream_not_found_error_for_unknown_logstream() -> anyhow::Result<()> {
        let req = TestRequest::default()
            .param("logstream", "test")
            .to_http_request();

        match get_stats(req).await {
            Err(StreamError::StreamNotFound(_)) => Ok(()),
            _ => bail!("expected StreamNotFound error"),
        }
    }
}
