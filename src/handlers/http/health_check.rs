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

use std::sync::Arc;

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    error::Error,
    error::ErrorServiceUnavailable,
    middleware::Next,
    HttpResponse,
};
use http::StatusCode;
use once_cell::sync::Lazy;
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{error, info, warn};

use crate::parseable::PARSEABLE;

// Create a global variable to store signal status
static SIGNAL_RECEIVED: Lazy<Arc<Mutex<bool>>> = Lazy::new(|| Arc::new(Mutex::new(false)));

pub async fn liveness() -> HttpResponse {
    HttpResponse::new(StatusCode::OK)
}

pub async fn check_shutdown_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    // Acquire the shutdown flag to check if the server is shutting down.
    if *SIGNAL_RECEIVED.lock().await {
        // Return 503 Service Unavailable if the server is shutting down.
        Err(ErrorServiceUnavailable("Server is shutting down"))
    } else {
        // Continue processing the request if the server is not shutting down.
        next.call(req).await
    }
}

// This function is called when the server is shutting down
pub async fn shutdown() {
    // Set the shutdown flag to true
    let mut shutdown_flag = SIGNAL_RECEIVED.lock().await;
    *shutdown_flag = true;

    let mut joinset = JoinSet::new();

    // Sync staging
    PARSEABLE.streams.flush_and_convert(&mut joinset, true);

    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(Ok(_)) => info!("Successfully converted arrow files to parquet."),
            Ok(Err(err)) => warn!("Failed to convert arrow files to parquet. {err:?}"),
            Err(err) => error!("Failed to join async task: {err}"),
        }
    }

    if let Err(e) = PARSEABLE
        .storage
        .get_object_store()
        .upload_files_from_staging()
        .await
    {
        warn!("Failed to sync local data with object store. {:?}", e);
    } else {
        info!("Successfully synced all data to S3.");
    }
}

pub async fn readiness() -> HttpResponse {
    // Check the object store connection
    if PARSEABLE.storage.get_object_store().check().await.is_ok() {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
    }
}
