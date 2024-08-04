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

use actix_web::http::header::ContentType;
use actix_web::web::{self, Json};
use actix_web::{FromRequest, HttpRequest, Responder};
use chrono::DateTime;
use http::StatusCode;
use pyo3::Python;
use serde_json::{json, Value};
use tonic::{Response, Status};
use crate::panorama::{Panorama, PANORAMA_STATIC};

/// Panorama Request through http endpoint.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanoramaHttp {
    pub stream: String,
    pub start_time: String,
    pub end_time: String,
}

/// This function is the API call
/// Assume that the PanoramaHttp object contains the required parameters
pub fn detect_anomaly(
    req: HttpRequest,
    query_request: PanoramaHttp
) -> Result<impl Responder, PanoramaError> {

    Python::with_gil(|py| {
        let panorama_state = PANORAMA_STATIC.get(py).unwrap();
        panorama_state.detect_anomaly("SomeStream".to_owned(), DateTime::from_timestamp_nanos(129491), DateTime::from_timestamp_nanos(129492)).unwrap();

    });
    

    return Ok(PanoramaResponse{message: String::from("SomeMessage")}.to_http()?)
}










#[derive(Debug, thiserror::Error)]
pub enum PanoramaError {
    #[error("The provided stream name does not exist")]
    StreamDoesNotExists,
    #[error("Start time cannot be empty")]
    EmptyStartTime,
    #[error("End time cannot be empty")]
    EmptyEndTime,
    #[error("Could not parse start time correctly")]
    StartTimeParse,
    #[error("Could not parse end time correctly")]
    EndTimeParse,
    #[error("While generating times for 'now' failed to parse duration")]
    NotValidDuration(#[from] humantime::DurationError),
    #[error("Parsed duration out of range")]
    OutOfRange(#[from] chrono::OutOfRangeError),
    #[error("Start time cannot be greater than the end time")]
    StartTimeAfterEndTime,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("")]
    CacheMiss,
    #[allow(unused)]
    #[error(
        r#"Error: Failed to Parse Record Batch into Json
Description: {0}"#
    )]
    JsonParse(String),
    #[error("Error: {0}")]
    ActixError(#[from] actix_web::Error),
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl actix_web::ResponseError for PanoramaError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PanoramaError::JsonParse(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}


pub struct PanoramaResponse {
    pub message: String
}

impl PanoramaResponse {
    pub fn to_http(&self) -> Result<impl Responder, PanoramaError> {

        let response = json!({
                        "message": "SomeMessage",
                    });

        Ok(web::Json(response))
    }
}