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
use correlation_utils::user_auth_for_query;
use http::StatusCode;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use tokio::sync::RwLock;
use tracing::{trace, warn};

use crate::{
    handlers::http::rbac::RBACError, option::CONFIG, rbac::map::SessionKey,
    storage::ObjectStorageError, utils::uid::Uid,
};

pub mod correlation_utils;
pub mod http_handlers;

pub static CORRELATIONS: Lazy<Correlation> = Lazy::new(Correlation::default);

#[derive(Debug, Default)]
pub struct Correlation(RwLock<Vec<CorrelationConfig>>);

impl Correlation {
    pub async fn load(&self) -> Result<(), CorrelationError> {
        // lead correlations from storage
        let store = CONFIG.storage().get_object_store();
        let all_correlations = store.get_correlations().await.unwrap_or_default();

        let mut correlations = vec![];
        for corr in all_correlations {
            if corr.is_empty() {
                continue;
            }

            let correlation: CorrelationConfig = serde_json::from_slice(&corr)?;

            correlations.push(correlation);
        }

        let mut s = self.0.write().await;
        s.append(&mut correlations.clone());
        Ok(())
    }

    pub async fn list_correlations_for_user(
        &self,
        session_key: &SessionKey,
    ) -> Result<Vec<CorrelationConfig>, CorrelationError> {
        let correlations = self.0.read().await.iter().cloned().collect_vec();

        let mut user_correlations = vec![];
        for c in correlations {
            if user_auth_for_query(session_key, &c.query).await.is_ok() {
                user_correlations.push(c);
            }
        }
        Ok(user_correlations)
    }

    pub async fn get_correlation_by_id(
        &self,
        correlation_id: &str,
    ) -> Result<CorrelationConfig, CorrelationError> {
        let read = self.0.read().await;
        let correlation = read
            .iter()
            .find(|c| c.id.to_string() == correlation_id)
            .cloned();

        if let Some(c) = correlation {
            Ok(c)
        } else {
            Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                "Unable to find correlation with ID- {correlation_id}"
            ))))
        }
    }

    pub async fn update(&self, correlation: &CorrelationConfig) -> Result<(), CorrelationError> {
        // save to memory
        let mut s = self.0.write().await;
        s.retain(|c| c.id != correlation.id);
        s.push(correlation.clone());
        Ok(())
    }

    pub async fn delete(&self, correlation_id: &str) -> Result<(), CorrelationError> {
        // now delete from memory
        let read_access = self.0.read().await;

        let index = read_access
            .iter()
            .enumerate()
            .find(|(_, c)| c.id.to_string() == correlation_id)
            .to_owned();

        if let Some((index, _)) = index {
            // drop the read access in order to get exclusive write access
            drop(read_access);
            self.0.write().await.remove(index);
            trace!("removed correlation from memory");
        } else {
            warn!("Correlation ID- {correlation_id} not found in memory!");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CorrelationVersion {
    V1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationConfig {
    pub version: CorrelationVersion,
    pub id: Uid,
    pub query: String,
}

impl CorrelationConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationRequest {
    pub version: CorrelationVersion,
    pub query: String,
}

impl From<CorrelationRequest> for CorrelationConfig {
    fn from(val: CorrelationRequest) -> Self {
        Self {
            version: val.version,
            id: crate::utils::uid::gen(),
            query: val.query,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
    #[error("Error: {0}")]
    AnyhowError(#[from] anyhow::Error),
    #[error("Unauthorized")]
    Unauthorized,
}

impl actix_web::ResponseError for CorrelationError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::AnyhowError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
