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

use actix_web::{http::header::ContentType, web, HttpResponse, Responder};
use http::StatusCode;
use utoipa::ToSchema;

use crate::{
    option::CONFIG,
    rbac::{
        map::{mut_roles, DEFAULT_ROLE},
        role::model::DefaultPrivilege,
    },
    storage::{self, ObjectStorageError, StorageMetadata},
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
#[utoipa::path(
    put,
    tag = "role",
    context_path = "/api/v1/role",
    path = "/{name}",
    params(
        ("name" = String, Path, description = "Name of the role to create or update")
    ),
    request_body(
        content = Vec<Object>, description = "Privilege and resource stream"
    ),
    responses(
        (status = 200, description = "Created/updated the role"),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put(
    name: web::Path<String>,
    body: web::Json<Vec<DefaultPrivilege>>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = body.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());
    put_metadata(&metadata).await?;
    mut_roles().insert(name, privileges);
    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/{name}
// Fetch role by name
#[utoipa::path(
    get,
    tag = "role",
    context_path = "/api/v1/role",
    path = "/{name}",
    params(
        ("name" = String, Path, description = "Name of the role to fetch")
    ),
    responses(
        (status = 200, description = "Fetched role"),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let metadata = get_metadata().await?;
    let privileges = metadata.roles.get(&name).cloned().unwrap_or_default();
    Ok(web::Json(privileges))
}

// Handler for GET /api/v1/role
// Fetch all roles in the system
#[utoipa::path(
    get,
    tag = "role",
    context_path = "/api/v1",
    path = "/role",
    responses(
        (status = 200, description = "Fetches all roles in the system", body = Object<Vec<String>>),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn list() -> Result<impl Responder, RoleError> {
    let metadata = get_metadata().await?;
    let roles: Vec<String> = metadata.roles.keys().cloned().collect();
    Ok(web::Json(roles))
}

// Handler for DELETE /api/v1/role/{username}
// Delete existing role
#[utoipa::path(
    delete,
    tag = "role",
    context_path = "/api/v1/role",
    path = "/{name}",
    params(
        ("name" = String, Path, description = "Deletes the given role")
    ),
    responses(
        (status = 200, description = "Deleted given role"),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn delete(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    if metadata.users.iter().any(|user| user.roles.contains(&name)) {
        return Err(RoleError::RoleInUse);
    }
    metadata.roles.remove(&name);
    put_metadata(&metadata).await?;
    mut_roles().remove(&name);
    Ok(HttpResponse::Ok().finish())
}

// Handler for PUT /api/v1/role/default
// Put default role
#[utoipa::path(
    put,
    tag = "role",
    context_path = "/api/v1/role",
    path = "/default",
    request_body(content = String, description = "Name of the role", content_type = "application/json"),
    responses(
        (status = 200, description = "Created a default role with the given name"),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn put_default(name: web::Json<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.default_role = Some(name.clone());
    *DEFAULT_ROLE.lock().unwrap() = Some(name);
    put_metadata(&metadata).await?;
    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/default
// get the default role
#[utoipa::path(
    get,
    tag = "role",
    context_path = "/api/v1/role",
    path = "/default",
    responses(
        (status = 200, description = "Fetched the default role", body = Object<String>),
        (status = 500, description = "Failed to connect to storage: 0"),
        (status = 400, description = "Cannot perform this operation as role is assigned to an existing user.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn get_default() -> Result<impl Responder, RoleError> {
    let res = match DEFAULT_ROLE.lock().unwrap().clone() {
        Some(role) => serde_json::Value::String(role),
        None => serde_json::Value::Null,
    };

    Ok(web::Json(res))
}

async fn get_metadata() -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = CONFIG
        .storage()
        .get_object_store()
        .get_metadata()
        .await?
        .expect("metadata is initialized");
    Ok(metadata)
}

async fn put_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata).await?;
    storage::put_staging_metadata(metadata)?;
    Ok(())
}

#[derive(Debug, thiserror::Error, ToSchema)]
pub enum RoleError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Cannot perform this operation as role is assigned to an existing user.")]
    RoleInUse,
}

impl actix_web::ResponseError for RoleError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RoleInUse => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
