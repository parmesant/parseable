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

use crate::handlers::airplane;
use crate::handlers::http::cluster::utils::check_liveness;
use crate::handlers::http::cluster::{self, get_querier_info_storage, init_cluster_metrics_scheduler};
use crate::handlers::http::logstream::create_internal_stream_if_not_exists;
use crate::handlers::http::middleware::{DisAllowRootUser, RouteExt};
use crate::handlers::http::{self, role};
use crate::handlers::http::{base_path, cross_origin_config, API_BASE_PATH, API_VERSION};
use crate::handlers::http::{health_check, logstream, MAX_EVENT_PAYLOAD_SIZE};
use crate::hottier::HotTierManager;
use crate::migration::metadata_migration::migrate_querier_metadata;
use crate::rbac::role::Action;
use crate::storage::object_storage::{parseable_json_path, querier_metadata_path};
use crate::storage::{staging, ObjectStorageError};
use crate::sync;
use crate::users::dashboards::DASHBOARDS;
use crate::users::filters::FILTERS;
use crate::{analytics, banner, metrics, migration, rbac, storage};
use actix_web::body::MessageBody;
use actix_web::web::{resource, ServiceConfig};
use actix_web::{web, Resource, Scope};
use actix_web::{App, HttpServer};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use anyhow::anyhow;

use crate::option::CONFIG;

use super::query::{self, querier_ingest, querier_logstream, querier_query, querier_rbac, querier_role};
use super::server::Server;
use super::ssl_acceptor::get_ssl_acceptor;
use super::{OpenIdClient, ParseableServer, QuerierMetadata};

/// ! have to use a guard before using it
pub static QUERIER_META: Lazy<QuerierMetadata> =
    Lazy::new(|| staging::get_querier_info_staging().expect("Should Be valid Json"));

pub static QUERY_ROUTING: Lazy<Mutex<QueryRouting>> = Lazy::new(|| Mutex::new(QueryRouting::default()));

#[derive(Debug, Clone, Default)]
pub struct QueryNodeStats {
    pub ticket: String,
    pub start_time: u128,
    pub hottier_info: Option<Vec<String>>
}

#[derive(Debug, Clone, Default)]
pub struct QueryRouting {
    pub available_nodes: HashSet<String>,
    pub stats: HashMap<String, QueryNodeStats>,
    pub info: HashMap<String, QuerierMetadata>,
}

impl QueryRouting {
    /// this function will be called when a query node is made the leader
    /// for now it will start without any information about what the other nodes are doing
    pub async fn new(&mut self) {
        let querier_metas = get_querier_info_storage().await.unwrap();
        let mut available_nodes = HashSet::new();
        let mut stats: HashMap<String, QueryNodeStats> = HashMap::new();
        let mut info: HashMap<String, QuerierMetadata> = HashMap::new();
        for qm in querier_metas {
            if qm.eq(&QUERIER_META) {
                // don't append self to the list
                // using self is an edge case
                continue;
            }

            if !check_liveness(&qm.domain_name).await {
                // only append if node is live
                continue;
            }

            available_nodes.insert(qm.querier_id.clone());
            
            stats.insert(qm.querier_id.clone(), QueryNodeStats{
                start_time: qm.start_time,
                ..Default::default()
            });

            info.insert(qm.querier_id.clone(),qm);
        }
        self.available_nodes = available_nodes;
        self.info = info;
        self.stats = stats;
    }

    /// This function is supposed to look at all available query nodes
    /// in `available_nodes` and return one.
    /// If none is available, it will return one at random from `query_map`.
    /// if info is also empty, it will re-read metas from storage and try to recreate itself
    /// as a last resort, it will answer the query itself
    /// It can later be augmented to accept the stream name(s)
    /// to figure out which Query Nodes have those streams in their hottier
    pub async fn get_query_node(&mut self) -> QuerierMetadata {

        log::warn!("available_nodes- {:?}",self.available_nodes);
        // get node from query coodinator
        if self.available_nodes.len() > 0 {
            let mut drain = self.available_nodes.drain();
            let node_id = drain.next().unwrap();
            self.available_nodes = HashSet::from_iter(drain);
            log::warn!("updated available_nodes- {:?}",self.available_nodes);
            self.info.get(&node_id).unwrap().to_owned()
        } else if self.info.len() > 0 {
            self.info.values().next().unwrap().to_owned()
        } else {
            // no nodes available, send query request to self?
            // first check if any new query nodes are available
            self.new().await;

            if self.available_nodes.len() > 0 {
                let mut drain = self.available_nodes.drain();
                let node_id = drain.next().unwrap();
                self.available_nodes = HashSet::from_iter(drain);
                log::warn!("updated available_nodes- {:?}",self.available_nodes);
                self.info.get(&node_id).unwrap().to_owned()
            } else {
                QUERIER_META.clone()
            }
        }
    }

    pub fn append_querier_info(&mut self, node: QuerierMetadata) {
        // append the incoming metadata info to info
        let node_id = node.querier_id.clone();

        self.info.insert(node_id.clone(), node);
        self.available_nodes.insert(node_id);
        
        log::warn!("QUERY_COORDINATION- {:?}",self);
    }

    pub fn reinstate_node(&mut self, node: QuerierMetadata) {
        // make this node available again
        self.available_nodes.insert(node.querier_id);
    }

    pub async fn check_liveness(&mut self) {
        let mut to_remove: Vec<String> = Vec::new();
        for (node_id,node) in self.info.iter() {
            if !check_liveness(&node.domain_name).await {
                to_remove.push(node_id.clone());
            }
        }

        for node_id in to_remove {
            log::warn!("Removing node_id- {node_id}");
            self.info.remove(&node_id);
            self.available_nodes.remove(&node_id);
            self.stats.remove(&node_id);
        }
    }
}

#[derive(Default, Debug)]
pub struct QueryServer;

#[async_trait(?Send)]
impl ParseableServer for QueryServer {
    async fn start(
        &self,
        prometheus: actix_web_prometheus::PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }

            None => None,
        };

        self.set_querier_metadata().await?;

        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
            &CONFIG.parseable.trusted_ca_certs_path,
        )?;

        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| QueryServer::configure_routes(config, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // Create a channel to trigger server shutdown
        let (shutdown_trigger, shutdown_rx) = oneshot::channel::<()>();
        let server_shutdown_signal = Arc::new(Mutex::new(Some(shutdown_trigger)));

        // Clone the shutdown signal for the signal handler
        let shutdown_signal = server_shutdown_signal.clone();

        // Spawn the signal handler task
        tokio::spawn(async move {
            health_check::handle_signals(shutdown_signal).await;
        });

        // Create the HTTP server
        let http_server = HttpServer::new(create_app_fn)
            .workers(num_cpus::get())
            .shutdown_timeout(120);

        // Start the server with or without TLS
        let srv = if let Some(config) = ssl {
            http_server
                .bind_rustls_0_22(&CONFIG.parseable.address, config)?
                .run()
        } else {
            http_server.bind(&CONFIG.parseable.address)?.run()
        };

        // Graceful shutdown handling
        let srv_handle = srv.handle();

        tokio::spawn(async move {
            // Wait for the shutdown signal
            shutdown_rx.await.ok();

            // Initiate graceful shutdown
            log::info!("Graceful shutdown of HTTP server triggered");
            srv_handle.stop(true).await;
        });

        // Await the server to run and handle shutdown
        srv.await?;

        Ok(())
    }

    /// implementation of init should just invoke a call to initialize
    async fn init(&self) -> anyhow::Result<()> {
        self.validate()?;

        // Check if coordinator is present
        let parseable_json = self.check_coordinator_state().await?;
        // migration::run_file_migration(&CONFIG).await?;
        
        // migration::run_metadata_migration(&CONFIG, &parseable_json).await?;
        
        let metadata = storage::resolve_parseable_metadata(&parseable_json).await?;
        banner::print(&CONFIG, &metadata).await;
        // initialize the rbac map
        rbac::map::init(&metadata);
        // keep metadata info in mem
        metadata.set_global();
        self.initialize().await
    }

    fn validate(&self) -> anyhow::Result<()> {
        if CONFIG.get_storage_mode_string() == "Local drive" {
            return Err(anyhow::anyhow!(
                 "Query Server cannot be started in local storage mode. Please start the server in a supported storage mode.",
             ));
        }

        Ok(())
    }
}

impl QueryServer {
    // configure the api routes
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        config
            .service(
                web::scope(&base_path())
                    // POST "/query" ==> Get results of the SQL query passed in request body
                    .service(Self::get_query_factory())
                    .service(Self::get_query_coordinator_factory())
                    .service(Server::get_trino_factory())
                    .service(Server::get_cache_webscope())
                    .service(Server::get_liveness_factory())
                    .service(Server::get_readiness_factory())
                    .service(Server::get_about_factory())
                    .service(Self::get_logstream_webscope())
                    .service(Self::get_user_webscope())
                    .service(Server::get_dashboards_webscope())
                    .service(Server::get_filters_webscope())
                    .service(Server::get_llm_webscope())
                    .service(Server::get_oauth_webscope(oidc_client))
                    .service(Self::get_user_role_webscope())
                    .service(Server::get_metrics_webscope())
                    .service(Self::get_cluster_web_scope())
                    .service(Self::get_leader_factory()),
            )
            .service(Server::get_generated());
    }

    fn get_leader_factory() -> Resource {
        web::resource("/leader").post(query::make_leader)
    }

    fn get_query_factory() -> Resource {
        web::resource("/query").route(web::post().to(http::query::query).authorize(Action::Query))
    }

    fn get_query_coordinator_factory() -> Resource {
        web::resource("/query_coordinator").route(web::post().to(querier_query::query_coordinator).authorize(Action::Query))
    }

    // get the role webscope
    fn get_user_role_webscope() -> Scope {
        web::scope("/role")
            // GET Role List
            .service(resource("").route(web::get().to(role::list).authorize(Action::ListRole)))
            .service(
                // PUT and GET Default Role
                resource("/default")
                    .route(web::put().to(role::put_default).authorize(Action::PutRole))
                    .route(web::get().to(role::get_default).authorize(Action::GetRole)),
            )
            .service(
                // PUT, GET, DELETE Roles
                resource("/{name}")
                    .route(web::put().to(querier_role::put).authorize(Action::PutRole))
                    .route(web::delete().to(role::delete).authorize(Action::DeleteRole))
                    .route(web::get().to(role::get).authorize(Action::GetRole)),
            )
    }

    // get the user webscope
    fn get_user_webscope() -> Scope {
        web::scope("/user")
            .service(
                web::resource("")
                    // GET /user => List all users
                    .route(
                        web::get()
                            .to(http::rbac::list_users)
                            .authorize(Action::ListUser),
                    ),
            )
            .service(
                web::resource("/{username}")
                    // PUT /user/{username} => Create a new user
                    .route(
                        web::post()
                            .to(querier_rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{username} => Delete a user
                    .route(
                        web::delete()
                            .to(querier_rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{username}/role")
                    // PUT /user/{username}/role => Put roles for user
                    .route(
                        web::put()
                            .to(querier_rbac::put_role)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    )
                    .route(
                        web::get()
                            .to(http::rbac::get_role)
                            .authorize_for_user(Action::GetUserRoles),
                    ),
            )
            .service(
                web::resource("/{username}/generate-new-password")
                    // POST /user/{username}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(querier_rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
                    ),
            )
    }

    // get the logstream web scope
    fn get_logstream_webscope() -> Scope {
        web::scope("/logstream")
            .service(
                // GET "/logstream" ==> Get list of all Log Streams on the server
                web::resource("")
                    .route(web::get().to(logstream::list).authorize(Action::ListStream)),
            )
            .service(
                web::scope("/{logstream}")
                    .service(
                        web::resource("")
                            // PUT "/logstream/{logstream}" ==> Create log stream
                            .route(
                                web::put()
                                    .to(querier_logstream::put_stream)
                                    .authorize_for_stream(Action::CreateStream),
                            )
                            // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            .route(
                                web::post()
                                    .to(querier_ingest::post_event)
                                    .authorize_for_stream(Action::Ingest),
                            )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(querier_logstream::delete)
                                    .authorize_for_stream(Action::DeleteStream),
                            )
                            .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
                    )
                    .service(
                        // GET "/logstream/{logstream}/info" ==> Get info for given log stream
                        web::resource("/info").route(
                            web::get()
                                .to(logstream::get_stream_info)
                                .authorize_for_stream(Action::GetStreamInfo),
                        ),
                    )
                    .service(
                        web::resource("/alert")
                            // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                            .route(
                                web::put()
                                    .to(logstream::put_alert)
                                    .authorize_for_stream(Action::PutAlert),
                            )
                            // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                            .route(
                                web::get()
                                    .to(logstream::get_alert)
                                    .authorize_for_stream(Action::GetAlert),
                            ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                        web::resource("/schema").route(
                            web::get()
                                .to(logstream::schema)
                                .authorize_for_stream(Action::GetSchema),
                        ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                        web::resource("/stats").route(
                            web::get()
                                .to(querier_logstream::get_stats)
                                .authorize_for_stream(Action::GetStats),
                        ),
                    )
                    .service(
                        web::resource("/retention")
                            // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_retention)
                                    .authorize_for_stream(Action::PutRetention),
                            )
                            // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(logstream::get_retention)
                                    .authorize_for_stream(Action::GetRetention),
                            ),
                    )
                    .service(
                        web::resource("/cache")
                            // PUT "/logstream/{logstream}/cache" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(querier_logstream::put_enable_cache)
                                    .authorize_for_stream(Action::PutCacheEnabled),
                            )
                            // GET "/logstream/{logstream}/cache" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(querier_logstream::get_cache_enabled)
                                    .authorize_for_stream(Action::GetCacheEnabled),
                            ),
                    )
                    .service(
                        web::resource("/hottier")
                            // PUT "/logstream/{logstream}/hottier" ==> Set hottier for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_stream_hot_tier)
                                    .authorize_for_stream(Action::PutHotTierEnabled),
                            )
                            .route(
                                web::get()
                                    .to(logstream::get_stream_hot_tier)
                                    .authorize_for_stream(Action::GetHotTierEnabled),
                            )
                            .route(
                                web::delete()
                                    .to(logstream::delete_stream_hot_tier)
                                    .authorize_for_stream(Action::DeleteHotTierEnabled),
                            ),
                    ),
            )
    }

    fn get_cluster_web_scope() -> actix_web::Scope {
        web::scope("/cluster")
            .service(
                // GET "/cluster/info" ==> Get info of the cluster
                web::resource("/info").route(
                    web::get()
                        .to(cluster::get_cluster_info)
                        .authorize(Action::ListCluster),
                ),
            )
            // GET "/cluster/metrics" ==> Get metrics of the cluster
            .service(
                web::resource("/metrics").route(
                    web::get()
                        .to(cluster::get_cluster_metrics)
                        .authorize(Action::ListClusterMetrics),
                ),
            )
            // DELETE "/cluster/{ingestor_domain:port}" ==> Delete an ingestor from the cluster
            .service(
                web::scope("/{ingestor}").service(
                    web::resource("").route(
                        web::delete()
                            .to(cluster::remove_ingestor)
                            .authorize(Action::Deleteingestor),
                    ),
                ),
            )
    }

    // create the querier metadata and put the .querier.json file in the object store
    async fn set_querier_metadata(&self) -> anyhow::Result<()> {
        let storage_querier_metadata = migrate_querier_metadata().await?;
        let store = CONFIG.storage().get_object_store();

        // find the meta file in staging if not generate new metadata
        let resource = QUERIER_META.clone();
        // use the id that was generated/found in the staging and
        // generate the path for the object store
        let path = querier_metadata_path(None);

        // we are considering that we can always get from object store
        if storage_querier_metadata.is_some() {
            let mut store_data = storage_querier_metadata.unwrap();

            if store_data.domain_name != QUERIER_META.domain_name {
                store_data
                    .domain_name
                    .clone_from(&QUERIER_META.domain_name);
                store_data.port.clone_from(&QUERIER_META.port);

                let resource = serde_json::to_string(&store_data)?
                    .try_into_bytes()
                    .map_err(|err| anyhow!(err))?;

                // if pushing to object store fails propagate the error
                return store
                    .put_object(&path, resource)
                    .await
                    .map_err(|err| anyhow!(err));
            }
        } else {
            let resource = serde_json::to_string(&resource)?
                .try_into_bytes()
                .map_err(|err| anyhow!(err))?;

            store.put_object(&path, resource).await?;
        }

        Ok(())
    }

    /// initialize the server, run migrations as needed and start the server
    async fn initialize(&self) -> anyhow::Result<()> {
        let prometheus = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prometheus);

        migration::run_migration(&CONFIG).await?;

        //create internal stream at server start
        create_internal_stream_if_not_exists().await?;

        FILTERS.load().await?;
        DASHBOARDS.load().await?;
        // track all parquet files already in the data directory
        storage::retention::load_retention_from_global();

        // all internal data structures populated now.
        // start the analytics scheduler if enabled
        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler()?;
        }

        if matches!(init_cluster_metrics_scheduler(), Ok(())) {
            log::info!("Cluster metrics scheduler started successfully");
        }
        if let Some(hot_tier_manager) = HotTierManager::global() {
            hot_tier_manager.put_internal_stream_hot_tier().await?;
            hot_tier_manager.download_from_s3()?;
        };
        let (localsync_handler, mut localsync_outbox, localsync_inbox) =
            sync::run_local_sync().await;
        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
            sync::object_store_sync().await;

        tokio::spawn(airplane::server());
        let app = self.start(prometheus, CONFIG.parseable.openid.clone());

        tokio::pin!(app);

        loop {
            tokio::select! {
                e = &mut app => {
                    // actix server finished .. stop other threads and stop the server
                    remote_sync_inbox.send(()).unwrap_or(());
                    localsync_inbox.send(()).unwrap_or(());
                    if let Err(e) = localsync_handler.await {
                        log::error!("Error joining localsync_handler: {:?}", e);
                    }
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    return e
                },
                _ = &mut localsync_outbox => {
                    // crash the server if localsync fails for any reason
                    // panic!("Local Sync thread died. Server will fail now!")
                    return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
                },
                _ = &mut remote_sync_outbox => {
                    // remote_sync failed, this is recoverable by just starting remote_sync thread again
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync().await;
                }

            };
        }
    }

    // check for coordinator state. Is it there, or was it there in the past
    // this should happen before the set the querier metadata
    async fn check_coordinator_state(&self) -> anyhow::Result<Option<Bytes>, ObjectStorageError> {
        // how do we check for coordinator state?
        // based on the work flow of the system, the coordinator will always need to start first
        // i.e the coordinator will create the `.parseable.json` file
        // if the file already exists, it might need to be migrated

        let store = CONFIG.storage().get_object_store();
        let path = parseable_json_path();

        let parseable_json = store.get_object(&path).await;
        match parseable_json {
            Ok(_) => Ok(Some(parseable_json.unwrap())),
            Err(_) => Err(ObjectStorageError::Custom(
                "Coordinator Server has not been started yet. Please start the coordinator server first."
                    .to_string(),
            )),
        }
    }
}
