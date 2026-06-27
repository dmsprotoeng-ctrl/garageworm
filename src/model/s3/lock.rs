use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::{global, metrics::BoundCounter};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;

use garage_util::data::*;
use garage_util::error::*;

use garage_rpc::rpc_helper::RequestStrategy;
use garage_rpc::system::System;
use garage_rpc::*;

#[allow(dead_code)]
pub struct LockManagerMetrics {
	pub acquired_counter: BoundCounter<u64>,
	pub contended_counter: BoundCounter<u64>,
	pub retention_blocked_counter: BoundCounter<u64>,
}

impl LockManagerMetrics {
	fn new() -> Self {
		let meter = global::meter("garage_model/lock");
		Self {
			acquired_counter: meter
				.u64_counter("lock.acquired_total")
				.with_description("Number of distributed locks acquired")
				.init()
				.bind(&[]),
			contended_counter: meter
				.u64_counter("lock.contended_total")
				.with_description("Number of distributed lock acquisitions that failed due to contention")
				.init()
				.bind(&[]),
			retention_blocked_counter: meter
				.u64_counter("lock.retention_blocked_total")
				.with_description("Number of operations blocked by retention policy")
				.init()
				.bind(&[]),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LockKey {
	pub bucket_id: Uuid,
	pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LockRpc {
	Acquire {
		key: LockKey,
		owner: Uuid,
		ttl_ms: u64,
	},
	Release {
		key: LockKey,
		owner: Uuid,
	},
	Ok,
}

impl Rpc for LockRpc {
	type Response = Result<LockRpc, Error>;
}

struct LockEntry {
	owner: Uuid,
	expires_at: Instant,
}

pub struct LockManager {
	locks: Mutex<HashMap<LockKey, LockEntry>>,
	pub(crate) system: Arc<System>,
	pub(crate) endpoint: Arc<Endpoint<LockRpc, LockManager>>,
	ttl_duration: Duration,
	pub metrics: LockManagerMetrics,
}

impl LockManager {
	pub fn new(system: Arc<System>, lock_ttl_ms: u64) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint("garage_model::s3::lock/LockRpc".to_string());

		let mgr = Arc::new(Self {
			locks: Mutex::new(HashMap::new()),
			system,
			endpoint,
			ttl_duration: Duration::from_millis(lock_ttl_ms),
			metrics: LockManagerMetrics::new(),
		});
		mgr.endpoint.set_handler(mgr.clone());
		mgr
	}

	pub fn spawn_reaper(self: &Arc<Self>) {
		let this = self.clone();
		tokio::spawn(async move {
			let mut interval = tokio::time::interval(Duration::from_secs(5));
			loop {
				interval.tick().await;
				this.cleanup_expired().await;
			}
		});
	}

	async fn cleanup_expired(&self) {
		let mut locks = self.locks.lock().await;
		locks.retain(|_, entry| Instant::now() < entry.expires_at);
	}

	async fn handle_acquire(
		&self,
		key: &LockKey,
		owner: Uuid,
		ttl_ms: u64,
	) -> Result<(), Error> {
		let mut locks = self.locks.lock().await;
		let now = Instant::now();
		if let Some(entry) = locks.get(key) {
			if entry.expires_at > now && entry.owner != owner {
				return Err(Error::Message(format!("lock held by {:?}", entry.owner)));
			}
		}
		locks.insert(
			key.clone(),
			LockEntry {
				owner,
				expires_at: now + Duration::from_millis(ttl_ms),
			},
		);
		Ok(())
	}

	async fn handle_release(&self, key: &LockKey, owner: Uuid) {
		let mut locks = self.locks.lock().await;
		if let Some(entry) = locks.get(key) {
			if entry.owner == owner {
				locks.remove(key);
			}
		}
	}

	/// Spawn a background task that periodically renews the lock until
	/// the returned `tokio::sync::mpsc::Sender` is dropped.
	pub fn spawn_renew(
		self: &Arc<Self>,
		lock_key: LockKey,
		owner: Uuid,
		who: Vec<Uuid>,
	) -> tokio::sync::mpsc::Sender<()> {
		let (tx, mut rx) = tokio::sync::mpsc::channel(1);
		let this = self.clone();
		let ttl = self.ttl_duration;
		tokio::spawn(async move {
			loop {
				tokio::select! {
					_ = tokio::time::sleep(ttl / 2) => {
						let msg = LockRpc::Acquire {
							key: lock_key.clone(),
							owner,
							ttl_ms: ttl.as_millis() as u64,
						};
						let _ = this
							.system
							.rpc_helper()
							.try_call_many(
								&this.endpoint,
								&who,
								msg,
								RequestStrategy::with_priority(PRIO_BACKGROUND).with_quorum(0),
							)
							.await;
					}
					_ = rx.recv() => break,
				}
			}
		});
		tx
	}

	pub async fn acquire_distributed(
		self: &Arc<Self>,
		bucket_id: Uuid,
		key: &str,
	) -> Result<LockGuard, Error> {
		let who = {
			let layout = self.system.cluster_layout();
			let current = layout.current()?;
			let hash: Hash = bucket_id;
			current.nodes_of(&hash).collect::<Vec<_>>()
		};

		let lock_key = LockKey {
			bucket_id,
			key: key.to_string(),
		};
		let owner = gen_uuid();
		let ttl_ms = self.ttl_duration.as_millis() as u64;
		let quorum = who.len() / 2 + 1;

		let msg = LockRpc::Acquire {
			key: lock_key.clone(),
			owner,
			ttl_ms,
		};

		match self
			.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				&who,
				msg,
				RequestStrategy::with_priority(PRIO_NORMAL).with_quorum(quorum),
			)
			.await
		{
			Ok(_) => {
				self.metrics.acquired_counter.add(1);
				Ok(LockGuard {
					lock_manager: self.clone(),
					lock_key,
					owner,
					who,
				})
			}
			Err(e) => {
				self.metrics.contended_counter.add(1);
				let release_msg = LockRpc::Release {
					key: lock_key,
					owner,
				};
				let _ = self
					.system
					.rpc_helper()
					.call_many(
						&self.endpoint,
						&who,
						release_msg,
						RequestStrategy::with_priority(PRIO_BACKGROUND).with_quorum(0),
					)
					.await;
				Err(e)
			}
		}
	}
}

impl EndpointHandler<LockRpc> for LockManager {
	async fn handle(self: &Arc<Self>, msg: &LockRpc, _from: NodeID) -> Result<LockRpc, Error> {
		match msg {
			LockRpc::Acquire { key, owner, ttl_ms } => {
				self.handle_acquire(key, *owner, *ttl_ms).await?;
				Ok(LockRpc::Ok)
			}
			LockRpc::Release { key, owner } => {
				self.handle_release(key, *owner).await;
				Ok(LockRpc::Ok)
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

pub struct LockGuard {
	lock_manager: Arc<LockManager>,
	pub lock_key: LockKey,
	pub owner: Uuid,
	pub who: Vec<Uuid>,
}

impl Drop for LockGuard {
	fn drop(&mut self) {
		let msg = LockRpc::Release {
			key: self.lock_key.clone(),
			owner: self.owner,
		};
		let endpoint = self.lock_manager.endpoint.clone();
		let rpc_helper = self.lock_manager.system.rpc_helper().clone();
		let who = self.who.clone();
		tokio::spawn(async move {
			let _ = rpc_helper
				.call_many(
					&endpoint,
					&who,
					msg,
					RequestStrategy::with_priority(PRIO_BACKGROUND).with_quorum(0),
				)
				.await;
		});
	}
}
