use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use dashmap::DashMap;
use bytes::Bytes;

use crate::{
    config::settings::Config,
    domain::{quality::StreamQuality, stream::VideoSegment},
};

use super::transcoder::{TranscoderService, TranscoderError};

/// Pool of transcoding workers for load distribution
pub struct TranscoderPool {
    /// Worker instances
    workers: Vec<Arc<TranscoderService>>,
    
    /// Current worker index (round-robin)
    current_worker: Arc<std::sync::atomic::AtomicUsize>,
    
    /// Active jobs per worker
    worker_loads: DashMap<usize, usize>,
    
    /// Configuration
    config: Arc<Config>,
}

impl TranscoderPool {
    /// Create new transcoder pool
    pub fn new(config: Arc<Config>, pool_size: usize) -> Result<Arc<Self>, TranscoderError> {
        let mut workers = Vec::with_capacity(pool_size);
        
        for _ in 0..pool_size {
            workers.push(TranscoderService::new(config.clone())?);
        }

        let worker_loads = DashMap::new();
        for i in 0..pool_size {
            worker_loads.insert(i, 0);
        }

        Ok(Arc::new(Self {
            workers,
            current_worker: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            worker_loads,
            config,
        }))
    }

    /// Get next available worker (least loaded)
    fn get_next_worker(&self) -> Arc<TranscoderService> {
        // Find worker with least load
        let mut min_load = usize::MAX;
        let mut min_idx = 0;

        for entry in self.worker_loads.iter() {
            if *entry.value() < min_load {
                min_load = *entry.value();
                min_idx = *entry.key();
            }
        }

        // Increment load
        self.worker_loads
            .entry(min_idx)
            .and_modify(|load| *load += 1);

        self.workers[min_idx].clone()
    }

    /// Start transcoding session
    pub async fn start_session(
        &self,
        stream_id: String,
    ) -> Result<mpsc::Sender<VideoSegment>, TranscoderError> {
        let worker = self.get_next_worker();
        worker.start_session(stream_id).await
    }

    /// Stop transcoding session
    pub async fn stop_session(&self, stream_id: &str) -> Result<(), TranscoderError> {
        // Try to stop on all workers
        for worker in &self.workers {
            let _ = worker.stop_session(stream_id).await;
        }
        Ok(())
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        let total_sessions: usize = self.workers
            .iter()
            .map(|w| w.active_sessions())
            .sum();

        let loads: Vec<usize> = self.worker_loads
            .iter()
            .map(|entry| *entry.value())
            .collect();

        PoolStats {
            worker_count: self.workers.len(),
            total_sessions,
            worker_loads: loads,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub worker_count: usize,
    pub total_sessions: usize,
    pub worker_loads: Vec<usize>,
}
