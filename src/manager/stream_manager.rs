use std::{collections::{HashMap, VecDeque}, sync::Arc, time::SystemTime};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use crate::{cdn::{CacheStats, EdgeCache, EdgeNodeManager, edge_manager::LoadBalancingStrategy}, config::Config, domain::{AdaptiveStream, EdgeNode, StreamMetadata, StreamQuality, VideoSegment}, streaming::{dash::manifest::DASHManifestGenerator, hls::manifest::HLSManifestGenerator, webrtc::signaling::WebRTCManager}};

pub struct StreamManager {
    pub streams: DashMap<String, Arc<RwLock<StreamMetadata>>>,
    broadcast_channels: DashMap<String, broadcast::Sender<VideoSegment>>,
    adaptive_streams: DashMap<String, HashMap<StreamQuality, AdaptiveStream>>,
    pub viewer_counts: DashMap<String, usize>,
    
    // CDN components
    pub edge_cache: Arc<EdgeCache>,
    hls_generator: HLSManifestGenerator,
    dash_generator: DASHManifestGenerator,
    pub webrtc_manager: Arc<WebRTCManager>,
    
    // Geo-distributed edge nodes
    pub edge_nodes: Arc<DashMap<String, EdgeNode>>,

    /// Edge node manager for CDN distribution
    pub edge_manager: Arc<EdgeNodeManager>,
}

/// System metrics
#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemMetrics {
    pub active_streams: usize,
    pub total_viewers: usize,
    pub cache_hit_rate: f64,
}

impl StreamManager {
    pub async fn new(config: &Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let edge_cache = EdgeCache::new(&config.redis.url, config.cdn.memory_cache_size)?;
        
        // Initialize edge manager with strategy from config
        let load_balancing_strategy = match config.cdn.load_balancing_strategy.as_str() {
            "round-robin" => LoadBalancingStrategy::RoundRobin,
            "least-connections" => LoadBalancingStrategy::LeastConnections,
            "geographic" => LoadBalancingStrategy::Geographic,
            "weighted" => LoadBalancingStrategy::Weighted,
            "random" => LoadBalancingStrategy::Random,
            _ => LoadBalancingStrategy::LeastConnections, // default
        };
        
        let edge_manager = EdgeNodeManager::new(load_balancing_strategy);
        
        Ok(Arc::new(Self {
            streams: DashMap::new(),
            broadcast_channels: DashMap::new(),
            adaptive_streams: DashMap::new(),
            viewer_counts: DashMap::new(),
            edge_cache,
            hls_generator: HLSManifestGenerator::new(),
            dash_generator: DASHManifestGenerator::new(),
            webrtc_manager: WebRTCManager::new(),
            edge_nodes: Arc::new(DashMap::new()), // Deprecated, use edge_manager
            edge_manager,
        }))
    }

    pub async fn create_stream(
        &self,
        stream_id: String,
        broadcaster_id: String,
        title: String,
    ) -> Result<(), String> {
        if self.streams.contains_key(&stream_id) {
            return Err("Stream already exists".to_string());
        }

        let metadata = StreamMetadata {
            stream_id: stream_id.clone(),
            broadcaster_id,
            title,
            viewers: 0,
            started_at: SystemTime::now(),
            bitrate_kbps: 3000,
            resolution: "1280x720".to_string(),
            codec: "H264".to_string(),
        };

        let (tx, _) = broadcast::channel(100);
        
        self.streams.insert(stream_id.clone(), Arc::new(RwLock::new(metadata)));
        self.broadcast_channels.insert(stream_id.clone(), tx);
        self.viewer_counts.insert(stream_id.clone(), 0);

        // Initialize adaptive streams with ring buffers
        let mut variants = HashMap::new();
        for quality in [
            StreamQuality::Low,
            StreamQuality::Medium,
            StreamQuality::High,
            StreamQuality::Ultra,
        ] {
            let (bitrate, resolution) = match quality {
                StreamQuality::Low => (1000, (640, 360)),
                StreamQuality::Medium => (3000, (1280, 720)),
                StreamQuality::High => (6000, (1920, 1080)),
                StreamQuality::Ultra => (12000, (3840, 2160)),
            };

            variants.insert(
                quality,
                AdaptiveStream {
                    quality,
                    bitrate_kbps: bitrate,
                    resolution,
                    segments: VecDeque::with_capacity(60),
                    max_segments: 60, // 60 seconds DVR window
                },
            );
        }
        
        self.adaptive_streams.insert(stream_id, variants);
        Ok(())
    }

    pub async fn ingest_segment(
        &self,
        stream_id: &str,
        segment: VideoSegment,
    ) -> Result<(), String> {
        let tx = self
            .broadcast_channels
            .get(stream_id)
            .ok_or("Stream not found")?;

        // Store segment in adaptive streams
        if let Some(mut variants) = self.adaptive_streams.get_mut(stream_id) {
            for (quality, adaptive_stream) in variants.iter_mut() {
                let mut transcoded_segment = segment.clone();
                transcoded_segment.quality = *quality;
                transcoded_segment.data = self.simulate_transcode(&segment.data, *quality);
                
                // Add to ring buffer
                adaptive_stream.segments.push_back(transcoded_segment.clone());
                if adaptive_stream.segments.len() > adaptive_stream.max_segments {
                    adaptive_stream.segments.pop_front();
                }

                // Cache segment in CDN
                let cache_key = format!(
                    "segment:{}:{}:{}",
                    stream_id,
                    quality.to_string(),
                    transcoded_segment.sequence
                );
                self.edge_cache
                    .set(&cache_key, transcoded_segment.data, 300)
                    .await;
            }
        }

        // Broadcast to all viewers
        let _ = tx.send(segment);

        Ok(())
    }

    // Get HLS master playlist
    pub fn get_hls_master_playlist(&self, stream_id: &str) -> Option<String> {
        if !self.streams.contains_key(stream_id) {
            return None;
        }

        let qualities = vec![
            StreamQuality::Low,
            StreamQuality::Medium,
            StreamQuality::High,
            StreamQuality::Ultra,
        ];

        Some(self.hls_generator.generate_master_playlist(stream_id, &qualities))
    }

    // Get HLS media playlist for specific quality
    pub fn get_hls_media_playlist(
        &self,
        stream_id: &str,
        quality: StreamQuality,
    ) -> Option<String> {
        let variants = self.adaptive_streams.get(stream_id)?;
        let adaptive_stream = variants.get(&quality)?;

        Some(self.hls_generator.generate_media_playlist(
            stream_id,
            quality,
            &adaptive_stream.segments,
        ))
    }

    // Get DASH manifest
    pub fn get_dash_manifest(&self, stream_id: &str) -> Option<String> {
        let variants = self.adaptive_streams.get(stream_id)?;

        let qualities = vec![
            StreamQuality::Low,
            StreamQuality::Medium,
            StreamQuality::High,
            StreamQuality::Ultra,
        ];

        let segments: HashMap<StreamQuality, VecDeque<VideoSegment>> = variants
            .iter()
            .map(|(q, s)| (*q, s.segments.clone()))
            .collect();

        Some(self.dash_generator.generate_mpd(stream_id, &qualities, &segments))
    }

    // Get segment from cache or storage
    pub async fn get_segment(
        &self,
        stream_id: &str,
        quality: StreamQuality,
        sequence: u64,
    ) -> Option<Bytes> {
        let cache_key = format!("segment:{}:{}:{}", stream_id, quality.to_string(), sequence);

        // Try cache first
        if let Some(data) = self.edge_cache.get(&cache_key).await {
            return Some(data);
        }

        // Fallback to in-memory segments
        let variants = self.adaptive_streams.get(stream_id)?;
        let adaptive_stream = variants.get(&quality)?;
        
        adaptive_stream
            .segments
            .iter()
            .find(|s| s.sequence == sequence)
            .map(|s| s.data.clone())
    }

    // Register edge node
    pub fn register_edge_node(&self, node: EdgeNode) {
        self.edge_nodes.insert(node.node_id.clone(), node);
    }

    // Select best edge node for viewer based on location
    pub fn select_edge_node(&self, viewer_location: &str) -> Option<EdgeNode> {
        // Simple selection - in production use geolocation and load balancing
        self.edge_nodes
            .iter()
            .filter(|n| n.current_load < n.capacity)
            .min_by_key(|n| n.current_load)
            .map(|n| n.value().clone())
    }

    pub fn subscribe_to_stream(
        &self,
        stream_id: &str,
        quality: StreamQuality,
    ) -> Result<broadcast::Receiver<VideoSegment>, String> {
        let tx = self
            .broadcast_channels
            .get(stream_id)
            .ok_or("Stream not found")?;

        self.viewer_counts
            .entry(stream_id.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        if let Some(metadata) = self.streams.get(stream_id) {
            let count = self.viewer_counts.get(stream_id).map(|c| *c).unwrap_or(0);
            metadata.write().viewers = count;
        }

        Ok(tx.subscribe())
    }

    pub fn unsubscribe_from_stream(&self, stream_id: &str) {
        self.viewer_counts
            .entry(stream_id.to_string())
            .and_modify(|count| {
                if *count > 0 {
                    *count -= 1;
                }
            });

        if let Some(metadata) = self.streams.get(stream_id) {
            let count = self.viewer_counts.get(stream_id).map(|c| *c).unwrap_or(0);
            metadata.write().viewers = count;
        }
    }

    pub fn get_stream_metadata(&self, stream_id: &str) -> Option<StreamMetadata> {
        self.streams.get(stream_id).map(|m| m.read().clone())
    }

    pub fn list_streams(&self) -> Vec<StreamMetadata> {
        self.streams
            .iter()
            .map(|entry| entry.value().read().clone())
            .collect()
    }

    pub async fn end_stream(&self, stream_id: &str) -> Result<(), String> {
        // Invalidate cache
        self.edge_cache.invalidate(&format!("segment:{}", stream_id)).await;
        
        self.streams.remove(stream_id);
        self.broadcast_channels.remove(stream_id);
        self.adaptive_streams.remove(stream_id);
        self.viewer_counts.remove(stream_id);
        Ok(())
    }

    fn simulate_transcode(&self, data: &Bytes, quality: StreamQuality) -> Bytes {
        let scale_factor = match quality {
            StreamQuality::Low => 0.3,
            StreamQuality::Medium => 0.5,
            StreamQuality::High => 0.8,
            StreamQuality::Ultra => 1.0,
        };
        
        let new_size = (data.len() as f32 * scale_factor) as usize;
        data.slice(0..new_size.min(data.len()))
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> HashMap<String, CacheStats> {
        self.edge_cache.get_stats()
    }

    /// Invalidate cache for a stream
    pub async fn invalidate_cache(&self, stream_id: &str) {
        let pattern = format!("segment:{}", stream_id);
        self.edge_cache.invalidate(&pattern).await;
    }

    /// Get all edge nodes
    pub fn get_all_edge_nodes(&self) -> Vec<EdgeNode> {
        self.edge_nodes
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get current load (total viewers)
    pub async fn get_current_load(&self) -> usize {
        self.viewer_counts
            .iter()
            .map(|entry| *entry.value())
            .sum()
    }

    /// Get metrics
    pub async fn get_metrics(&self) -> SystemMetrics {
        let active_streams = self.streams.len();
        let total_viewers: usize = self.viewer_counts
            .iter()
            .map(|entry| *entry.value())
            .sum();

        let cache_stats = self.get_cache_stats().await;
        let total_hits: u64 = cache_stats.values().map(|s| s.hits).sum();
        let total_misses: u64 = cache_stats.values().map(|s| s.misses).sum();
        let cache_hit_rate = if total_hits + total_misses > 0 {
            total_hits as f64 / (total_hits + total_misses) as f64
        } else {
            0.0
        };

        SystemMetrics {
            active_streams,
            total_viewers,
            cache_hit_rate,
        }
    }

    /// Cleanup expired cache entries
    pub async fn cleanup_cache(&self) -> Result<(), String> {
        // In production: implement cache expiration logic
        // For now, just log
        tracing::debug!("Cache cleanup running");
        Ok(())
    }
}