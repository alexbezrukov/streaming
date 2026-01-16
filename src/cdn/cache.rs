use std::{collections::HashMap, sync::Arc};
use parking_lot::RwLock;
use bytes::Bytes;
use dashmap::DashMap;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

pub struct EdgeCache {
    // In-memory LRU cache for hot segments
    memory_cache: Arc<RwLock<lru::LruCache<String, Bytes>>>,
    
    // Redis for distributed cache across edge nodes
    pub redis_client: redis::Client,
    
    // Cache statistics
    stats: Arc<DashMap<String, CacheStats>>,
}


impl EdgeCache {
    pub fn new(redis_url: &str, memory_capacity: usize) -> Result<Arc<Self>, redis::RedisError> {
        let redis_client = redis::Client::open(redis_url)?;
        
        Ok(Arc::new(Self {
            memory_cache: Arc::new(RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(memory_capacity).unwrap()
            ))),
            redis_client,
            stats: Arc::new(DashMap::new()),
        }))
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // Try memory cache first (L1)
        if let Some(data) = self.memory_cache.write().get(key) {
            self.record_hit(key);
            return Some(data.clone());
        }

        // Try Redis (L2)
        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            if let Ok(data) = redis::cmd("GET")
                .arg(key)
                .query_async::<_, Vec<u8>>(&mut conn)
                .await
            {
                let bytes = Bytes::from(data);

                // Promote to memory cache
                self.memory_cache.write().put(key.to_string(), bytes.clone());
                self.record_hit(key);

                return Some(bytes);
            }
        }

        self.record_miss(key);
        None
    }

    pub async fn set(&self, key: &str, data: Bytes, ttl_seconds: u64) {
        // Store in memory cache
        self.memory_cache.write().put(key.to_string(), data.clone());

        // Store in Redis with TTL
        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            let _: Result<(), redis::RedisError> = redis::cmd("SETEX")
                .arg(key)
                .arg(ttl_seconds)
                .arg(data.as_ref())
                .query_async(&mut conn)
                .await;
        }
    }

    pub async fn invalidate(&self, pattern: &str) {
        // Remove from memory
        let keys_to_remove: Vec<String> = self
            .memory_cache
            .write()
            .iter()
            .filter(|(k, _)| k.contains(pattern))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            self.memory_cache.write().pop(&key);
        }

        // Remove from Redis
        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            let _: Result<Vec<String>, redis::RedisError> = redis::cmd("KEYS")
                .arg(format!("{}*", pattern))
                .query_async(&mut conn)
                .await;
        }
    }

    fn record_hit(&self, key: &str) {
        self.stats
            .entry(key.to_string())
            .or_insert(CacheStats::default())
            .hits += 1;
    }

    fn record_miss(&self, key: &str) {
        self.stats
            .entry(key.to_string())
            .or_insert(CacheStats::default())
            .misses += 1;
    }

    pub fn get_stats(&self) -> HashMap<String, CacheStats> {
        self.stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
}