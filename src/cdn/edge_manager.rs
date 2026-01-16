use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::domain::edge_node::EdgeNode;

/// Edge node manager for CDN distribution
pub struct EdgeNodeManager {
    /// Active edge nodes
    nodes: DashMap<String, Arc<RwLock<EdgeNode>>>,
    
    /// Geographic regions mapping
    regions: DashMap<String, Vec<String>>,
    
    /// Load balancing strategy
    strategy: LoadBalancingStrategy,
    
    /// Health check configuration
    health_config: HealthCheckConfig,
}

/// Load balancing strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalancingStrategy {
    /// Round-robin distribution
    RoundRobin,
    
    /// Least connections (least loaded node)
    LeastConnections,
    
    /// Geographic proximity (closest node)
    Geographic,
    
    /// Weighted round-robin based on capacity
    Weighted,
    
    /// Random selection
    Random,
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Maximum time since last heartbeat before node is unhealthy
    pub heartbeat_timeout: Duration,
    
    /// Maximum load percentage before node is overloaded
    pub max_load_percentage: f64,
    
    /// Enable automatic failover
    pub auto_failover: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            max_load_percentage: 90.0,
            auto_failover: true,
        }
    }
}

/// Node selection result
#[derive(Debug, Clone)]
pub struct NodeSelection {
    pub node: EdgeNode,
    pub reason: SelectionReason,
    pub alternatives: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SelectionReason {
    LeastLoaded,
    Geographic,
    Weighted,
    RoundRobin,
    OnlyAvailable,
}

/// Edge node statistics
#[derive(Debug, Clone, Serialize)]
pub struct EdgeNodeStats {
    pub node_id: String,
    pub location: String,
    pub total_requests: u64,
    pub active_connections: usize,
    pub avg_response_time_ms: f64,
    pub uptime_seconds: u64,
    pub last_heartbeat: SystemTime,
}

impl EdgeNodeManager {
    /// Create new edge node manager
    pub fn new(strategy: LoadBalancingStrategy) -> Arc<Self> {
        Arc::new(Self {
            nodes: DashMap::new(),
            regions: DashMap::new(),
            strategy,
            health_config: HealthCheckConfig::default(),
        })
    }

    /// Create with custom health check config
    pub fn with_health_config(
        strategy: LoadBalancingStrategy,
        health_config: HealthCheckConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            nodes: DashMap::new(),
            regions: DashMap::new(),
            strategy,
            health_config,
        })
    }

    /// Register a new edge node
    pub async fn register_node(&self, node: EdgeNode) {
        let node_id = node.node_id.clone();
        let location = node.location.clone();
        
        self.nodes.insert(
            node_id.clone(),
            Arc::new(RwLock::new(node))
        );

        // Add to region mapping
        self.regions
            .entry(location.clone())
            .or_insert_with(Vec::new)
            .push(node_id.clone());

        tracing::info!(
            "Registered edge node: {} in region {}",
            node_id,
            location
        );
    }

    /// Unregister an edge node
    pub async fn unregister_node(&self, node_id: &str) -> bool {
        if let Some((_, node_arc)) = self.nodes.remove(node_id) {
            let node = node_arc.read().await;
            let location = node.location.clone();
            
            // Remove from region mapping
            if let Some(mut nodes) = self.regions.get_mut(&location) {
                nodes.retain(|id| id != node_id);
            }
            
            tracing::info!("Unregistered edge node: {}", node_id);
            true
        } else {
            false
        }
    }

    /// Update node heartbeat
    pub async fn update_heartbeat(&self, node_id: &str, load: usize) -> bool {
        if let Some(node_arc) = self.nodes.get(node_id) {
            let mut node = node_arc.write().await;
            node.update_load(load);
            tracing::debug!(
                "Updated heartbeat for {}: load={}",
                node_id,
                load
            );
            true
        } else {
            false
        }
    }

    /// Select best edge node for viewer
    pub async fn select_node(
        &self,
        viewer_location: &str,
    ) -> Option<NodeSelection> {
        match self.strategy {
            LoadBalancingStrategy::LeastConnections => {
                self.select_least_loaded().await
            }
            LoadBalancingStrategy::Geographic => {
                self.select_by_geography(viewer_location).await
            }
            LoadBalancingStrategy::Weighted => {
                self.select_weighted().await
            }
            LoadBalancingStrategy::RoundRobin => {
                self.select_round_robin().await
            }
            LoadBalancingStrategy::Random => {
                self.select_random().await
            }
        }
    }

    /// Select node with least load
    async fn select_least_loaded(&self) -> Option<NodeSelection> {
        let healthy_nodes = self.get_healthy_nodes().await;
        
        if healthy_nodes.is_empty() {
            return None;
        }

        let mut best_node: Option<EdgeNode> = None;
        let mut min_load = f64::MAX;

        for node in &healthy_nodes {
            let load = node.load_percentage();
            if load < min_load {
                min_load = load;
                best_node = Some(node.clone());
            }
        }

        best_node.map(|node| {
            let alternatives = healthy_nodes
                .iter()
                .filter(|n| n.node_id != node.node_id)
                .take(3)
                .map(|n| n.node_id.clone())
                .collect();

            NodeSelection {
                node,
                reason: SelectionReason::LeastLoaded,
                alternatives,
            }
        })
    }

    /// Select node by geographic proximity
    async fn select_by_geography(
        &self,
        viewer_location: &str,
    ) -> Option<NodeSelection> {
        // First try exact location match
        if let Some(nodes) = self.regions.get(viewer_location) {
            for node_id in nodes.value() {
                if let Some(node_arc) = self.nodes.get(node_id) {
                    let node = node_arc.read().await;
                    if self.is_node_healthy(&node).await {
                        return Some(NodeSelection {
                            node: node.clone(),
                            reason: SelectionReason::Geographic,
                            alternatives: self.get_alternative_nodes(&node.node_id, 3).await,
                        });
                    }
                }
            }
        }

        // Fallback to closest region
        let closest_region = self.find_closest_region(viewer_location);
        if let Some(region) = closest_region {
            if let Some(nodes) = self.regions.get(&region) {
                for node_id in nodes.value() {
                    if let Some(node_arc) = self.nodes.get(node_id) {
                        let node = node_arc.read().await;
                        if self.is_node_healthy(&node).await {
                            return Some(NodeSelection {
                                node: node.clone(),
                                reason: SelectionReason::Geographic,
                                alternatives: self.get_alternative_nodes(&node.node_id, 3).await,
                            });
                        }
                    }
                }
            }
        }

        // Final fallback to any healthy node
        self.select_least_loaded().await
    }

    /// Select node using weighted distribution
    async fn select_weighted(&self) -> Option<NodeSelection> {
        let healthy_nodes = self.get_healthy_nodes().await;
        
        if healthy_nodes.is_empty() {
            return None;
        }

        // Calculate weights based on available capacity
        let total_available_capacity: usize = healthy_nodes
            .iter()
            .map(|node| node.capacity.saturating_sub(node.current_load))
            .sum();

        if total_available_capacity == 0 {
            return self.select_least_loaded().await;
        }

        // Select using weighted random
        let mut rng_value = rand::random::<f64>() * total_available_capacity as f64;
        
        for node in &healthy_nodes {
            let available = node.capacity.saturating_sub(node.current_load) as f64;
            if rng_value <= available {
                let alternatives = healthy_nodes
                    .iter()
                    .filter(|n| n.node_id != node.node_id)
                    .take(3)
                    .map(|n| n.node_id.clone())
                    .collect();

                return Some(NodeSelection {
                    node: node.clone(),
                    reason: SelectionReason::Weighted,
                    alternatives,
                });
            }
            rng_value -= available;
        }

        // Fallback
        self.select_least_loaded().await
    }

    /// Select node using round-robin
    async fn select_round_robin(&self) -> Option<NodeSelection> {
        let healthy_nodes = self.get_healthy_nodes().await;
        
        if healthy_nodes.is_empty() {
            return None;
        }

        // Simple round-robin based on current time
        let index = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize) % healthy_nodes.len();

        let node = healthy_nodes[index].clone();
        let alternatives = healthy_nodes
            .iter()
            .filter(|n| n.node_id != node.node_id)
            .take(3)
            .map(|n| n.node_id.clone())
            .collect();

        Some(NodeSelection {
            node,
            reason: SelectionReason::RoundRobin,
            alternatives,
        })
    }

    /// Select random node
    async fn select_random(&self) -> Option<NodeSelection> {
        let healthy_nodes = self.get_healthy_nodes().await;
        
        if healthy_nodes.is_empty() {
            return None;
        }

        let index = rand::random::<usize>() % healthy_nodes.len();
        let node = healthy_nodes[index].clone();

        Some(NodeSelection {
            node,
            reason: SelectionReason::OnlyAvailable,
            alternatives: vec![],
        })
    }

    /// Get all healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<EdgeNode> {
        let mut healthy = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            if self.is_node_healthy(&node).await {
                healthy.push(node.clone());
            }
        }

        healthy
    }

    /// Check if node is healthy
    async fn is_node_healthy(&self, node: &EdgeNode) -> bool {
        let now = SystemTime::now();
        
        // Check heartbeat
        let heartbeat_age = now
            .duration_since(node.last_heartbeat)
            .unwrap_or(Duration::from_secs(u64::MAX));
        
        if heartbeat_age > self.health_config.heartbeat_timeout {
            return false;
        }

        // Check load
        let load_percentage = node.load_percentage();
        if load_percentage > self.health_config.max_load_percentage {
            return false;
        }

        // Check availability
        node.is_available()
    }

    /// Get all edge nodes
    pub async fn get_all_nodes(&self) -> Vec<EdgeNode> {
        let mut nodes = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            nodes.push(node.clone());
        }

        nodes
    }

    /// Get node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<EdgeNode> {
        if let Some(node_arc) = self.nodes.get(node_id) {
            let node = node_arc.read().await;
            Some(node.clone())
        } else {
            None
        }
    }

    /// Get nodes in region
    pub fn get_nodes_in_region(&self, location: &str) -> Vec<String> {
        self.regions
            .get(location)
            .map(|nodes| nodes.clone())
            .unwrap_or_default()
    }

    /// Get alternative nodes
    async fn get_alternative_nodes(&self, exclude_id: &str, count: usize) -> Vec<String> {
        let healthy_nodes = self.get_healthy_nodes().await;
        
        healthy_nodes
            .iter()
            .filter(|node| node.node_id != exclude_id)
            .take(count)
            .map(|node| node.node_id.clone())
            .collect()
    }

    /// Find closest region (simplified geographic matching)
    fn find_closest_region(&self, viewer_location: &str) -> Option<String> {
        // Simple region matching based on prefixes
        let viewer_prefix = viewer_location.split('-').next()?;
        
        for region in self.regions.iter() {
            if region.key().starts_with(viewer_prefix) {
                return Some(region.key().clone());
            }
        }

        // Fallback to any region with nodes
        self.regions
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .map(|entry| entry.key().clone())
            .next()
    }

    /// Get node statistics
    pub async fn get_node_stats(&self, node_id: &str) -> Option<EdgeNodeStats> {
        let node_arc = self.nodes.get(node_id)?;
        let node = node_arc.read().await;
        
        let uptime = SystemTime::now()
            .duration_since(node.last_heartbeat)
            .unwrap_or_default();

        Some(EdgeNodeStats {
            node_id: node.node_id.clone(),
            location: node.location.clone(),
            total_requests: 0, // Would be tracked separately in production
            active_connections: node.current_load,
            avg_response_time_ms: 0.0, // Would be tracked separately
            uptime_seconds: uptime.as_secs(),
            last_heartbeat: node.last_heartbeat,
        })
    }

    /// Get cluster statistics
    pub async fn get_cluster_stats(&self) -> ClusterStats {
        let all_nodes = self.get_all_nodes().await;
        let healthy_nodes = self.get_healthy_nodes().await;
        
        let total_capacity: usize = all_nodes.iter().map(|n| n.capacity).sum();
        let used_capacity: usize = all_nodes.iter().map(|n| n.current_load).sum();
        
        let avg_load = if !all_nodes.is_empty() {
            all_nodes.iter().map(|n| n.load_percentage()).sum::<f64>()
                / all_nodes.len() as f64
        } else {
            0.0
        };

        ClusterStats {
            total_nodes: all_nodes.len(),
            healthy_nodes: healthy_nodes.len(),
            total_capacity,
            used_capacity,
            available_capacity: total_capacity.saturating_sub(used_capacity),
            avg_load_percentage: avg_load,
            regions: self.regions.len(),
        }
    }

    /// Run health check on all nodes
    pub async fn run_health_check(&self) -> HealthCheckReport {
        let mut report = HealthCheckReport::default();
        
        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            
            if self.is_node_healthy(&node).await {
                report.healthy_nodes.push(node.node_id.clone());
            } else {
                let reason = if SystemTime::now()
                    .duration_since(node.last_heartbeat)
                    .unwrap_or(Duration::from_secs(u64::MAX))
                    > self.health_config.heartbeat_timeout
                {
                    "Heartbeat timeout"
                } else if node.load_percentage() > self.health_config.max_load_percentage {
                    "Overloaded"
                } else {
                    "Unavailable"
                };
                
                report.unhealthy_nodes.push((node.node_id.clone(), reason.to_string()));
            }
        }

        report
    }

    /// Set load balancing strategy
    pub fn set_strategy(&mut self, strategy: LoadBalancingStrategy) {
        self.strategy = strategy;
        tracing::info!("Load balancing strategy changed to: {:?}", strategy);
    }

    /// Get current strategy
    pub fn get_strategy(&self) -> LoadBalancingStrategy {
        self.strategy
    }
}

/// Cluster statistics
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub total_capacity: usize,
    pub used_capacity: usize,
    pub available_capacity: usize,
    pub avg_load_percentage: f64,
    pub regions: usize,
}

/// Health check report
#[derive(Debug, Clone, Default)]
pub struct HealthCheckReport {
    pub healthy_nodes: Vec<String>,
    pub unhealthy_nodes: Vec<(String, String)>,
}

impl HealthCheckReport {
    pub fn total_nodes(&self) -> usize {
        self.healthy_nodes.len() + self.unhealthy_nodes.len()
    }

    pub fn health_percentage(&self) -> f64 {
        if self.total_nodes() == 0 {
            return 100.0;
        }
        (self.healthy_nodes.len() as f64 / self.total_nodes() as f64) * 100.0
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let manager = EdgeNodeManager::new(LoadBalancingStrategy::LeastConnections);
        
        let node = EdgeNode::new(
            "test-node-1".to_string(),
            "EU-WEST".to_string(),
            1000,
        );
        
        manager.register_node(node).await;
        assert_eq!(manager.nodes.len(), 1);
        
        assert!(manager.unregister_node("test-node-1").await);
        assert_eq!(manager.nodes.len(), 0);
    }

    #[tokio::test]
    async fn test_least_connections_selection() {
        let manager = EdgeNodeManager::new(LoadBalancingStrategy::LeastConnections);
        
        // Register nodes with different loads
        let mut node1 = EdgeNode::new("node-1".to_string(), "EU".to_string(), 100);
        node1.update_load(50); // 50% load
        
        let mut node2 = EdgeNode::new("node-2".to_string(), "EU".to_string(), 100);
        node2.update_load(20); // 20% load
        
        manager.register_node(node1).await;
        manager.register_node(node2).await;
        
        // Should select node-2 (lower load)
        let selection = manager.select_least_loaded().await.unwrap();
        assert_eq!(selection.node.node_id, "node-2");
    }

    #[tokio::test]
    async fn test_geographic_selection() {
        let manager = EdgeNodeManager::new(LoadBalancingStrategy::Geographic);
        
        let node1 = EdgeNode::new("node-1".to_string(), "EU-WEST".to_string(), 100);
        let node2 = EdgeNode::new("node-2".to_string(), "US-EAST".to_string(), 100);
        
        manager.register_node(node1).await;
        manager.register_node(node2).await;
        
        // Should select EU node for EU viewer
        let selection = manager.select_by_geography("EU-WEST").await.unwrap();
        assert_eq!(selection.node.location, "EU-WEST");
    }

    #[tokio::test]
    async fn test_health_check() {
        let manager = EdgeNodeManager::new(LoadBalancingStrategy::LeastConnections);
        
        let node = EdgeNode::new("test-node".to_string(), "EU".to_string(), 100);
        manager.register_node(node).await;
        
        let report = manager.run_health_check().await;
        assert_eq!(report.healthy_nodes.len(), 1);
        assert_eq!(report.unhealthy_nodes.len(), 0);
    }
}