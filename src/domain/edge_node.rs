use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNode {
    pub node_id: String,
    pub location: String,
    pub capacity: usize,
    pub current_load: usize,
    pub last_heartbeat: SystemTime,
}

impl EdgeNode {
    pub fn new(node_id: String, location: String, capacity: usize) -> Self {
        Self {
            node_id,
            location,
            capacity,
            current_load: 0,
            last_heartbeat: SystemTime::now(),
        }
    }

    pub fn update_load(&mut self, load: usize) {
        self.current_load = load;
        self.last_heartbeat = SystemTime::now();
    }

    pub fn is_available(&self) -> bool {
        self.current_load < self.capacity
    }

    pub fn load_percentage(&self) -> f64 {
        if self.capacity == 0 {
            return 100.0;
        }
        (self.current_load as f64 / self.capacity as f64) * 100.0
    }
}