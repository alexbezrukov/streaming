use std::sync::Arc;
use crate::{config::settings::Config, manager::stream_manager::StreamManager};

/// Global application state shared across all handlers
#[derive(Clone)]
pub struct AppState {
    /// Main stream manager
    pub stream_manager: Arc<StreamManager>,
    
    /// Application configuration
    pub config: Arc<Config>,
}

impl AppState {
    /// Create new application state
    pub fn new(stream_manager: Arc<StreamManager>, config: Config) -> Self {
        Self {
            stream_manager,
            config: Arc::new(config),
        }
    }
}