use std::collections::VecDeque;
use std::time::{Duration, Instant};
use crate::domain::VideoSegment;

/// Buffer for smoothing input video segments
pub struct SegmentBuffer {
    buffer: VecDeque<VideoSegment>,
    max_size: usize,
    max_duration: Duration,
    first_segment_time: Option<Instant>,
}

impl SegmentBuffer {
    pub fn new(max_size: usize, max_duration: Duration) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size),
            max_size,
            max_duration,
            first_segment_time: None,
        }
    }

    /// Add segment to buffer
    pub fn push(&mut self, segment: VideoSegment) -> bool {
        if self.buffer.len() >= self.max_size {
            return false;
        }

        if self.first_segment_time.is_none() {
            self.first_segment_time = Some(Instant::now());
        }

        self.buffer.push_back(segment);
        true
    }

    /// Get next segment if available
    pub fn pop(&mut self) -> Option<VideoSegment> {
        self.buffer.pop_front()
    }

    /// Check if buffer is full
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_size
    }

    /// Check if buffer has expired
    pub fn is_expired(&self) -> bool {
        if let Some(first_time) = self.first_segment_time {
            first_time.elapsed() > self.max_duration
        } else {
            false
        }
    }

    /// Get buffer fill percentage
    pub fn fill_percentage(&self) -> f64 {
        (self.buffer.len() as f64 / self.max_size as f64) * 100.0
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.first_segment_time = None;
    }
}