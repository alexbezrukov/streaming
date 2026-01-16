use bytes::Bytes;
use std::{
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{mpsc, RwLock, Semaphore},
};
use dashmap::DashMap;
use crate::{
    config::settings::{Config, TranscodingProfile},
    domain::{quality::StreamQuality, stream::VideoSegment},
};

/// Transcoder service for converting video to multiple qualities
pub struct TranscoderService {
    /// Application configuration
    config: Arc<Config>,
    
    /// Active transcoding sessions
    sessions: DashMap<String, Arc<TranscodingSession>>,
    
    /// Semaphore to limit concurrent transcoding jobs
    job_limiter: Arc<Semaphore>,
    
    /// Hardware acceleration settings
    hw_accel: HardwareAcceleration,
}

/// Hardware acceleration configuration
#[derive(Debug, Clone)]
pub struct HardwareAcceleration {
    pub enabled: bool,
    pub accel_type: AccelType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccelType {
    None,
    Nvidia,   // NVENC
    Intel,    // QSV
    Amd,      // AMF
    VideoToolbox, // macOS
}

/// A transcoding session for a stream
struct TranscodingSession {
    stream_id: String,
    processes: DashMap<StreamQuality, TranscodingProcess>,
}

/// Individual transcoding process for one quality level
struct TranscodingProcess {
    quality: StreamQuality,
    child: Arc<RwLock<Option<tokio::process::Child>>>,
    input_tx: mpsc::Sender<Bytes>,
}

/// Transcoding result
#[derive(Debug)]
pub struct TranscodedSegment {
    pub quality: StreamQuality,
    pub data: Bytes,
    pub sequence: u64,
    pub duration_ms: u32,
    pub keyframe: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TranscoderError {
    #[error("FFmpeg not found or not installed")]
    FfmpegNotFound,
    
    #[error("Failed to spawn FFmpeg process: {0}")]
    ProcessSpawnError(String),
    
    #[error("Failed to write to FFmpeg stdin: {0}")]
    WriteError(String),
    
    #[error("Failed to read from FFmpeg stdout: {0}")]
    ReadError(String),
    
    #[error("Transcoding session not found: {0}")]
    SessionNotFound(String),
    
    #[error("Invalid video data")]
    InvalidVideoData,
    
    #[error("Transcoding job limit reached")]
    JobLimitReached,
}

impl TranscoderService {
    /// Create new transcoder service
    pub fn new(config: Arc<Config>) -> Result<Arc<Self>, TranscoderError> {
        // Check if FFmpeg is installed
        Self::check_ffmpeg_installation()?;
        
        let hw_accel = Self::detect_hardware_acceleration();
        
        // Limit to 4 concurrent transcoding sessions per CPU core
        let max_jobs = num_cpus::get() * 4;
        
        Ok(Arc::new(Self {
            config,
            sessions: DashMap::new(),
            job_limiter: Arc::new(Semaphore::new(max_jobs)),
            hw_accel,
        }))
    }

    /// Check if FFmpeg is installed and accessible
    fn check_ffmpeg_installation() -> Result<(), TranscoderError> {
        let output = std::process::Command::new("ffmpeg")
            .arg("-version")
            .output();
        
        match output {
            Ok(_) => Ok(()),
            Err(_) => Err(TranscoderError::FfmpegNotFound),
        }
    }

    /// Detect available hardware acceleration
    fn detect_hardware_acceleration() -> HardwareAcceleration {
        // Check for NVIDIA GPU
        if Self::check_nvidia_support() {
            return HardwareAcceleration {
                enabled: true,
                accel_type: AccelType::Nvidia,
            };
        }

        // Check for Intel QSV
        if Self::check_intel_support() {
            return HardwareAcceleration {
                enabled: true,
                accel_type: AccelType::Intel,
            };
        }

        // Check for macOS VideoToolbox
        #[cfg(target_os = "macos")]
        {
            return HardwareAcceleration {
                enabled: true,
                accel_type: AccelType::VideoToolbox,
            };
        }

        // Fallback to software encoding
        HardwareAcceleration {
            enabled: false,
            accel_type: AccelType::None,
        }
    }

    fn check_nvidia_support() -> bool {
        std::process::Command::new("nvidia-smi")
            .output()
            .is_ok()
    }

    fn check_intel_support() -> bool {
        // Check for Intel GPU on Linux
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/dev/dri/renderD128").exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Start transcoding session for a stream
    pub async fn start_session(
        &self,
        stream_id: String,
    ) -> Result<mpsc::Sender<VideoSegment>, TranscoderError> {
        let session = Arc::new(TranscodingSession {
            stream_id: stream_id.clone(),
            processes: DashMap::new(),
        });

        // Start transcoding processes for each quality
        for profile in &self.config.streaming.profiles {
            let quality = Self::profile_to_quality(profile);
            
            let process = self.spawn_transcoding_process(
                &stream_id,
                quality,
                profile,
            ).await?;
            
            session.processes.insert(quality, process);
        }

        self.sessions.insert(stream_id.clone(), session);

        // Create input channel
        let (tx, mut rx) = mpsc::channel::<VideoSegment>(100);

        // Spawn task to distribute segments to all quality processes
        let session_clone = self.sessions.get(&stream_id)
            .ok_or_else(|| TranscoderError::SessionNotFound(stream_id.clone()))?
            .clone();
        
        tokio::spawn(async move {
            while let Some(segment) = rx.recv().await {
                // Send to all quality processes
                for entry in session_clone.processes.iter() {
                    let process = entry.value();
                    let _ = process.input_tx.send(segment.data.clone()).await;
                }
            }
        });

        Ok(tx)
    }

    /// Spawn FFmpeg process for specific quality
    async fn spawn_transcoding_process(
        &self,
        stream_id: &str,
        quality: StreamQuality,
        profile: &TranscodingProfile,
    ) -> Result<TranscodingProcess, TranscoderError> {
        // Acquire job limiter permit
        let _permit = self.job_limiter
            .acquire()
            .await
            .map_err(|_| TranscoderError::JobLimitReached)?;

        let mut cmd = Command::new("ffmpeg");
        
        // Input from stdin (raw video)
        cmd.arg("-i").arg("pipe:0");
        cmd.arg("-f").arg("h264");  // Input format

        // Hardware acceleration
        self.add_hw_accel_args(&mut cmd);

        // Video encoding settings
        cmd.arg("-c:v").arg(self.get_video_codec());
        cmd.arg("-preset").arg("veryfast");  // Balance speed vs quality
        cmd.arg("-b:v").arg(format!("{}k", profile.bitrate_kbps));
        cmd.arg("-maxrate").arg(format!("{}k", profile.bitrate_kbps * 12 / 10));
        cmd.arg("-bufsize").arg(format!("{}k", profile.bitrate_kbps * 2));
        
        // Resolution
        cmd.arg("-s").arg(format!("{}x{}", profile.width, profile.height));
        cmd.arg("-r").arg(profile.fps.to_string());

        // Keyframe interval (2 seconds)
        cmd.arg("-g").arg((profile.fps * 2).to_string());
        cmd.arg("-keyint_min").arg((profile.fps * 2).to_string());

        // Output format (FLV for streaming)
        cmd.arg("-f").arg("flv");
        cmd.arg("pipe:1");

        // Hide banner and set log level
        cmd.arg("-hide_banner");
        cmd.arg("-loglevel").arg("error");

        // Stdio configuration
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // Spawn process
        let mut child = cmd.spawn()
            .map_err(|e| TranscoderError::ProcessSpawnError(e.to_string()))?;

        let stdin = child.stdin.take()
            .ok_or_else(|| TranscoderError::ProcessSpawnError("Failed to open stdin".into()))?;
        
        let stdout = child.stdout.take()
            .ok_or_else(|| TranscoderError::ProcessSpawnError("Failed to open stdout".into()))?;

        // Create channels
        let (input_tx, mut input_rx) = mpsc::channel::<Bytes>(100);
        let child_arc = Arc::new(RwLock::new(Some(child)));

        // Spawn task to write to FFmpeg stdin
        let child_clone = child_arc.clone();
        tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(data) = input_rx.recv().await {
                if let Err(e) = stdin.write_all(&data).await {
                    tracing::error!("Failed to write to FFmpeg stdin: {}", e);
                    break;
                }
                let _ = stdin.flush().await;
            }
            
            // Close stdin when done
            drop(stdin);
            
            // Wait for process to finish
            if let Some(mut child) = child_clone.write().await.take() {
                let _ = child.wait().await;
            }
        });

        // Spawn task to read from FFmpeg stdout
        let stream_id = stream_id.to_string();
        tokio::spawn(async move {
            let mut stdout = stdout;
            let mut buffer = vec![0u8; 65536];
            let mut sequence = 0u64;
            
            while let Ok(n) = stdout.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                
                let data = Bytes::copy_from_slice(&buffer[..n]);
                
                // In production: parse transcoded segments and send to stream manager
                tracing::debug!(
                    "Transcoded segment for stream {} quality {:?}: {} bytes",
                    stream_id,
                    quality,
                    n
                );
                
                sequence += 1;
            }
        });

        Ok(TranscodingProcess {
            quality,
            child: child_arc,
            input_tx,
        })
    }

    /// Add hardware acceleration arguments
    fn add_hw_accel_args(&self, cmd: &mut Command) {
        if !self.hw_accel.enabled {
            return;
        }

        match self.hw_accel.accel_type {
            AccelType::Nvidia => {
                cmd.arg("-hwaccel").arg("cuda");
                cmd.arg("-hwaccel_output_format").arg("cuda");
            }
            AccelType::Intel => {
                cmd.arg("-hwaccel").arg("qsv");
                cmd.arg("-hwaccel_output_format").arg("qsv");
            }
            AccelType::VideoToolbox => {
                cmd.arg("-hwaccel").arg("videotoolbox");
            }
            AccelType::Amd => {
                cmd.arg("-hwaccel").arg("amf");
            }
            AccelType::None => {}
        }
    }

    /// Get video codec based on hardware acceleration
    fn get_video_codec(&self) -> &'static str {
        if !self.hw_accel.enabled {
            return "libx264";
        }

        match self.hw_accel.accel_type {
            AccelType::Nvidia => "h264_nvenc",
            AccelType::Intel => "h264_qsv",
            AccelType::VideoToolbox => "h264_videotoolbox",
            AccelType::Amd => "h264_amf",
            AccelType::None => "libx264",
        }
    }

    /// Map transcoding profile to stream quality
    fn profile_to_quality(profile: &TranscodingProfile) -> StreamQuality {
        match profile.height {
            360 => StreamQuality::Low,
            720 => StreamQuality::Medium,
            1080 => StreamQuality::High,
            2160 => StreamQuality::Ultra,
            _ => StreamQuality::Medium,
        }
    }

    /// Stop transcoding session
    pub async fn stop_session(&self, stream_id: &str) -> Result<(), TranscoderError> {
        if let Some((_, session)) = self.sessions.remove(stream_id) {
            // Stop all processes
            for entry in session.processes.iter() {
                let process = entry.value();
                if let Some(mut child) = process.child.write().await.take() {
                    let _ = child.kill().await;
                }
            }
        }
        
        Ok(())
    }

    /// Get active session count
    pub fn active_sessions(&self) -> usize {
        self.sessions.len()
    }

    /// Check if session exists
    pub fn has_session(&self, stream_id: &str) -> bool {
        self.sessions.contains_key(stream_id)
    }
}

// ============================================================================
// SIMPLE TRANSCODER (Fallback when FFmpeg is not available)
// ============================================================================

/// Simple transcoder that simulates transcoding by scaling data
/// Used when FFmpeg is not available or for testing
pub struct SimpleTranscoder;

impl SimpleTranscoder {
    pub fn transcode(data: &Bytes, quality: StreamQuality) -> Bytes {
        let scale_factor = match quality {
            StreamQuality::Low => 0.3,
            StreamQuality::Medium => 0.5,
            StreamQuality::High => 0.8,
            StreamQuality::Ultra => 1.0,
        };
        
        let new_size = (data.len() as f32 * scale_factor) as usize;
        data.slice(0..new_size.min(data.len()))
    }
}

// ============================================================================
// TRANSCODING STATISTICS
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct TranscodingStats {
    pub total_segments: u64,
    pub failed_segments: u64,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub avg_transcode_time_ms: f64,
}

impl TranscodingStats {
    pub fn compression_ratio(&self) -> f64 {
        if self.total_bytes_in == 0 {
            return 0.0;
        }
        self.total_bytes_out as f64 / self.total_bytes_in as f64
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_transcoder() {
        let data = Bytes::from(vec![0u8; 1000]);
        
        let low = SimpleTranscoder::transcode(&data, StreamQuality::Low);
        assert!(low.len() < data.len());
        
        let medium = SimpleTranscoder::transcode(&data, StreamQuality::Medium);
        assert!(low.len() < medium.len());
        
        let high = SimpleTranscoder::transcode(&data, StreamQuality::High);
        assert!(medium.len() < high.len());
    }

    #[test]
    fn test_hw_accel_detection() {
        let hw_accel = TranscoderService::detect_hardware_acceleration();
        println!("Detected hardware acceleration: {:?}", hw_accel);
    }

    #[tokio::test]
    async fn test_ffmpeg_check() {
        match TranscoderService::check_ffmpeg_installation() {
            Ok(_) => println!("FFmpeg is installed"),
            Err(e) => println!("FFmpeg not found: {}", e),
        }
    }
}