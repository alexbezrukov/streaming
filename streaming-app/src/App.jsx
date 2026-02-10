import { useState, useEffect, useRef } from "react";
import {
  Camera,
  Users,
  Radio,
  Settings,
  Play,
  Square,
  RefreshCw,
} from "lucide-react";
import Hls from "hls.js";

const API_BASE = "http://localhost:3005";
const WS_BASE = "ws://localhost:3005";

// Broadcaster Component
const Broadcaster = () => {
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamInfo, setStreamInfo] = useState(null);
  const [devices, setDevices] = useState({ cameras: [], microphones: [] });
  const [selectedCamera, setSelectedCamera] = useState("");
  const [selectedMic, setSelectedMic] = useState("");
  const [streamTitle, setStreamTitle] = useState("My Live Stream");
  const [viewers, setViewers] = useState(0);
  const [duration, setDuration] = useState(0);
  const [uploadSpeed, setUploadSpeed] = useState(0);
  const [devicePermission, setDevicePermission] = useState("prompt"); // 'prompt' | 'granted' | 'denied'

  const videoRef = useRef(null);
  const mediaStreamRef = useRef(null);
  const mediaRecorderRef = useRef(null);
  const websocketRef = useRef(null);
  const startTimeRef = useRef(null);
  const lastBytesSentRef = useRef(0);

  useEffect(() => {
    initDevices();
    return () => stopStreaming();
  }, []);

  useEffect(() => {
    if (!isStreaming) return;

    const interval = setInterval(() => {
      if (startTimeRef.current) {
        setDuration(Math.floor((Date.now() - startTimeRef.current) / 1000));
      }
    }, 1000);

    return () => clearInterval(interval);
  }, [isStreaming]);

  const initDevices = async () => {
    try {
      // Request permission first
      const tempStream = await navigator.mediaDevices.getUserMedia({
        video: true,
        audio: true,
      });

      // Now we can get device labels
      const devices = await navigator.mediaDevices.enumerateDevices();
      const cameras = devices.filter((d) => d.kind === "videoinput");
      const microphones = devices.filter((d) => d.kind === "audioinput");

      setDevices({ cameras, microphones });
      setDevicePermission("granted");

      // Set default devices if available
      if (cameras.length > 0 && !selectedCamera) {
        setSelectedCamera(cameras[0].deviceId);
      }
      if (microphones.length > 0 && !selectedMic) {
        setSelectedMic(microphones[0].deviceId);
      }

      // Stop the temporary stream
      tempStream.getTracks().forEach((track) => track.stop());

      console.log(
        `Found ${cameras.length} cameras and ${microphones.length} microphones`,
      );
    } catch (error) {
      console.error("Error getting devices:", error);
      setDevicePermission("denied");

      // Even without permission, list devices (without labels)
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        setDevices({
          cameras: devices.filter((d) => d.kind === "videoinput"),
          microphones: devices.filter((d) => d.kind === "audioinput"),
        });
      } catch (e) {
        console.error("Failed to enumerate devices:", e);
      }
    }
  };

  const startStreaming = async () => {
    try {
      // Create stream on server
      const response = await fetch(`${API_BASE}/api/streams`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          broadcaster_id: `user-${Date.now()}`,
          title: streamTitle,
        }),
      });

      if (!response.ok) throw new Error("Failed to create stream");
      const data = await response.json();
      setStreamInfo(data);

      // Get user media
      const constraints = {
        video: {
          ...(selectedCamera ? { deviceId: { exact: selectedCamera } } : {}),
          width: { ideal: 1280 },
          height: { ideal: 720 },
          frameRate: { ideal: 30 },
        },
        audio: selectedMic ? { deviceId: { exact: selectedMic } } : true,
      };

      const stream = await navigator.mediaDevices.getUserMedia(constraints);
      mediaStreamRef.current = stream;

      if (videoRef.current) {
        videoRef.current.srcObject = stream;
      }

      // Connect WebSocket for streaming
      const ws = new WebSocket(`${WS_BASE}/ws/ingest/${data.stream_id}`);
      websocketRef.current = ws;

      ws.onopen = () => {
        console.log("WebSocket connected, starting MediaRecorder...");

        // Start MediaRecorder
        const options = {
          mimeType: "video/webm;codecs=vp8,opus",
          videoBitsPerSecond: 2500000, // 2.5 Mbps
        };

        const mediaRecorder = new MediaRecorder(stream, options);
        mediaRecorderRef.current = mediaRecorder;

        let bytesSent = 0;
        const speedInterval = setInterval(() => {
          const bytesPerSecond = bytesSent - lastBytesSentRef.current;
          setUploadSpeed(Math.round((bytesPerSecond * 8) / 1000)); // kbps
          lastBytesSentRef.current = bytesSent;
        }, 1000);

        mediaRecorder.ondataavailable = (event) => {
          if (event.data.size > 0 && ws.readyState === WebSocket.OPEN) {
            ws.send(event.data);
            bytesSent += event.data.size;
          }
        };

        mediaRecorder.onerror = (error) => {
          console.error("MediaRecorder error:", error);
          clearInterval(speedInterval);
        };

        mediaRecorder.onstop = () => {
          clearInterval(speedInterval);
        };

        // Send chunks every 100ms for low latency
        mediaRecorder.start(100);
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        alert("Failed to connect to streaming server");
        stopStreaming();
      };

      ws.onclose = () => {
        console.log("WebSocket closed");
        if (isStreaming) {
          stopStreaming();
        }
      };

      setIsStreaming(true);
      startTimeRef.current = Date.now();

      // Poll viewer count
      const viewerInterval = setInterval(async () => {
        try {
          const res = await fetch(`${API_BASE}/api/streams/${data.stream_id}`);
          if (res.ok) {
            const streamData = await res.json();
            setViewers(streamData.viewers || 0);
          }
        } catch (e) {
          console.error("Error fetching viewer count:", e);
        }
      }, 20000);

      // Store interval ID for cleanup
      ws._viewerInterval = viewerInterval;
    } catch (error) {
      console.error("Failed to start streaming:", error);
      alert("Failed to start streaming: " + error.message);
    }
  };

  const stopStreaming = async () => {
    // Stop MediaRecorder
    if (
      mediaRecorderRef.current &&
      mediaRecorderRef.current.state !== "inactive"
    ) {
      mediaRecorderRef.current.stop();
      mediaRecorderRef.current = null;
    }

    // Stop media tracks
    if (mediaStreamRef.current) {
      mediaStreamRef.current.getTracks().forEach((track) => track.stop());
      mediaStreamRef.current = null;
    }

    // Close WebSocket
    if (websocketRef.current) {
      if (websocketRef.current._viewerInterval) {
        clearInterval(websocketRef.current._viewerInterval);
      }
      if (websocketRef.current.readyState === WebSocket.OPEN) {
        websocketRef.current.close();
      }
      websocketRef.current = null;
    }

    // Delete stream on server
    if (streamInfo) {
      try {
        await fetch(`${API_BASE}/api/streams/${streamInfo.stream_id}`, {
          method: "DELETE",
        });
      } catch (error) {
        console.error("Error ending stream:", error);
      }
    }

    setIsStreaming(false);
    setStreamInfo(null);
    setDuration(0);
    setViewers(0);
    setUploadSpeed(0);
    startTimeRef.current = null;
    lastBytesSentRef.current = 0;
  };

  const formatDuration = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
  };

  return (
    <div className="p-6">
      <div className="flex items-center gap-2 mb-6">
        <Radio className="text-purple-600" size={24} />
        <h2 className="text-2xl font-bold text-gray-800">Start Broadcasting</h2>
        {devicePermission === "granted" && (
          <span className="ml-auto text-sm text-green-600 flex items-center gap-1">
            <div className="w-2 h-2 bg-green-600 rounded-full"></div>
            Camera & Mic Ready
          </span>
        )}
        {devicePermission === "denied" && (
          <span className="ml-auto text-sm text-red-600 flex items-center gap-1">
            <div className="w-2 h-2 bg-red-600 rounded-full"></div>
            Permission Denied
          </span>
        )}
      </div>

      {devicePermission === "denied" && (
        <div className="mb-4 bg-red-50 border-2 border-red-200 rounded-xl p-4">
          <h3 className="font-semibold text-red-800 mb-2">
            ⚠️ Camera/Microphone Access Needed
          </h3>
          <p className="text-sm text-red-700">
            Please allow camera and microphone access in your browser settings
            to start broadcasting.
          </p>
          <button
            onClick={initDevices}
            className="mt-3 px-4 py-2 bg-red-600 text-white rounded-lg text-sm font-semibold hover:bg-red-700"
          >
            Request Permission Again
          </button>
        </div>
      )}

      {!isStreaming ? (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Stream Title
            </label>
            <input
              type="text"
              value={streamTitle}
              onChange={(e) => setStreamTitle(e.target.value)}
              className="w-full px-4 py-2 border-2 border-gray-200 rounded-lg focus:border-purple-500 focus:outline-none"
              placeholder="Enter stream title..."
            />
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Camera
            </label>
            <select
              value={selectedCamera}
              onChange={(e) => setSelectedCamera(e.target.value)}
              className="w-full px-4 py-2 border-2 border-gray-200 rounded-lg focus:border-purple-500 focus:outline-none"
            >
              <option value="">Default Camera</option>
              {devices.cameras.map((cam, i) => (
                <option key={cam.deviceId} value={cam.deviceId}>
                  {cam.label || `Camera ${i + 1}`}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Microphone
            </label>
            <select
              value={selectedMic}
              onChange={(e) => setSelectedMic(e.target.value)}
              className="w-full px-4 py-2 border-2 border-gray-200 rounded-lg focus:border-purple-500 focus:outline-none"
            >
              <option value="">Default Microphone</option>
              {devices.microphones.map((mic, i) => (
                <option key={mic.deviceId} value={mic.deviceId}>
                  {mic.label || `Microphone ${i + 1}`}
                </option>
              ))}
            </select>
          </div>
        </div>
      ) : null}

      <div
        className="relative bg-black rounded-xl overflow-hidden my-6"
        style={{ aspectRatio: "16/9" }}
      >
        <video
          ref={videoRef}
          autoPlay
          muted
          playsInline
          className="w-full h-full"
        />
        {!isStreaming && (
          <div className="absolute inset-0 flex items-center justify-center bg-gradient-to-br from-purple-900/90 to-pink-900/90">
            <div className="text-center text-white">
              <Camera size={64} className="mx-auto mb-4 opacity-70" />
              <p className="text-lg font-semibold">Camera Preview</p>
              <p className="text-sm opacity-80 mt-2">
                Start broadcasting to see preview
              </p>
            </div>
          </div>
        )}
        {isStreaming && (
          <div className="absolute top-4 right-4 flex gap-2">
            <div className="bg-red-600 text-white px-3 py-1 rounded-full text-sm font-semibold flex items-center gap-2">
              <span className="w-2 h-2 bg-white rounded-full animate-pulse"></span>
              LIVE
            </div>
          </div>
        )}
      </div>

      <div className="flex gap-3">
        {!isStreaming ? (
          <button
            onClick={startStreaming}
            className="flex-1 bg-gradient-to-r from-purple-600 to-pink-600 text-white px-6 py-3 rounded-lg font-semibold hover:shadow-lg transition-all flex items-center justify-center gap-2"
          >
            <Play size={20} />
            Start Broadcasting
          </button>
        ) : (
          <button
            onClick={stopStreaming}
            className="flex-1 bg-red-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-red-700 transition-all flex items-center justify-center gap-2"
          >
            <Square size={20} />
            Stop Broadcasting
          </button>
        )}
      </div>

      {isStreaming && streamInfo && (
        <div className="mt-6 space-y-4">
          <div className="bg-gradient-to-r from-purple-50 to-pink-50 p-4 rounded-xl border-l-4 border-purple-500">
            <h3 className="font-semibold text-gray-800 mb-2">
              Stream Information
            </h3>
            <div className="space-y-1 text-sm text-gray-600">
              <p>
                <strong>Stream ID:</strong> {streamInfo.stream_id}
              </p>
              <p>
                <strong>HLS URL:</strong> {streamInfo.hls_url}
              </p>
              <p className="text-xs mt-2 text-gray-500">
                Share the stream ID with viewers
              </p>
            </div>
          </div>

          <div className="grid grid-cols-4 gap-4">
            <div className="bg-gradient-to-br from-purple-500 to-purple-600 text-white p-4 rounded-xl text-center">
              <div className="text-3xl font-bold">{viewers}</div>
              <div className="text-sm opacity-90">Viewers</div>
            </div>
            <div className="bg-gradient-to-br from-pink-500 to-pink-600 text-white p-4 rounded-xl text-center">
              <div className="text-3xl font-bold">
                {formatDuration(duration)}
              </div>
              <div className="text-sm opacity-90">Duration</div>
            </div>
            <div className="bg-gradient-to-br from-indigo-500 to-indigo-600 text-white p-4 rounded-xl text-center">
              <div className="text-3xl font-bold">720p</div>
              <div className="text-sm opacity-90">Quality</div>
            </div>
            <div className="bg-gradient-to-br from-green-500 to-green-600 text-white p-4 rounded-xl text-center">
              <div className="text-2xl font-bold">{uploadSpeed}</div>
              <div className="text-sm opacity-90">kbps</div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Viewer Component
const Viewer = () => {
  const [streamId, setStreamId] = useState("");
  const [isWatching, setIsWatching] = useState(false);
  const [streamData, setStreamData] = useState(null);
  const [quality, setQuality] = useState("720p");
  const [buffering, setBuffering] = useState(false);

  const videoRef = useRef(null);
  const hlsRef = useRef(null);

  const startWatching = async () => {
    if (!streamId.trim()) {
      alert("Please enter a stream ID");
      return;
    }

    try {
      const response = await fetch(`${API_BASE}/api/streams/${streamId}`);
      if (!response.ok) throw new Error("Stream not found");

      const data = await response.json();
      setStreamData(data);

      const streamUrl = `${API_BASE}/hls/${streamId}/master.m3u8`;
      const video = videoRef.current;

      if (!video) return;

      setBuffering(true);

      // Safari (native HLS)
      if (video.canPlayType("application/vnd.apple.mpegurl")) {
        video.src = streamUrl;
        video.addEventListener("canplay", () => setBuffering(false), {
          once: true,
        });
        await video.play();
      }
      // Firefox / Chrome
      else if (Hls.isSupported()) {
        const hls = new Hls({
          lowLatencyMode: true,
          maxBufferLength: 10,
          maxMaxBufferLength: 20,
          liveSyncDurationCount: 3,
        });

        hls.loadSource(streamUrl);
        hls.attachMedia(video);

        hls.on(Hls.Events.MANIFEST_PARSED, async () => {
          setBuffering(false);
          try {
            await video.play();
          } catch (e) {
            console.log("Autoplay prevented, waiting for user interaction");
          }
        });

        hls.on(Hls.Events.ERROR, (event, data) => {
          console.error("HLS error:", data);
          if (data.fatal) {
            setBuffering(false);
            alert("Playback error: " + data.type);
          }
        });

        hlsRef.current = hls;
      } else {
        throw new Error("HLS is not supported in this browser");
      }

      setIsWatching(true);
    } catch (error) {
      console.error("Failed to watch stream:", error);
      setBuffering(false);
      alert(error.message);
    }
  };

  const stopWatching = () => {
    if (hlsRef.current) {
      hlsRef.current.destroy();
      hlsRef.current = null;
    }

    if (videoRef.current) {
      videoRef.current.pause();
      videoRef.current.removeAttribute("src");
      videoRef.current.load();
    }

    setIsWatching(false);
    setStreamData(null);
    setBuffering(false);
  };

  useEffect(() => {
    return () => {
      if (hlsRef.current) {
        hlsRef.current.destroy();
      }
    };
  }, []);

  return (
    <div className="p-6">
      <div className="flex items-center gap-2 mb-6">
        <Play className="text-purple-600" size={24} />
        <h2 className="text-2xl font-bold text-gray-800">Watch Stream</h2>
      </div>

      <div className="mb-4">
        <label className="block text-sm font-semibold text-gray-700 mb-2">
          Stream ID
        </label>
        <input
          type="text"
          value={streamId}
          onChange={(e) => setStreamId(e.target.value)}
          disabled={isWatching}
          className="w-full px-4 py-2 border-2 border-gray-200 rounded-lg focus:border-purple-500 focus:outline-none disabled:bg-gray-100"
          placeholder="Enter stream ID..."
        />
      </div>

      <div
        className="relative bg-black rounded-xl overflow-hidden mb-4"
        style={{ aspectRatio: "16/9" }}
      >
        <video ref={videoRef} controls playsInline className="w-full h-full" />

        {buffering && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/50">
            <div className="text-white text-center">
              <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-white border-t-transparent mb-2"></div>
              <p>Loading stream...</p>
            </div>
          </div>
        )}

        {!isWatching && !buffering && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/80">
            <div className="text-center text-white">
              <Users size={64} className="mx-auto mb-4 opacity-70" />
              <p className="text-lg font-semibold">No Stream Selected</p>
              <p className="text-sm opacity-80 mt-2">
                Enter stream ID to start watching
              </p>
            </div>
          </div>
        )}
      </div>

      <div className="flex gap-3">
        {!isWatching ? (
          <button
            onClick={startWatching}
            disabled={buffering}
            className="flex-1 bg-purple-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-purple-700 disabled:bg-gray-400 transition-all"
          >
            {buffering ? "Connecting..." : "Watch Stream"}
          </button>
        ) : (
          <button
            onClick={stopWatching}
            className="flex-1 bg-red-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-red-700 transition-all"
          >
            Stop Watching
          </button>
        )}
      </div>

      {isWatching && streamData && (
        <div className="mt-6 grid grid-cols-3 gap-4">
          <div className="bg-green-500 text-white p-4 rounded-xl text-center">
            <div className="text-2xl font-bold">LIVE</div>
            <div className="text-sm">Status</div>
          </div>
          <div className="bg-blue-500 text-white p-4 rounded-xl text-center">
            <div className="text-3xl font-bold">{streamData.viewers}</div>
            <div className="text-sm">Viewers</div>
          </div>
          <div className="bg-purple-500 text-white p-4 rounded-xl text-center">
            <div className="text-3xl font-bold">{quality}</div>
            <div className="text-sm">Quality</div>
          </div>
        </div>
      )}
    </div>
  );
};

// Streams List Component
const StreamsList = () => {
  const [streams, setStreams] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchStreams();
    const interval = setInterval(fetchStreams, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchStreams = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE}/api/streams`);
      const data = await response.json();
      setStreams(data);
    } catch (error) {
      console.error("Failed to fetch streams:", error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <Camera className="text-purple-600" size={24} />
          <h2 className="text-2xl font-bold text-gray-800">Live Streams</h2>
        </div>
        <button
          onClick={fetchStreams}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-all"
        >
          <RefreshCw size={16} />
          Refresh
        </button>
      </div>

      {loading ? (
        <div className="text-center py-12">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-purple-200 border-t-purple-600"></div>
          <p className="mt-4 text-gray-600">Loading streams...</p>
        </div>
      ) : streams.length === 0 ? (
        <div className="bg-purple-50 border-2 border-purple-200 rounded-xl p-8 text-center">
          <Camera size={48} className="mx-auto text-purple-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-800 mb-2">
            No Live Streams
          </h3>
          <p className="text-gray-600">
            Start broadcasting to see your stream here!
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {streams.map((stream) => (
            <div
              key={stream.stream_id}
              className="bg-white border-2 border-gray-200 rounded-xl p-4 hover:border-purple-500 hover:shadow-lg transition-all cursor-pointer"
            >
              <div className="flex items-center gap-2 mb-3">
                <div className="w-3 h-3 rounded-full bg-red-500 animate-pulse"></div>
                <h3 className="font-semibold text-gray-800 flex-1 truncate">
                  {stream.title}
                </h3>
              </div>
              <div className="space-y-2 text-sm text-gray-600">
                <div className="flex items-center gap-2">
                  <Users size={16} />
                  <span>{stream.viewers} viewers</span>
                </div>
                <div className="flex items-center gap-2">
                  <Settings size={16} />
                  <span>{stream.resolution || "720p"}</span>
                </div>
                <div className="mt-3 p-2 bg-gray-50 rounded text-xs font-mono truncate">
                  {stream.stream_id}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

// Main App
export default function App() {
  const [activeTab, setActiveTab] = useState("broadcast");

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-100 via-pink-50 to-blue-50 p-4">
      <div className="max-w-6xl mx-auto">
        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          <div className="flex border-b-2 border-gray-200">
            {[
              { id: "broadcast", label: "📹 Broadcast", icon: Radio },
              { id: "watch", label: "👁️ Watch", icon: Play },
              { id: "streams", label: "📺 Streams", icon: Camera },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex-1 py-4 px-6 font-semibold transition-all ${
                  activeTab === tab.id
                    ? "bg-white text-purple-600 border-b-4 border-purple-600"
                    : "bg-gray-50 text-gray-600 hover:bg-gray-100"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {activeTab === "broadcast" && <Broadcaster />}
          {activeTab === "watch" && <Viewer />}
          {activeTab === "streams" && <StreamsList />}
        </div>
      </div>
    </div>
  );
}
