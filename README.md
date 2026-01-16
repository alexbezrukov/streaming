# Configuration Guide

## Environment Variables

### Server Configuration
- `HTTP_PORT` - HTTP API port (default: 3000)
- `RTMP_PORT` - RTMP ingest port (default: 1935)
- `NODE_ID` - Unique node identifier
- `NODE_TYPE` - Node type: "origin" or "edge"
- `NODE_LOCATION` - Geographic location

### Redis Configuration
- `REDIS_URL` - Redis connection URL
- `REDIS_POOL_SIZE` - Connection pool size
- `REDIS_CLUSTER` - Enable cluster mode

### CDN Configuration
- `CACHE_SIZE` - In-memory cache size
- `SEGMENT_TTL` - Segment TTL in seconds
- `ORIGIN_URL` - Origin server URL (edge nodes only)

### Streaming Configuration
- `HLS_SEGMENT_DURATION` - HLS segment duration
- `ENABLE_DASH` - Enable DASH streaming
- `ENABLE_WEBRTC` - Enable WebRTC
- `DVR_WINDOW` - DVR window in seconds

## Configuration File

You can also use a TOML configuration file:

```bash
cargo run -- --config config.toml
```

## Docker Deployment

### Origin Server
```bash
docker-compose up origin
```

### Edge Nodes
```bash
docker-compose up edge-us-east edge-eu-west edge-apac
```

### Full Stack
```bash
docker-compose up
```

## Scaling

### Horizontal Scaling
Add more edge nodes by duplicating the edge service in docker-compose.yml with different ports and locations.

### Redis Cluster
Set `REDIS_CLUSTER=true` and provide cluster endpoints in `REDIS_URL`.

## Monitoring

Access metrics at:
- http://localhost:3000/api/metrics (origin)
- http://localhost:3001/api/metrics (edge-us-east)
- http://localhost:3002/api/metrics (edge-eu-west)