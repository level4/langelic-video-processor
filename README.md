# RunPod Video Processor

Custom RunPod serverless endpoint for GPU-accelerated video re-encoding
and HLS packaging using FFmpeg with NVENC.

## Build & Push

```bash
docker build -t your-registry/langelic-video-processor:latest .
docker push your-registry/langelic-video-processor:latest
```

## Deploy to RunPod

1. Go to https://www.runpod.io/console/serverless
2. Create new endpoint
3. Set the Docker image to your pushed image
4. Select a GPU type with NVENC support (e.g. RTX A4000, RTX 4090)
5. Set idle timeout, max workers as needed
6. Copy the endpoint ID
7. Set RUNPOD_VIDEO_ENDPOINT_ID in your .env and 1Password

## Testing

```bash
curl -X POST "https://api.runpod.ai/v2/YOUR_ENDPOINT_ID/run" \
  -H "Authorization: Bearer $RUNPOD_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "source_url": "https://example.com/test-video.mp4",
      "r2": {
        "endpoint": "https://xxx.r2.cloudflarestorage.com",
        "access_key_id": "...",
        "secret_access_key": "...",
        "bucket": "langelic",
        "prefix": "hls/test/"
      },
      "ffmpeg_args": {
        "crf": 23,
        "preset": "p4"
      }
    }
  }'
```
