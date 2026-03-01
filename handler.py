"""
RunPod serverless handler for GPU-accelerated video processing.
Downloads source video, re-encodes with NVENC + segments to HLS,
uploads results to R2.
"""

import os
import glob
import subprocess
import tempfile

import boto3
import requests
import runpod


def handler(event):
    """Main RunPod handler."""
    try:
        return _process(event)
    except Exception as e:
        print(f"Unhandled error: {e}")
        return {"error": str(e)}


def _process(event):
    """Process video: download, encode, upload."""
    job_input = event["input"]

    source_url = job_input["source_url"]
    audio_url = job_input.get("audio_url")
    r2_config = job_input["r2"]
    ffmpeg_args = job_input.get("ffmpeg_args", {})

    crf = str(ffmpeg_args.get("crf", 23))
    preset = ffmpeg_args.get("preset", "p4")
    keyframe_interval = str(ffmpeg_args.get("force_keyframes_interval", 2))
    segment_duration = str(ffmpeg_args.get("segment_duration", 6))

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input.mp4")
        output_dir = os.path.join(tmp_dir, "output")
        os.makedirs(output_dir)

        # 1. Download source video
        print("Downloading source video...")
        try:
            response = requests.get(source_url, stream=True, timeout=3600)
            response.raise_for_status()
            with open(input_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        except requests.RequestException as e:
            return {"error": f"Download failed: {e}"}

        size_mb = os.path.getsize(input_path) / (1024 * 1024)
        print(f"Downloaded {size_mb:.1f}MB")

        # 1b. Download separate audio if provided
        audio_path = None
        if audio_url:
            audio_path = os.path.join(tmp_dir, "audio.m4a")
            print("Downloading separate audio...")
            try:
                audio_resp = requests.get(audio_url, stream=True, timeout=3600)
                audio_resp.raise_for_status()
                with open(audio_path, "wb") as f:
                    for chunk in audio_resp.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                audio_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
                print(f"Downloaded audio {audio_size_mb:.1f}MB")
            except requests.RequestException as e:
                return {"error": f"Audio download failed: {e}"}

        # 2. Re-encode with NVENC + output HLS segments
        stream_path = os.path.join(output_dir, "stream.m3u8")
        segment_pattern = os.path.join(output_dir, "segment_%03d.m4s")

        audio_inputs = ["-i", audio_path] if audio_path else []
        # Explicit stream mapping when muxing separate audio
        map_args = ["-map", "0:v:0", "-map", "1:a:0"] if audio_path else []

        cmd = [
            "ffmpeg",
            "-hwaccel", "cuda",
            "-i", input_path,
        ] + audio_inputs + map_args + [
            "-c:v", "h264_nvenc",
            "-preset", preset,
            "-rc", "constqp",
            "-qp", crf,
            "-force_key_frames", f"expr:gte(t,n_forced*{keyframe_interval})",
            "-c:a", "aac",
            "-f", "hls",
            "-hls_time", segment_duration,
            "-hls_segment_type", "fmp4",
            "-hls_segment_filename", segment_pattern,
            "-hls_playlist_type", "vod",
            "-master_pl_name", "master.m3u8",
            "-y",
            stream_path,
        ]

        print(f"Running FFmpeg: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if result.returncode != 0:
            stderr_tail = result.stderr[-1000:] if result.stderr else "(no stderr)"
            print(f"FFmpeg stderr: {stderr_tail}")
            return {"error": f"FFmpeg failed with code {result.returncode}: {stderr_tail}"}

        print("FFmpeg complete")

        # 3. Upload output to R2
        s3 = boto3.client(
            "s3",
            endpoint_url=r2_config["endpoint"],
            aws_access_key_id=r2_config["access_key_id"],
            aws_secret_access_key=r2_config["secret_access_key"],
            region_name="auto",
        )
        bucket = r2_config["bucket"]
        prefix = r2_config["prefix"]

        # Content type mapping
        content_types = {
            ".m3u8": "application/vnd.apple.mpegurl",
            ".m4s": "video/iso.segment",
            ".mp4": "video/mp4",
        }

        output_files = []
        for filepath in sorted(glob.glob(os.path.join(output_dir, "*"))):
            filename = os.path.basename(filepath)
            key = prefix + filename
            ext = os.path.splitext(filename)[1]
            content_type = content_types.get(ext, "application/octet-stream")

            print(f"Uploading {key} ({content_type})")
            s3.upload_file(filepath, bucket, key, ExtraArgs={"ContentType": content_type})
            output_files.append(key)

        manifest_key = prefix + "master.m3u8"
        print(f"Upload complete: {len(output_files)} files, manifest: {manifest_key}")

        return {
            "manifest_key": manifest_key,
            "segment_count": len(output_files) - 1,  # exclude manifest
            "output_files": output_files,
        }


runpod.serverless.start({"handler": handler})
