"""
FastAPI HTTP service for video encoding on OVH cloud instances.
Wraps the same FFmpeg encoding logic as the RunPod handler.
Accepts jobs via HTTP, reports status, and phones home on startup.
"""

import os
import glob
import subprocess
import tempfile
import threading
import time
import uuid

from s2_logger import S2Logger

import boto3
import requests
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional

app = FastAPI()
security = HTTPBearer()

AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "")
CALLBACK_URL = os.environ.get("CALLBACK_URL", "")
INSTANCE_ID = os.environ.get("INSTANCE_ID", "")

# Job state (single job at a time)
current_job = {"id": None, "status": "idle", "output": None, "error": None}
job_lock = threading.Lock()


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials


class JobInput(BaseModel):
    source_url: str
    audio_url: Optional[str] = None
    r2: dict
    ffmpeg_args: Optional[dict] = {}
    s2_stream: Optional[str] = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs")
def create_job(job_input: JobInput, _=Security(verify_token)):
    with job_lock:
        if current_job["status"] == "processing":
            raise HTTPException(status_code=409, detail="Instance busy")

        job_id = str(uuid.uuid4())
        current_job["id"] = job_id
        current_job["status"] = "processing"
        current_job["output"] = None
        current_job["error"] = None

    thread = threading.Thread(
        target=_run_encode,
        args=(job_id, job_input.model_dump()),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "processing"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str, _=Security(verify_token)):
    if current_job["id"] != job_id:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "status": current_job["status"],
        "output": current_job["output"],
        "error": current_job["error"],
    }


def _run_encode(job_id, job_input):
    """Run the encode in a background thread."""
    logger = S2Logger(stream=job_input.get("s2_stream"))
    try:
        logger.log(f"Starting encode job {job_id}")
        result = _process(job_input, logger)

        with job_lock:
            if "error" in result:
                logger.log(f"Job failed: {result['error']}")
                current_job["status"] = "failed"
                current_job["error"] = result["error"]
            else:
                logger.log(f"Job completed: {result.get('manifest_key')}")
                current_job["status"] = "completed"
                current_job["output"] = result
    except Exception as e:
        logger.log(f"Job exception: {e}")
        with job_lock:
            current_job["status"] = "failed"
            current_job["error"] = str(e)
    finally:
        logger.close()


def _process(job_input, logger):
    """Process video: download, encode, upload. Same logic as handler.py."""
    source_url = job_input["source_url"]
    audio_url = job_input.get("audio_url")
    r2_config = job_input["r2"]
    ffmpeg_args = job_input.get("ffmpeg_args", {})

    crf = str(ffmpeg_args.get("crf", 23))
    preset = ffmpeg_args.get("preset", "medium")
    keyframe_interval = str(ffmpeg_args.get("force_keyframes_interval", 2))
    segment_duration = str(ffmpeg_args.get("segment_duration", 6))

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input.mp4")
        output_dir = os.path.join(tmp_dir, "output")
        os.makedirs(output_dir)

        # 1. Download source video
        logger.log("Downloading source video...")
        try:
            response = requests.get(source_url, stream=True, timeout=3600)
            response.raise_for_status()
            with open(input_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        except requests.RequestException as e:
            return {"error": f"Download failed: {e}"}

        size_mb = os.path.getsize(input_path) / (1024 * 1024)
        logger.log(f"Downloaded {size_mb:.1f}MB")

        # 1b. Download separate audio if provided
        audio_path = None
        if audio_url:
            audio_path = os.path.join(tmp_dir, "audio.m4a")
            logger.log("Downloading separate audio...")
            try:
                audio_resp = requests.get(audio_url, stream=True, timeout=3600)
                audio_resp.raise_for_status()
                with open(audio_path, "wb") as f:
                    for chunk in audio_resp.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
            except requests.RequestException as e:
                return {"error": f"Audio download failed: {e}"}

        # 2. Encode with libx264 + segment to HLS
        stream_path = os.path.join(output_dir, "stream.m3u8")
        segment_pattern = os.path.join(output_dir, "segment_%03d.m4s")

        audio_inputs = ["-i", audio_path] if audio_path else []
        map_args = ["-map", "0:v:0", "-map", "1:a:0"] if audio_path else []

        hls_args = [
            "-f", "hls",
            "-hls_time", segment_duration,
            "-hls_segment_type", "fmp4",
            "-hls_segment_filename", segment_pattern,
            "-hls_playlist_type", "vod",
            "-master_pl_name", "master.m3u8",
            "-y",
            stream_path,
        ]

        cmd = [
            "ffmpeg",
            "-i", input_path,
        ] + audio_inputs + map_args + [
            "-c:v", "libx264",
            "-preset", preset,
            "-crf", crf,
            "-threads", "0",
            "-force_key_frames", f"expr:gte(t,n_forced*{keyframe_interval})",
            "-c:a", "aac",
        ] + hls_args

        logger.log(f"Running FFmpeg: {' '.join(cmd)}")

        # Insert -progress pipe:1 before the HLS output args (last 2 args: -y and stream_path)
        cmd_with_progress = cmd[:-2] + ["-progress", "pipe:1"] + cmd[-2:]

        process = subprocess.Popen(
            cmd_with_progress,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Parse FFmpeg progress output from stdout
        progress_block = {}
        for line in process.stdout:
            line = line.strip()
            if "=" in line:
                key, _, value = line.partition("=")
                progress_block[key] = value

                if key == "progress":
                    # End of a progress block
                    if "out_time" in progress_block:
                        logger.progress({
                            "time": progress_block.get("out_time", "").strip(),
                            "speed": progress_block.get("speed", "").strip(),
                            "fps": progress_block.get("fps", "").strip(),
                            "frame": progress_block.get("frame", "").strip(),
                            "total_size": progress_block.get("total_size", "").strip(),
                        })
                    progress_block = {}

        process.wait(timeout=7200)
        stderr_output = process.stderr.read()

        if process.returncode != 0:
            stderr_tail = stderr_output[-1000:] if stderr_output else "(no stderr)"
            return {"error": f"FFmpeg failed with code {process.returncode}: {stderr_tail}"}

        logger.log("FFmpeg complete")

        # 3. Upload to R2
        s3 = boto3.client(
            "s3",
            endpoint_url=r2_config["endpoint"],
            aws_access_key_id=r2_config["access_key_id"],
            aws_secret_access_key=r2_config["secret_access_key"],
            region_name="auto",
        )
        bucket = r2_config["bucket"]
        prefix = r2_config["prefix"]

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

            logger.log(f"Uploading {key} ({content_type})")
            s3.upload_file(filepath, bucket, key, ExtraArgs={"ContentType": content_type})
            output_files.append(key)

        manifest_key = prefix + "master.m3u8"
        logger.log(f"Upload complete: {len(output_files)} files, manifest: {manifest_key}")

        return {
            "manifest_key": manifest_key,
            "segment_count": len(output_files) - 1,
            "output_files": output_files,
        }


def _phone_home():
    """Phone home to the app to announce readiness."""
    if not CALLBACK_URL or not INSTANCE_ID:
        print("No callback URL or instance ID configured, skipping phone home")
        return

    # Detect our public IP
    try:
        ip = requests.get("https://ifconfig.me", timeout=10).text.strip()
    except Exception:
        ip = "unknown"

    print(f"Phoning home to {CALLBACK_URL} with IP {ip}")

    try:
        resp = requests.post(
            f"{CALLBACK_URL}/api/internal/encoder/ready",
            json={"instance_id": INSTANCE_ID, "ip": ip},
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            timeout=30,
        )
        print(f"Phone home response: {resp.status_code}")
    except Exception as e:
        print(f"Phone home failed: {e}")


def _heartbeat_loop():
    """Send heartbeats every 60 seconds."""
    while True:
        time.sleep(60)
        if not CALLBACK_URL or not INSTANCE_ID:
            continue
        try:
            requests.post(
                f"{CALLBACK_URL}/api/internal/encoder/heartbeat",
                json={"instance_id": INSTANCE_ID},
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                timeout=10,
            )
        except Exception as e:
            print(f"Heartbeat failed: {e}")


@app.on_event("startup")
def on_startup():
    # Phone home in a thread so we don't block startup
    threading.Thread(target=_phone_home, daemon=True).start()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
