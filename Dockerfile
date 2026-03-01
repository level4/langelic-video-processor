FROM runpod/base:0.6.2-cuda12.2.0

# Install FFmpeg with NVENC support from BtbN static builds
# NOTE: The BtbN release URL format may change over time. If the build fails,
# check https://github.com/BtbN/FFmpeg-Builds/releases for the latest
# linux64-gpl variant (which includes NVENC support).
RUN apt-get update && apt-get install -y --no-install-recommends wget xz-utils && \
    wget -q https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n7.1-latest-linux64-gpl-7.1.tar.xz && \
    tar xf ffmpeg-n7.1-latest-linux64-gpl-7.1.tar.xz && \
    cp ffmpeg-n7.1-latest-linux64-gpl-7.1/bin/ffmpeg /usr/local/bin/ffmpeg && \
    cp ffmpeg-n7.1-latest-linux64-gpl-7.1/bin/ffprobe /usr/local/bin/ffprobe && \
    rm -rf ffmpeg-n7.1-latest-linux64-gpl-7.1* && \
    apt-get remove -y wget xz-utils && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir boto3 requests runpod

# Copy handler
COPY handler.py /handler.py

CMD ["python3.11", "-u", "/handler.py"]
