FROM ubuntu:latest

# Avoid prompt dialogs during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, media tools, torrent client, and archive extraction libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ffmpeg \
    aria2 \
    unzip \
    unrar \
    p7zip-full \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app ./app

RUN useradd -m botuser && mkdir -p /app/data /app/logs && chown -R botuser:botuser /app
USER botuser

VOLUME ["/app/data", "/app/logs"]

ENTRYPOINT ["python3", "-m", "app.bot"]
