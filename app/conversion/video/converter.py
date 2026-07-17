from __future__ import annotations

import av
import logging
from pathlib import Path
import asyncio

log = logging.getLogger(__name__)

def _remux_or_transcode(input_path: Path, output_path: Path) -> bool:
    # 1. Try remuxing (fast stream copy)
    try:
        log.info("Attempting PyAV fast stream copy (remuxing) for %s to %s", input_path.name, output_path.name)
        with av.open(str(input_path)) as input_container, av.open(str(output_path), mode="w") as output_container:
            streams_map = {}
            for stream in input_container.streams:
                if stream.type in ("video", "audio"):
                    try:
                        try:
                            out_stream = output_container.add_stream_from_template(stream)
                        except (AttributeError, TypeError):
                            out_stream = output_container.add_stream(template=stream)
                        out_stream.time_base = stream.time_base
                        streams_map[stream.index] = out_stream
                    except Exception as e:
                        log.warning("Could not copy stream %s: %s", stream, e)
            
            if not streams_map:
                raise ValueError("No copyable video/audio streams found")

            for packet in input_container.demux():
                if packet.stream.index not in streams_map:
                    continue
                if packet.dts is None:
                    continue
                # Assign to output stream
                packet.stream = streams_map[packet.stream.index]
                output_container.mux(packet)
                
        log.info("Fast stream copy (remuxing) successful for %s", input_path.name)
        return True
    except Exception as remux_err:
        log.warning("Fast stream copy failed for %s: %s. Falling back to full transcoding.", input_path.name, remux_err)
        output_path.unlink(missing_ok=True)

    # 2. Try transcoding fallback (H.264 + AAC)
    try:
        log.info("Starting PyAV transcoding fallback for %s to %s", input_path.name, output_path.name)
        with av.open(str(input_path)) as input_container, av.open(str(output_path), mode="w") as output_container:
            in_video = next((s for s in input_container.streams if s.type == "video"), None)
            in_audio = next((s for s in input_container.streams if s.type == "audio"), None)

            out_video = None
            out_audio = None

            if in_video:
                # Add libx264 video stream
                # Default to 30 fps if rate is not set
                fps = in_video.average_rate if in_video.average_rate else 30
                out_video = output_container.add_stream("libx264", rate=fps)
                out_video.width = in_video.width
                out_video.height = in_video.height
                out_video.pix_fmt = "yuv420p"
                out_video.options = {"preset": "superfast", "crf": "18"}

            if in_audio:
                # Add AAC audio stream
                rate = in_audio.rate if in_audio.rate else 44100
                out_audio = output_container.add_stream("aac", rate=rate)
                if in_audio.channels:
                    out_audio.channels = in_audio.channels
                if in_audio.layout:
                    out_audio.layout = in_audio.layout

            if not out_video and not out_audio:
                raise ValueError("No video or audio stream to transcode")

            # Decode and encode frame-by-frame
            for frame in input_container.decode():
                if isinstance(frame, av.VideoFrame) and out_video:
                    # Let encoder automatically compute timestamps
                    frame.pts = None
                    frame.time_base = None
                    for packet in out_video.encode(frame):
                        output_container.mux(packet)
                elif isinstance(frame, av.AudioFrame) and out_audio:
                    frame.pts = None
                    frame.time_base = None
                    for packet in out_audio.encode(frame):
                        output_container.mux(packet)

            # Flush encoders
            if out_video:
                for packet in out_video.encode():
                    output_container.mux(packet)
            if out_audio:
                for packet in out_audio.encode():
                    output_container.mux(packet)

        log.info("Transcoding successful for %s", input_path.name)
        return True
    except Exception as trans_err:
        log.exception("Transcoding failed for %s: %s", input_path.name, trans_err)
        output_path.unlink(missing_ok=True)
        return False

async def convert_video_async(input_path: Path, output_path: Path) -> bool:
    """Asynchronously convert video to MP4 container using PyAV."""
    return await asyncio.to_thread(_remux_or_transcode, input_path, output_path)

def _split_video_pyav_sync(video_path: Path, max_size_bytes: int) -> list[Path]:
    log.info("Splitting video %s using PyAV segmenter", video_path.name)
    parts: list[Path] = []
    
    try:
        input_container = av.open(str(video_path))
    except Exception as e:
        log.exception("Failed to open video for splitting with PyAV: %s", e)
        return []

    # Find video stream to decide keyframes
    video_stream = next((s for s in input_container.streams if s.type == "video"), None)
    if not video_stream:
        input_container.close()
        log.warning("No video stream found for PyAV segmenter. Falling back.")
        return []

    part_num = 1
    current_segment_size = 0
    target_segment_size = int(max_size_bytes * 0.95)
    
    output_container = None
    streams_map = {}
    
    segment_start_time_seconds = None

    def open_next_segment():
        nonlocal part_num, output_container, streams_map, current_segment_size, segment_start_time_seconds
        if output_container:
            output_container.close()
            
        part_path = video_path.parent / f"{video_path.stem}_part{part_num:03d}{video_path.suffix}"
        parts.append(part_path)
        
        output_container = av.open(str(part_path), mode="w")
        streams_map = {}
        for stream in input_container.streams:
            if stream.type in ("video", "audio"):
                try:
                    try:
                        out_stream = output_container.add_stream_from_template(stream)
                    except (AttributeError, TypeError):
                        out_stream = output_container.add_stream(template=stream)
                    out_stream.time_base = stream.time_base
                    streams_map[stream.index] = out_stream
                except Exception as e:
                    log.warning("Could not add stream template: %s", e)
                    
        part_num += 1
        current_segment_size = 0
        segment_start_time_seconds = None

    try:
        open_next_segment()
        
        for packet in input_container.demux():
            if packet.stream.index not in streams_map:
                continue
            if packet.dts is None:
                continue
            
            # Check if we should split before writing a new keyframe package
            if (packet.stream.type == "video" 
                and packet.is_keyframe 
                and current_segment_size >= target_segment_size):
                log.info("Splitting at keyframe PTS %s, current segment size %s MB", packet.pts, current_segment_size / (1024 * 1024))
                open_next_segment()
                
            out_stream = streams_map[packet.stream.index]
            
            # Sync start of segment to 0
            if segment_start_time_seconds is None:
                if packet.pts is not None:
                    segment_start_time_seconds = float(packet.pts * packet.stream.time_base)
                else:
                    segment_start_time_seconds = 0.0

            if packet.pts is not None:
                packet_time = float(packet.pts * packet.stream.time_base)
                packet.pts = int((packet_time - segment_start_time_seconds) / packet.stream.time_base)
            if packet.dts is not None:
                packet_dts = float(packet.dts * packet.stream.time_base)
                packet.dts = int((packet_dts - segment_start_time_seconds) / packet.stream.time_base)
            
            packet.stream = out_stream
            output_container.mux(packet)
            
            # Update accumulated size
            current_segment_size += packet.size
            
        if output_container:
            output_container.close()
            
    except Exception as e:
        log.exception("Error during PyAV video splitting: %s", e)
        if output_container:
            try:
                output_container.close()
            except Exception:
                pass
        for p in parts:
            p.unlink(missing_ok=True)
        return []
    finally:
        input_container.close()
        
    return parts

async def split_video_async(video_path: Path, max_size_bytes: int) -> list[Path]:
    """Asynchronously split a video using PyAV stream copier at keyframe boundaries."""
    return await asyncio.to_thread(_split_video_pyav_sync, video_path, max_size_bytes)

