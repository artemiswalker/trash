from __future__ import annotations

from .video import convert_video_async, split_video_async
from .image import convert_image_to_png_async
from .audio import convert_audio_async
from .telegram import (
    _conversion_ids,
    _conversion_events,
    _conversion_choices,
    _converted_files,
    handle_conversion_choice,
)

CONVERSION_EXT = {".ts", ".flv", ".avi", ".wmv", ".asf", ".mkv"}
AUDIO_CONVERSION_EXT = {".wav", ".flac", ".ogg", ".opus", ".aiff", ".aac"}

async def convert_media_async(input_path, output_path) -> bool:
    """Backward compatible video conversion entry point utilizing PyAV."""
    return await convert_video_async(input_path, output_path)

__all__ = [
    "_conversion_ids",
    "_conversion_events",
    "_conversion_choices",
    "_converted_files",
    "CONVERSION_EXT",
    "AUDIO_CONVERSION_EXT",
    "convert_media_async",
    "convert_image_to_png_async",
    "convert_audio_async",
    "split_video_async",
    "handle_conversion_choice",
]

