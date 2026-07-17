from __future__ import annotations

import PIL.Image
import av
import logging
from pathlib import Path
import asyncio

log = logging.getLogger(__name__)

def _convert_image_to_png(input_path: Path, output_path: Path) -> bool:
    # 1. Try using Pillow (PIL)
    try:
        log.info("Attempting Pillow image conversion for %s to %s", input_path.name, output_path.name)
        with PIL.Image.open(input_path) as img:
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGBA")
            img.save(output_path, "PNG")
        log.info("Pillow image conversion successful for %s", input_path.name)
        return True
    except Exception as pil_err:
        log.warning("Pillow image conversion failed for %s: %s. Trying PyAV fallback.", input_path.name, pil_err)
        output_path.unlink(missing_ok=True)

    # 2. Try using PyAV fallback
    try:
        log.info("Starting PyAV image conversion fallback for %s to %s", input_path.name, output_path.name)
        with av.open(str(input_path)) as container:
            video_stream = next((s for s in container.streams if s.type == "video"), None)
            if not video_stream:
                raise ValueError("No video/image stream found in container")
            
            for frame in container.decode(video_stream):
                # Convert PyAV frame to PIL Image
                img = frame.to_image()
                if img.mode not in ("RGB", "RGBA"):
                    img = img.convert("RGBA")
                img.save(output_path, "PNG")
                log.info("PyAV fallback image conversion successful for %s", input_path.name)
                return True
                
        raise ValueError("No frames could be decoded from container")
    except Exception as av_err:
        log.exception("PyAV image conversion fallback failed for %s: %s", input_path.name, av_err)
        output_path.unlink(missing_ok=True)
        return False

async def convert_image_to_png_async(input_path: Path, output_path: Path) -> bool:
    """Asynchronously convert an image format to PNG using Pillow or PyAV."""
    return await asyncio.to_thread(_convert_image_to_png, input_path, output_path)
