from __future__ import annotations

import os
import av
import logging
import tempfile
from pathlib import Path
import asyncio
from pedalboard.io import AudioFile
from pedalboard import Pedalboard, HighpassFilter, Compressor

log = logging.getLogger(__name__)

def _transcode_to_wav_pyav(input_path: Path, temp_wav_path: Path) -> None:
    """Fallback transcoder using PyAV to convert unsupported audio formats to WAV."""
    log.info("Decoding audio using PyAV to WAV for %s", input_path.name)
    with av.open(str(input_path)) as input_container, av.open(str(temp_wav_path), mode="w", format="wav") as output_container:
        in_audio = next((s for s in input_container.streams if s.type == "audio"), None)
        if not in_audio:
            raise ValueError("No audio stream found in file")

        # WAV container standard codec is pcm_s16le
        out_audio = output_container.add_stream("pcm_s16le", rate=in_audio.rate or 44100)
        if in_audio.channels:
            out_audio.channels = in_audio.channels
        if in_audio.layout:
            out_audio.layout = in_audio.layout

        for frame in input_container.decode(in_audio):
            frame.pts = None
            frame.time_base = None
            for packet in out_audio.encode(frame):
                output_container.mux(packet)

        for packet in out_audio.encode():
            output_container.mux(packet)

def _convert_audio(input_path: Path, output_path: Path) -> bool:
    temp_wav = None
    try:
        source_path = input_path
        
        # 1. Try reading with Pedalboard first. If it fails, transcode to WAV first.
        try:
            with AudioFile(str(source_path)) as f:
                # Just probe opening to see if it succeeds
                _ = f.samplerate
        except Exception as pedal_read_err:
            log.warning("Pedalboard failed to read audio natively: %s. Falling back to PyAV decoding.", pedal_read_err)
            # Create a temporary WAV file in the same directory (or system temp)
            temp_fd, temp_path_str = tempfile.mkstemp(suffix=".wav", dir=str(input_path.parent))
            os.close(temp_fd)
            temp_wav = Path(temp_path_str)
            
            # Transcode input to temp WAV
            _transcode_to_wav_pyav(input_path, temp_wav)
            source_path = temp_wav

        # 2. Process and convert to MP3 using Pedalboard
        log.info("Processing and converting audio %s to %s using Pedalboard", source_path.name, output_path.name)
        with AudioFile(str(source_path)) as f:
            audio = f.read(f.frames)
            samplerate = f.samplerate
            num_channels = f.num_channels

        # Apply a simple audio mastering chain:
        # HighpassFilter cuts subsonic low frequency rumble below 30Hz.
        # Compressor evens out levels (moderate threshold/ratio).
        board = Pedalboard([
            HighpassFilter(cutoff_frequency_hz=30),
            Compressor(threshold_db=-12, ratio=2.0),
        ])
        
        effected_audio = board(audio, samplerate)

        # Write out to MP3 format
        with AudioFile(str(output_path), "w", samplerate, num_channels=num_channels) as f:
            f.write(effected_audio)

        log.info("Audio conversion successful: %s", output_path.name)
        return True

    except Exception as e:
        log.exception("Pedalboard/PyAV audio conversion failed for %s: %s", input_path.name, e)
        output_path.unlink(missing_ok=True)
        return False
    finally:
        if temp_wav and temp_wav.exists():
            try:
                temp_wav.unlink(missing_ok=True)
            except Exception:
                pass

async def convert_audio_async(input_path: Path, output_path: Path) -> bool:
    """Asynchronously convert audio file to MP3 using Pedalboard (with PyAV fallback)."""
    return await asyncio.to_thread(_convert_audio, input_path, output_path)
