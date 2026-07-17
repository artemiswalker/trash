from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Common ignored file names (exact match, lowercase)
IGNORED_FILENAMES = {
    ".ds_store",
    "thumbs.db",
    "desktop.ini",
}

# Common ignored directory names (lowercase)
IGNORED_DIRNAMES = {
    "__macosx",
    ".git",
    ".svn",
    ".hg",
    "$recycle.bin",
}

# Common temporary / partial download extensions (lowercase)
IGNORED_EXTENSIONS = {
    ".part",
    ".crdownload",
    ".torrent",
}

def should_ignore_file(path: Path) -> bool:
    """
    Determines if a file or directory path should be ignored during bot operations.
    Excludes:
    - OS metadata (macOS resource forks starting with ._, .DS_Store, Windows Thumbs.db, desktop.ini)
    - Version control system folders (.git, .svn, .hg)
    - Temporary download markers (.part, .crdownload, .torrent)
    - Empty files (0 bytes) to prevent empty upload errors
    """
    try:
        name_lower = path.name.lower()
        
        # 1. Exact ignored filenames
        if name_lower in IGNORED_FILENAMES:
            return True
            
        # 2. Hidden resource forks
        if name_lower.startswith("._"):
            return True
            
        # 3. Temp/partial download extensions
        if path.suffix.lower() in IGNORED_EXTENSIONS:
            return True
            
        # 4. Ignored directories in any part of the path
        for part in path.parts:
            part_lower = part.lower()
            if part_lower in IGNORED_DIRNAMES or part_lower.startswith("__macosx"):
                return True
                
        # 5. Empty files
        if path.is_file() and path.stat().st_size == 0:
            log.debug("Ignoring empty file (0 bytes): %s", path.name)
            return True
            
    except Exception as e:
        log.warning("Error evaluating ignore filter for %s: %s", path, e)
        
    return False
