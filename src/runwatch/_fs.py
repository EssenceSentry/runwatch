from __future__ import annotations

import errno
import os
import stat
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


def ensure_private_directory(path: Path) -> None:
    """Create *path* and restrict it to the current user."""

    path.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIRECTORY_MODE)
    path.chmod(PRIVATE_DIRECTORY_MODE)


def fsync_directory(path: Path) -> None:
    """Persist directory-entry changes where the platform supports it."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno not in {
                errno.EBADF,
                errno.EINVAL,
                getattr(errno, "ENOTSUP", errno.EINVAL),
            }:
                raise
    finally:
        os.close(descriptor)


def atomic_write_bytes(
    path: Path,
    data: bytes,
    *,
    preserve_mode: bool = True,
    mode: int = PRIVATE_FILE_MODE,
    before_replace: Callable[[], None] | None = None,
) -> None:
    """Atomically replace *path* with fsynced bytes and a durable directory entry."""

    path.parent.mkdir(parents=True, exist_ok=True)
    destination_mode = mode
    if preserve_mode:
        try:
            destination_mode = stat.S_IMODE(path.stat().st_mode)
        except FileNotFoundError:
            pass

    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, mode)
    try:
        os.fchmod(descriptor, destination_mode)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if before_replace is not None:
            before_replace()
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
