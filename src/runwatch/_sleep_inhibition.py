"""Platform sleep inhibitors held for the lifetime of active notebook work."""

from __future__ import annotations

import asyncio
import ctypes
import shutil
import sys
from typing import Protocol


class SleepInhibitor(Protocol):
    @property
    def backend(self) -> str:
        """Return the operating-system backend name."""

        ...

    @property
    def active(self) -> bool:
        """Return whether sleep is currently inhibited."""

        ...

    async def acquire(self) -> None:
        """Prevent idle system sleep until ``release`` is called."""

        ...

    async def release(self) -> None:
        """Release the active sleep inhibitor, if any."""

        ...


_CreateCFString = ctypes.CFUNCTYPE(
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_uint32,
)
_ReleaseCFObject = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
_CreateIOPMAssertion = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ctypes.c_void_p,
    ctypes.c_uint32,
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint32),
)
_ReleaseIOPMAssertion = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_uint32)

_K_CF_STRING_ENCODING_UTF8 = 0x08000100
_K_IOPM_ASSERTION_LEVEL_ON = 255
_K_IO_SUCCESS = 0


class _IOKitBindings:
    def __init__(self) -> None:
        core_foundation = ctypes.CDLL(
            "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
        )
        iokit = ctypes.CDLL("/System/Library/Frameworks/IOKit.framework/IOKit")
        self._libraries = (core_foundation, iokit)
        self.create_cf_string = _CreateCFString(
            ("CFStringCreateWithCString", core_foundation)
        )
        self.release_cf_object = _ReleaseCFObject(("CFRelease", core_foundation))
        self.create_assertion = _CreateIOPMAssertion(
            ("IOPMAssertionCreateWithName", iokit)
        )
        self.release_assertion = _ReleaseIOPMAssertion(("IOPMAssertionRelease", iokit))


class _IOKitSleepInhibitor:
    def __init__(self, bindings: _IOKitBindings | None = None) -> None:
        self._bindings = bindings or _IOKitBindings()
        self._assertion_id: int | None = None

    @property
    def backend(self) -> str:
        return "iokit"

    @property
    def active(self) -> bool:
        return self._assertion_id is not None

    async def acquire(self) -> None:
        if self.active:
            return
        assertion_type = self._create_cf_string(b"NoIdleSleepAssertion")
        reason = self._create_cf_string(b"Runwatch notebook execution")
        assertion_id = ctypes.c_uint32()
        try:
            result = self._bindings.create_assertion(
                assertion_type,
                _K_IOPM_ASSERTION_LEVEL_ON,
                reason,
                ctypes.byref(assertion_id),
            )
        finally:
            self._bindings.release_cf_object(reason)
            self._bindings.release_cf_object(assertion_type)
        if result != _K_IO_SUCCESS:
            raise RuntimeError(f"IOKit sleep assertion failed with code {result}")
        self._assertion_id = int(assertion_id.value)

    def _create_cf_string(self, value: bytes) -> int:
        result = self._bindings.create_cf_string(
            None, value, _K_CF_STRING_ENCODING_UTF8
        )
        if result is None:
            raise RuntimeError(
                "CoreFoundation could not create a sleep assertion string"
            )
        return int(result)

    async def release(self) -> None:
        assertion_id = self._assertion_id
        if assertion_id is None:
            return
        result = self._bindings.release_assertion(assertion_id)
        if result != _K_IO_SUCCESS:
            raise RuntimeError(
                f"IOKit sleep assertion release failed with code {result}"
            )
        self._assertion_id = None


class _LogindSleepInhibitor:
    def __init__(self, binary: str | None = None) -> None:
        resolved_binary = binary or shutil.which("systemd-inhibit")
        if resolved_binary is None:
            raise RuntimeError(
                "systemd-inhibit was not found; a logind sleep inhibitor is unavailable"
            )
        self._binary = resolved_binary
        self._process: asyncio.subprocess.Process | None = None

    @property
    def backend(self) -> str:
        return "logind"

    @property
    def active(self) -> bool:
        return self._process is not None and self._process.returncode is None

    async def acquire(self) -> None:
        if self.active:
            return
        process = await asyncio.create_subprocess_exec(
            self._binary,
            "--what=idle:sleep",
            "--who=Runwatch",
            "--why=Runwatch notebook execution",
            "--mode=block",
            "/bin/cat",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._process = process
        try:
            return_code = await asyncio.wait_for(process.wait(), timeout=0.1)
        except TimeoutError:
            return
        error = await self._read_startup_error(process)
        self._process = None
        detail = f": {error}" if error else ""
        raise RuntimeError(
            f"logind sleep inhibitor exited with code {return_code}{detail}"
        )

    @staticmethod
    async def _read_startup_error(process: asyncio.subprocess.Process) -> str:
        if process.stderr is None:
            return ""
        payload = await process.stderr.read(4096)
        return payload.decode("utf-8", errors="replace").strip()

    async def release(self) -> None:
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            process.stdin.close()
            try:
                await process.stdin.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except TimeoutError:
                process.kill()
                await process.wait()
        self._process = None
        if process.returncode not in {0, -15}:
            raise RuntimeError(
                "logind sleep inhibitor exited unexpectedly with code "
                f"{process.returncode}"
            )


def create_sleep_inhibitor() -> SleepInhibitor:
    if sys.platform == "darwin":
        return _IOKitSleepInhibitor()
    if sys.platform.startswith("linux"):
        return _LogindSleepInhibitor()
    raise RuntimeError(
        f"System sleep inhibition is unsupported on platform {sys.platform!r}"
    )
