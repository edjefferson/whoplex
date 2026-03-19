"""Central registry for background subprocesses — killed on server shutdown."""
import asyncio

_procs: set[asyncio.subprocess.Process] = set()


def register(proc: asyncio.subprocess.Process) -> None:
    _procs.add(proc)


def unregister(proc: asyncio.subprocess.Process) -> None:
    _procs.discard(proc)


def kill_all() -> None:
    for proc in list(_procs):
        if proc.returncode is None:  # still running
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
    _procs.clear()
