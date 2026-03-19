import asyncio
from typing import Optional

import pychromecast
from pychromecast import Chromecast

# Module-level cache to avoid re-scanning on every request
_device_cache: dict[str, Chromecast] = {}


async def discover_devices(timeout: float = 5.0) -> list[dict]:
    """Discover Chromecasts on the LAN. Returns a list of device info dicts."""

    def _scan():
        chromecasts, browser = pychromecast.get_chromecasts(timeout=timeout)
        pychromecast.discovery.stop_discovery(browser)
        return chromecasts

    chromecasts = await asyncio.to_thread(_scan)

    devices = []
    for cc in chromecasts:
        uuid = str(cc.uuid)
        _device_cache[uuid] = cc
        devices.append(
            {
                "uuid": uuid,
                "name": cc.name,
                "model_name": cc.model_name,
                "host": cc.host,
                "port": cc.port,
            }
        )
    return devices


async def cast_url(
    device_uuid: str,
    url: str,
    content_type: str = "video/mp4",
    title: str = "",
    thumb_url: Optional[str] = None,
    current_time: float = 0.0,
) -> None:
    """
    Cast a URL to a Chromecast device.

    Note: The Default Media Receiver only supports MP4/H.264+AAC natively.
    MKV containers and HEVC video will fail unless a custom receiver is used.
    """
    cc = _device_cache.get(device_uuid)
    if cc is None:
        raise ValueError(f"Device {device_uuid} not found. Run discover first.")

    def _cast():
        cc.wait()
        mc = cc.media_controller
        mc.play_media(
            url,
            content_type,
            title=title or "",
            thumb=thumb_url or "",
            current_time=current_time,
        )
        mc.block_until_active(timeout=10)

    await asyncio.to_thread(_cast)
