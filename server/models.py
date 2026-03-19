from pydantic import BaseModel, computed_field
from typing import Optional


class MediaItem(BaseModel):
    id: int
    file_path: str
    file_size_bytes: int
    title: str
    duration_seconds: float
    video_codec: str
    audio_codec: str
    width: int
    height: int
    bit_rate: int
    thumb_ready: bool
    created_at: str
    updated_at: str

    # Injected at serialization time via a helper
    base_url: str = ""

    @computed_field
    @property
    def stream_url(self) -> str:
        return f"{self.base_url}/stream/{self.id}"

    @computed_field
    @property
    def thumb_url(self) -> str:
        return f"{self.base_url}/thumb/{self.id}"

    model_config = {"populate_by_name": True}


class LibraryResponse(BaseModel):
    items: list[MediaItem]
    page: int
    page_size: int
    total: int


class CastDevice(BaseModel):
    uuid: str
    name: str
    model_name: str
    host: str
    port: int


class CastPlayRequest(BaseModel):
    device_uuid: str
    media_id: int
    current_time: float = 0.0


class CastPlayResponse(BaseModel):
    success: bool
    message: str
