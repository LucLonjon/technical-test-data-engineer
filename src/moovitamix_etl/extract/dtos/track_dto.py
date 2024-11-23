from datetime import datetime
from dataclasses import dataclass

@dataclass
class TrackDto:
    """Data Transfer Object for Track entity"""
    id: int
    name: str
    artist: str
    songwriters: str
    duration: str
    genres: str
    album: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict:
        """
        Convert the DTO to a dictionary
        """
        return {
            "id": self.id,
            "name": self.name,
            "artist": self.artist,
            "songwriters": self.songwriters,
            "duration": self.duration,
            "genres": self.genres,
            "album": self.album,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TrackDto":
        """
        Create a TrackDto instance from a dictionary
        """
        return cls(
            id=data["id"],
            name=data["name"],
            artist=data["artist"],
            songwriters=data["songwriters"],
            duration=data["duration"],
            genres=data["genres"],
            album=data["album"],
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data["created_at"], str) else data["created_at"],
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data["updated_at"], str) else data["updated_at"]
        )