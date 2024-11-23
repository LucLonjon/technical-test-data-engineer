from datetime import datetime
from dataclasses import dataclass
from typing import List

@dataclass
class ListenHistoryDto:
    """Data Transfer Object for Listen History entity"""
    user_id: int
    items: List[int]  # List of track IDs
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_entity(cls, entity) -> "ListenHistoryDto":
        """
        Create a ListenHistoryDto instance from an entity object
        """
        return cls(
            user_id=entity.user_id,
            items=entity.items,
            created_at=entity.created_at,
            updated_at=entity.updated_at
        )

    def to_dict(self) -> dict:
        """
        Convert the DTO to a dictionary
        """
        return {
            "user_id": self.user_id,
            "items": self.items,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ListenHistoryDto":
        """
        Create a ListenHistoryDto instance from a dictionary
        """
        return cls(
            user_id=data["user_id"],
            items=data["items"],
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data["created_at"], str) else data["created_at"],
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data["updated_at"], str) else data["updated_at"]
        )
