from datetime import datetime
from dataclasses import dataclass

@dataclass
class UserDto:
    """Data Transfer Object for User entity"""
    id: int
    first_name: str
    last_name: str
    email: str
    gender: str
    favorite_genres: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict:
        """
        Convert the DTO to a dictionary
        """
        return {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "gender": self.gender,
            "favorite_genres": self.favorite_genres,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict) -> "UserDto":
        """
        Create a UserDto instance from a dictionary
        """
        return cls(
            id=data["id"],
            first_name=data["first_name"],
            last_name=data["last_name"],
            email=data["email"],
            gender=data["gender"],
            favorite_genres=data["favorite_genres"],
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data["created_at"], str) else data["created_at"],
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data["updated_at"], str) else data["updated_at"]
        )
