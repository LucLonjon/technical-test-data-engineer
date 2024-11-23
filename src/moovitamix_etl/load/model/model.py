from datetime import datetime
from typing import List
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

# Junction table for track_genres
track_genres = Table(
    'track_genres',
    Base.metadata,
    Column('track_id', Integer, ForeignKey('tracks.id'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('genres.id'), primary_key=True)
)

# Junction table for user_favorite_genres
user_favorite_genres = Table(
    'user_favorite_genres',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('genres.id'), primary_key=True)
)

class User(Base):
    """User model"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    gender = Column(String(50))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    favorite_genres = relationship("Genre", secondary=user_favorite_genres, back_populates="users_who_like")
    listen_history = relationship("ListenHistory", back_populates="user")

    def __repr__(self):
        return f"<User {self.first_name} {self.last_name}>"

class Track(Base):
    """Track model"""
    __tablename__ = 'tracks'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    artist = Column(String(255), nullable=False)
    songwriters = Column(String(255))
    duration = Column(String(50), nullable=False)
    album = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    genres = relationship("Genre", secondary=track_genres, back_populates="tracks")
    listen_history = relationship("ListenHistory", back_populates="track")

    def __repr__(self):
        return f"<Track {self.name} by {self.artist}>"

class Genre(Base):
    """Genre model"""
    __tablename__ = 'genres'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)

    # Relationships
    tracks = relationship("Track", secondary=track_genres, back_populates="genres")
    users_who_like = relationship("User", secondary=user_favorite_genres, back_populates="favorite_genres")

    def __repr__(self):
        return f"<Genre {self.name}>"

class ListenHistory(Base):
    """Listen History model"""
    __tablename__ = 'listen_history'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    track_id = Column(Integer, ForeignKey('tracks.id'), nullable=False)
    listened_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="listen_history")
    track = relationship("Track", back_populates="listen_history")

    def __repr__(self):
        return f"<ListenHistory User:{self.user_id} Track:{self.track_id}>"


