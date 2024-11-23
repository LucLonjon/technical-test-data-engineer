import pandas as pd
from typing import List, Tuple
from src.moovitamix_etl.load.model.model import Genre, Track, User, ListenHistory
from moovitamix_etl.extract.dtos.track_dto import TrackDto
from moovitamix_etl.extract.dtos.user_dto import UserDto
from moovitamix_etl.extract.dtos.listen_history import ListenHistoryDto


class DataTransformer:
    """Transform DTOs to database models using pandas for efficiency"""
    
    def __init__(self):
        self.genres_map = {}
        
    def create_genres_list(self, tracks_dtos: List[TrackDto], users_dtos: List[UserDto]) -> List[Genre]:
        """Create genres map using pandas for efficient processing"""
        
        # Convert tracks genres to DataFrame
        tracks_genres = pd.DataFrame([
            {'genres': track.genres}
            for track in tracks_dtos
            if track.genres
        ])
        
        # Convert users favorite genres to DataFrame
        users_genres = pd.DataFrame([
            {'genres': user.favorite_genres}
            for user in users_dtos
            if user.favorite_genres
        ])
        
        # Combine all genres
        all_genres = pd.concat([
            tracks_genres['genres'],
            users_genres['genres']
        ])
        
        # Split genres strings and explode to separate rows
        genres_split = all_genres.str.split(',').explode()
        
        # Clean and get unique genres
        unique_genres = (
            genres_split
            .str.strip()
            .dropna()
            .drop_duplicates()
            .sort_values()
        )
        
        # Create Genre objects
        self.genres = [
            Genre(name=genre_name)
            for genre_name in unique_genres
        ]
        
        # Create genres list
        self.genres_map = {
            genre.name: genre 
            for genre in self.genres
        }
        
        return self.genres
    
    def transform_tracks(self, tracks_dto: List[TrackDto]) -> List[Track]:
        """Transform track DTOs using pandas"""
        
        # Convert DTOs to DataFrame
        tracks_df = pd.DataFrame([
            {
                'id': t.id,
                'name': t.name,
                'artist': t.artist,
                'songwriters': t.songwriters,
                'duration': t.duration,
                'album': t.album,
                'genres': t.genres,
                'created_at': t.created_at,
                'updated_at': t.updated_at
            }
            for t in tracks_dto
        ])
        
        # Process genres
        def get_track_genres(genres_str):
            if not genres_str:
                return []
            genre_names = [g.strip() for g in genres_str.split(',')]
            return [self.genres_map[name] for name in genre_names if name in self.genres_map]
        
        # Create Track objects efficiently
        tracks = [
            Track(
                id=row['id'],
                name=row['name'],
                artist=row['artist'],
                songwriters=row['songwriters'],
                duration=row['duration'],
                album=row['album'],
                genres=get_track_genres(row['genres']),
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
            for _, row in tracks_df.iterrows()
        ]
        
        return tracks
    
    def transform_users(self, users_dto: List[UserDto]) -> List[User]:
        """Transform user DTOs using pandas"""
        
        # Convert DTOs to DataFrame
        users_df = pd.DataFrame([
            {
                'id': u.id,
                'first_name': u.first_name,
                'last_name': u.last_name,
                'email': u.email,
                'gender': u.gender,
                'favorite_genres': u.favorite_genres,
                'created_at': u.created_at,
                'updated_at': u.updated_at
            }
            for u in users_dto
        ])
        
        # Process favorite genres
        def get_user_genres(genres_str):
            if not genres_str:
                return []
            genre_names = [g.strip() for g in genres_str.split(',')]
            return [self.genres_map[name] for name in genre_names if name in self.genres_map]
        
        # Create User objects efficiently
        users = [
            User(
                id=row['id'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                email=row['email'],
                gender=row['gender'],
                favorite_genres=get_user_genres(row['favorite_genres']),
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
            for _, row in users_df.iterrows()
        ]
        
        return users
    
    def transform_listen_history(self, history_dto: List[ListenHistoryDto]) -> List[ListenHistory]:
        """Transform listen history DTOs using pandas"""
        
        # Create DataFrame from all listen history items
        history_data = []
        for h in history_dto:
            for track_id in h.items:
                history_data.append({
                    'user_id': h.user_id,
                    'track_id': track_id,
                    'listened_at': h.created_at
                })
        
        history_df = pd.DataFrame(history_data)
        
        # Create ListenHistory objects efficiently
        listen_history = [
            ListenHistory(
                user_id=row['user_id'],
                track_id=row['track_id'],
                listened_at=row['listened_at']
            )
            for _, row in history_df.iterrows()
        ]
        
        return listen_history
    
    def transform_all(
        self,
        tracks_dto: List[TrackDto],
        users_dto: List[UserDto],
        listen_history_dto: List[ListenHistoryDto]
    ) -> Tuple[List[Track], List[User], List[ListenHistory], List[Genre]]:
        """Transform all DTOs using pandas operations"""
        
        # First create genres map
        self.create_genres_list(tracks_dto, users_dto)
        
        # Transform all data using pandas
        tracks = self.transform_tracks(tracks_dto)
        users = self.transform_users(users_dto)
        listen_history = self.transform_listen_history(listen_history_dto)
        
        return tracks, users, listen_history, self.genres