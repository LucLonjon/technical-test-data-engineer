from typing import List, Dict
import os
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from src.moovitamix_etl.load.database_config import DatabaseConfig
from src.moovitamix_etl.load.model.model import Genre, Track, User, ListenHistory


import logging

class DataLoader:
    """Class to handle loading transformed data into either database or CSV files"""
    
    def __init__(self, db_config: DatabaseConfig = None, into_csv: bool = False, csv_folder: str = "csv_data"):
        self.db_config = db_config or DatabaseConfig()
        self.logger = logging.getLogger(__name__)
        self.into_csv = into_csv
        self.csv_folder = csv_folder
        # Initialize ID mappings
        self.track_id_map = {}
        self.user_id_map = {}
        self.genre_id_map = {}
        
        if self.into_csv:
            os.makedirs(self.csv_folder, exist_ok=True)
            self.logger.info(f"CSV data will be stored in: {self.csv_folder}")
    
    def _save_to_csv(self, data: List, filename: str) -> None:
        """Save data to CSV file"""
        filepath = os.path.join(self.csv_folder, filename)
        
        if data:
            records = []
            for item in data:
                record = item.__dict__.copy()
                record.pop('_sa_instance_state', None)
                
                # Handle relationships
                if hasattr(item, 'genres'):
                    record['genres'] = ','.join([g.name for g in item.genres]) if item.genres else ''
                if hasattr(item, 'favorite_genres'):
                    record['favorite_genres'] = ','.join([g.name for g in item.favorite_genres]) if item.favorite_genres else ''
                
                records.append(record)
            
            df = pd.DataFrame(records)
            df.to_csv(filepath, index=False)
            self.logger.info(f"Saved {len(records)} records to {filepath}")
        else:
            pd.DataFrame().to_csv(filepath, index=False)
            self.logger.info(f"Created empty CSV file: {filepath}")
    
    def _load_genres(self, session, genres: List[Genre], update_existing: bool) -> Dict[int, int]:
        """Load genres and return id mapping"""
        for genre in genres:
            existing = session.query(Genre).filter(Genre.name == genre.name).first()
            if existing:
                self.genre_id_map[genre.id] = existing.id
                if update_existing:
                    existing.name = genre.name
            else:
                session.add(genre)
                session.flush()
                self.genre_id_map[genre.id] = genre.id
        return self.genre_id_map
    
    def _load_tracks(self, session, tracks: List[Track], update_existing: bool) -> Dict[int, int]:
        """Load tracks and return id mapping"""
        for track in tracks:
            existing = session.query(Track).filter(
                Track.name == track.name,
                Track.artist == track.artist
            ).first()
            
            if existing:
                self.track_id_map[track.id] = existing.id
                if update_existing:
                    existing.songwriters = track.songwriters
                    existing.duration = track.duration
                    existing.album = track.album
                    existing.genres = [
                        session.get(Genre, self.genre_id_map[g.id])
                        for g in track.genres
                        if g.id in self.genre_id_map
                    ]
            else:
                track.genres = [
                    session.get(Genre, self.genre_id_map[g.id])
                    for g in track.genres
                    if g.id in self.genre_id_map
                ]
                session.add(track)
                session.flush()
                self.track_id_map[track.id] = track.id
        return self.track_id_map
    
    def _load_users(self, session, users: List[User], update_existing: bool) -> Dict[int, int]:
        """Load users and return id mapping"""
        for user in users:
            existing = session.query(User).filter(User.email == user.email).first()
            if existing:
                self.user_id_map[user.id] = existing.id
                if update_existing:
                    existing.first_name = user.first_name
                    existing.last_name = user.last_name
                    existing.gender = user.gender
                    existing.favorite_genres = [
                        session.get(Genre, self.genre_id_map[g.id])
                        for g in user.favorite_genres
                        if g.id in self.genre_id_map
                    ]
            else:
                user.favorite_genres = [
                    session.get(Genre, self.genre_id_map[g.id])
                    for g in user.favorite_genres
                    if g.id in self.genre_id_map
                ]
                session.add(user)
                session.flush()
                self.user_id_map[user.id] = user.id
        return self.user_id_map
    
    def _load_listen_history(self, session, listen_history: List[ListenHistory]) -> None:
        """Load listen history using the id mappings"""
        new_records = []
        for history in listen_history:
            if history.track_id in self.track_id_map and history.user_id in self.user_id_map:
                new_history = ListenHistory(
                    user_id=self.user_id_map[history.user_id],
                    track_id=self.track_id_map[history.track_id],
                    listened_at=history.listened_at
                )
                new_records.append(new_history)
            else:
                self.logger.warning(
                    f"Skipping listen history record: user_id={history.user_id}, "
                    f"track_id={history.track_id} - Missing reference"
                )
        
        if new_records:
            session.bulk_save_objects(new_records)
            session.flush()
    
    def load_all(
        self,
        tracks: List[Track],
        users: List[User],
        listen_history: List[ListenHistory],
        genres: List[Genre],
        update_existing: bool = True
    ) -> bool:
        """Load all data either to database or CSV files"""
        try:
            if self.into_csv:
                return self._save_all_to_csv(tracks, users, listen_history, genres)
            else:
                return self._save_all_to_db(tracks, users, listen_history, genres, update_existing)
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _save_all_to_csv(
        self,
        tracks: List[Track],
        users: List[User],
        listen_history: List[ListenHistory],
        genres: List[Genre]
    ) -> bool:
        """Save all data to CSV files"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            self._save_to_csv(genres, f"genres_{timestamp}.csv")
            self._save_to_csv(tracks, f"tracks_{timestamp}.csv")
            self._save_to_csv(users, f"users_{timestamp}.csv")
            self._save_to_csv(listen_history, f"listen_history_{timestamp}.csv")
            
            self._log_csv_files()
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")
            raise
    
    def _save_all_to_db(
        self,
        tracks: List[Track],
        users: List[User],
        listen_history: List[ListenHistory],
        genres: List[Genre],
        update_existing: bool
    ) -> bool:
        """Save all data to database"""
        try:
            with self.db_config.get_session() as session:
                # Step 1: Genres
                self.logger.info("Loading genres to database...")
                self._load_genres(session, genres, update_existing)
                session.flush()
                
                # Step 2: Tracks
                self.logger.info("Loading tracks to database...")
                self._load_tracks(session, tracks, update_existing)
                session.flush()
                
                # Step 3: Users
                self.logger.info("Loading users to database...")
                self._load_users(session, users, update_existing)
                session.flush()
                
                # Step 4: Listen History
                self.logger.info("Loading listen history to database...")
                self._load_listen_history(session, listen_history)
                session.flush()
                
                self._log_counts(session)
                return True
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error: {str(e)}")
            raise
    
    def _log_counts(self, session) -> None:
        """Log database record counts"""
        counts = {
            'genres': session.query(Genre).count(),
            'tracks': session.query(Track).count(),
            'users': session.query(User).count(),
            'listen_history': session.query(ListenHistory).count()
        }
        
        self.logger.info("=== Database Record Counts ===")
        for table, count in counts.items():
            self.logger.info(f"{table}: {count}")
    
    def _log_csv_files(self) -> None:
        """Log CSV file information"""
        self.logger.info("=== CSV Files Created ===")
        for filename in os.listdir(self.csv_folder):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.csv_folder, filename)
                size = os.path.getsize(filepath)
                df = pd.read_csv(filepath)
                self.logger.info(f"{filename}: {len(df)} records ({size/1024:.2f} KB)")
   