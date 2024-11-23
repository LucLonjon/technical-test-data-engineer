from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from typing import Generator
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig:
    """Database configuration and connection management class"""
    
    def __init__(
        self,
        user: str = os.getenv("DB_USER", "root"),
        password: str = os.getenv("DB_PASSWORD", "root"),
        host: str = os.getenv("DB_HOST", "localhost"),
        port: str = os.getenv("DB_PORT", "3305"),
        database: str = os.getenv("DB_NAME", "moovitamix"),
        echo: bool = os.getenv("DB_ECHO", "False").lower() == "true"
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.echo = echo
        
        # Initialize core SQLAlchemy components
        self._engine = None
        self._session_factory = None
        self.Base = declarative_base()
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
    @property
    def database_url(self) -> str:
        """Construct database URL from components"""
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def engine(self):
        """Lazy initialization of the database engine"""
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                echo=self.echo,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                # Add connect_args for additional MySQL configuration if needed
                connect_args={
                    "charset": "utf8mb4",
                    "connect_timeout": 60,
                }
            )
        return self._engine
    
    @property
    def session_factory(self):
        """Lazy initialization of the session factory"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
        return self._session_factory
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        
        Usage:
            with db_config.get_session() as session:
                session.query(User).all()
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()
    
    def init_database(self) -> bool:
        """
        Initialize the database by creating all tables.
        Returns True if successful, False otherwise.
        """
        try:
            self.Base.metadata.create_all(bind=self.engine)
            self.logger.info("Database tables created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating database tables: {str(e)}")
            return False
    
    def drop_database(self) -> bool:
        """
        Drop all tables in the database.
        Returns True if successful, False otherwise.
        """
        try:
            self.Base.metadata.drop_all(bind=self.engine)
            self.logger.info("Database tables dropped successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error dropping database tables: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        Returns True if successful, False otherwise.
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            self.logger.info("Successfully connected to the database")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def dispose_engine(self):
        """Dispose of the current engine and all its database connections"""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self.logger.info("Database engine disposed")

# Create a default instance
default_db_config = DatabaseConfig()

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create database configuration using environment variables
    db_config = DatabaseConfig()
    
    # Test connection and initialize database
    if db_config.test_connection():
        db_config.init_database()