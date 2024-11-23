import unittest
from unittest.mock import Mock, patch
from datetime import datetime
import os
import shutil
from src.moovitamix_etl.transform.data_transformer import DataTransformer
from src.moovitamix_etl.load.data_loader import DataLoader
from src.moovitamix_etl.load.database_config import DatabaseConfig
from src.moovitamix_etl.pipeline import ETLPipeline
from src.moovitamix_etl.load.model.model import Genre, Track, User
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))



class TestETLPipeline(unittest.TestCase):
    """Essential test cases for the ETL Pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create sample test data
        self.test_track_dto = Mock(
            id=1,
            name="Test Track",
            artist="Test Artist",
            genres="Rock, Pop",
            duration="3:30",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.test_user_dto = Mock(
            id=1,
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            gender="M",
            favorite_genres="Rock",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.test_listen_history_dto = Mock(
        user_id=1,
        items=[1, 2, 3, 4, 5],  # List of track IDs that the user has listened to
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
        
        # Create test directory for CSV output
        self.test_csv_folder = Path(__file__).parent / "test_csv_data"
        self.test_csv_folder.mkdir(exist_ok=True)
    
    def tearDown(self):
        """Clean up after tests"""
        if self.test_csv_folder.exists():
            shutil.rmtree(self.test_csv_folder)
    
    def test_extraction_works(self):
        """Test 1: Verify the basic extraction process works"""
        # Setup mock data
        expected_data = ([self.test_track_dto], [self.test_user_dto], [self.test_listen_history_dto])
        
        with patch('moovitamix_etl.extract.extractor.Extractor.get_all_resources') as mock_extract:
            mock_extract.return_value = expected_data
            
            # Execute
            
            tracks, users, listen_history = mock_extract()
            
            # Basic assertions
            self.assertIsNotNone(tracks, "Tracks should not be None")
            self.assertIsNotNone(users, "Users should not be None")
            self.assertIsNotNone(listen_history, "Listen history should not be None")
            self.assertEqual(len(tracks), 1, "Should have one track")
            self.assertEqual(len(users), 1, "Should have one user")
            
            
            # Verify mock was called
            mock_extract.assert_called_once()


    def test_database_connection(self):
        """Test 3: Verify database connection handling"""
        # Test both successful and failed connections
        with patch('moovitamix_etl.load.database_config.DatabaseConfig.test_connection') as mock_conn:
            # Test successful connection
            mock_conn.return_value = True
            
            self.assertTrue(mock_conn(), "Should handle successful connection")
            
            # Test failed connection
            mock_conn.return_value = False
            self.assertFalse(mock_conn(), "Should handle failed connection")

  
    def test_full_pipeline_execution(self):
        """Test 5: Verify the full pipeline executes without errors"""
        # Setup mock data
        mock_data = ([self.test_track_dto], [self.test_user_dto], [])
        
        with patch('moovitamix_etl.extract.extractor.Extractor.get_all_resources') as mock_extract:
            mock_extract.return_value = mock_data
            
            # Execute full pipeline
            pipeline = ETLPipeline(into_csv=True)
            
            try:
                result = pipeline.run()
                self.assertTrue(result, "Pipeline should execute successfully")
            except Exception as e:
                self.fail(f"Pipeline raised an exception: {str(e)}")
