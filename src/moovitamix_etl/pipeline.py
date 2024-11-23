import argparse
import logging
from src.moovitamix_etl.extract.extractor import Extractor
from src.moovitamix_etl.transform.data_transformer import DataTransformer
from src.moovitamix_etl.load.database_config import DatabaseConfig
from src.moovitamix_etl.load.data_loader import DataLoader

class ETLPipeline:
    """ETL Pipeline to process music data"""
    
    def __init__(self, into_csv: bool = False, csv_folder: str = "csv_data"):
        self.into_csv = into_csv
        self.csv_folder = csv_folder
        self.logger = logging.getLogger(__name__)
        
    def run(self):
        """Execute the ETL pipeline"""
        try:
            # Extract
            self.logger.info("Starting extraction phase...")
            tracks_dtos, users_dtos, listen_histories_dtos = self._extract()
            self.logger.info("Extraction completed successfully")
            
            # Transform
            self.logger.info("Starting transformation phase...")
            tracks, users, listen_history, genres = self._transform(
                tracks_dtos, 
                users_dtos, 
                listen_histories_dtos
            )
            self.logger.info("Transformation completed successfully")
            
            # Load
            self.logger.info("Starting loading phase...")
            success = self._load(tracks, users, listen_history, genres)
            
            if success:
                self.logger.info("Pipeline completed successfully!")
                return True
            else:
                self.logger.error("Pipeline failed during loading phase")
                return False
                
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
    
    def _extract(self):
        """Extract data from sources"""
        extractor = Extractor()
        return extractor.get_all_resources()
    
    def _transform(self, tracks_dtos, users_dtos, listen_histories_dtos):
        """Transform extracted data"""
        transformer = DataTransformer()
        return transformer.transform_all(
            tracks_dtos,
            users_dtos,
            listen_histories_dtos
        )
    
    def _load(self, tracks, users, listen_history, genres):
        """Load transformed data"""
        if not self.into_csv:
            # Verify database connection before loading
            db_config = DatabaseConfig()
            if not db_config.test_connection():
                self.logger.error("Failed to connect to database")
                return False
        
        # Initialize loader with specified destination
        loader = DataLoader(
            into_csv=self.into_csv,
            csv_folder=self.csv_folder
        )
        
        # Load the data
        return loader.load_all(tracks, users, listen_history, genres)

def main():
    """Main entry point with argument parsing"""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='MoovitaMix ETL Pipeline')
    
    parser.add_argument(
        '--into-csv',
        action='store_true',
        help='Store data in CSV files instead of database'
    )
    
    parser.add_argument(
        '--csv-folder',
        type=str,
        default='csv_data',
        help='Folder to store CSV files (default: csv_data)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        into_csv=args.into_csv,
        csv_folder=args.csv_folder
    )
    
    try:
        success = pipeline.run()
        exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Pipeline failed with error: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main()