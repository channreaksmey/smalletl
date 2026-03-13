"""
Database connection and operations handler
"""

from sqlalchemy import create_engine, text
from .config import DB_CONFIG
from .utils.logger import setup_logger

logger = setup_logger(__name__)


class DatabaseHandler:
    """
    Handles all database connections and operations
    """
    
    def __init__(self):
        self.config = DB_CONFIG
        self.engine = self._create_engine()
        
    def _create_engine(self):
        """Create SQLAlchemy engine"""
        connection_string = (
            f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
        return create_engine(connection_string)
    
    def test_connection(self, tracker=None):
        """
        Test database connection and verify table exists
        Returns: bool - True if successful
        """
        if tracker:
            tracker.log_step("0", "Testing Database Connection")
        else:
            logger.info("Testing database connection...")
            
        try:
            with self.engine.connect() as conn:
                # Test connection
                result = conn.execute(text("SELECT VERSION()"))
                version = result.fetchone()[0]
                
                if tracker:
                    tracker.log_progress(
                        f"Connected to MySQL version: {version}", 
                        'success'
                    )
                else:
                    logger.info(f"Connected to MySQL {version}")
                
                # Check table exists
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = :db AND table_name = 'daily_sales'
                """), {"db": self.config['database']})
                
                exists = result.fetchone()[0]
                if exists:
                    if tracker:
                        tracker.log_progress(
                            "Target table 'daily_sales' exists", 
                            'success'
                        )
                else:
                    if tracker:
                        tracker.log_progress(
                            "Table 'daily_sales' not found - will be created", 
                            'warning'
                        )
                
                return True
                
        except Exception as e:
            if tracker:
                tracker.log_progress(f"Connection failed: {e}", 'error')
            else:
                logger.error(f"Database connection failed: {e}")
            return False
    
    def load_data(self, df, table_name, tracker=None):
        """
        Load DataFrame into database table
        Args:
            df: DataFrame to load
            table_name: Target table name
            tracker: ProgressTracker instance
        Returns: int - number of rows loaded
        """
        if tracker:
            tracker.log_step("3", "LOADING")
            tracker.log_progress(f"Inserting {len(df)} rows into '{table_name}'")
        
        try:
            rows = len(df)
            
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi'
            )
            
            if tracker:
                tracker.log_progress(
                    f"Successfully loaded {rows} rows", 
                    'success'
                )
            
            return rows
            
        except Exception as e:
            if tracker:
                tracker.log_progress(f"Loading failed: {e}", 'error')
            raise