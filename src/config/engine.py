import os

from sqlalchemy import create_engine, event

class InternalDatabaseConfiguration:
    DB_URL = os.environ.get('HEROKU_POSTGRESQL_BLUE_URL', 'postgresql://test:FeedMer_Database_Test_For_Analytics@91.146.46.102:8628/FeedMerML')
    BATCH_SIZE = 1000
    
    engine = None

    @classmethod
    def getEngine(cls):
        if (cls.engine is None):
            cls.engine = create_engine(
                cls.DB_URL, 
                echo=False, 
                executemany_mode='values', 
                executemany_values_page_size=cls.BATCH_SIZE
            )
        return cls.engine

class ExternalDatabaseConfiguration:
    DB_URL = os.environ.get('EXTERNAL_DB_URL', 'postgresql://test:FeedMer_Database_Test_For_Analytics@91.146.46.102:8628/postgres')
    BATCH_SIZE = 1000
    
    engine = None

    @classmethod
    def getEngine(cls):
        if (cls.engine is None):
            cls.engine = create_engine(
                cls.DB_URL, 
                echo=False, 
                executemany_mode='values', 
                executemany_values_page_size=cls.BATCH_SIZE
            )
        return cls.engine