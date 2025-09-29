# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System
Database connection management module
"""
import psycopg2
from psycopg2 import pool
import threading
from typing import Optional
import logging

class DatabaseManager:
    """
    Singleton database connection manager with connection pooling
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, host: str, user: str, password: str, port: int, database_name: str):
        # Prevent re-initialization
        if hasattr(self, 'pool'):
            return
            
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                1, 20,  # min and max connections
                host=host,
                user=user,
                password=password,
                port=port,
                database=database_name
            )
            self.logger = logging.getLogger(self.__class__.__name__)
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool
        """
        return self.pool.getconn()
    
    def return_connection(self, conn):
        """
        Return a connection to the pool
        """
        self.pool.putconn(conn)
    
    def close_all_connections(self):
        """
        Close all connections in the pool
        """
        if self.pool:
            self.pool.closeall()

class BrokerTokenManager:
    """
    Manager for broker token operations
    """
    def __init__(self, db_manager: DatabaseManager, token_database_name: str):
        self.db_manager = db_manager
        self.token_database_name = token_database_name
    
    def store_access_token(self, token_url: str, request_token: str, access_token: str):
        """
        Store access token in database
        """
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cursor:
                # Ensure schema exists
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.token_database_name}")
                
                # Create table if not exists
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.token_database_name}.broker_tokens 
                    (timestamp TIMESTAMP UNIQUE, request_url varchar(255), 
                     request_token varchar(255), access_token varchar(255))
                """)
                
                # Insert token
                short_sql = f"INSERT INTO {self.token_database_name}.broker_tokens VALUES (%s, %s, %s, %s)"
                cursor.execute(short_sql, [self.get_current_timestamp(), token_url, request_token, access_token])
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.db_manager.return_connection(conn)
    
    def get_latest_access_token(self) -> Optional[str]:
        """
        Get the latest access token from database
        """
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f'''
                    SELECT access_token 
                    FROM {self.token_database_name}.broker_tokens 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''')
                result = cursor.fetchone()
                return result[0] if result else None
        finally:
            self.db_manager.return_connection(conn)
    
    def is_latest_token_fresh(self, after_hour: int = 8) -> dict:
        """
        Check if the latest token is from today after specified hour
        """
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f'''
                    SELECT timestamp 
                    FROM {self.token_database_name}.broker_tokens 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''')
                result = cursor.fetchone()
                
                if not result:
                    return {'status': False, 'timestamp': None}
                
                from datetime import datetime
                access_token_timestamp = result[0]
                
                # Parse the timestamp to a datetime object
                if isinstance(access_token_timestamp, str):
                    access_token_timestamp = datetime.fromisoformat(access_token_timestamp.replace('Z', '+00:00'))
                
                # Create a datetime object for today at the specified hour
                today_at_hour = datetime.now().replace(hour=after_hour, minute=0, second=0, microsecond=0)
                
                # Check if access token was generated after the specified hour today
                is_fresh = access_token_timestamp > today_at_hour
                return {'status': is_fresh, 'timestamp': access_token_timestamp}
        finally:
            self.db_manager.return_connection(conn)
    
    def get_current_timestamp(self):
        """
        Get current timestamp
        """
        from datetime import datetime
        return datetime.now()

# Global database manager instance creator
def create_db_manager(host: str, user: str, password: str, port: int, database_name: str) -> DatabaseManager:
    """
    Create a database manager instance
    """
    return DatabaseManager(host, user, password, port, database_name)