"""
db_utils.py - Thread-safe SQLite database utilities

Provides safe concurrent access to SQLite databases across multiple processes.
Uses connection-per-operation pattern with retry logic for locked databases.
"""

import sqlite3
import threading
import time
import logging
import json
from contextlib import contextmanager
from datetime import datetime

# Global locks for write operations (one per database file)
_db_locks = {}
_locks_lock = threading.Lock()

def get_db_lock(db_path):
    """Get or create a lock for a specific database file"""
    with _locks_lock:
        if db_path not in _db_locks:
            _db_locks[db_path] = threading.Lock()
        return _db_locks[db_path]


@contextmanager
def get_db_connection(db_path, row_factory=True):
    """
    Context manager for safe database connections.
    Creates a new connection per operation and ensures proper cleanup.

    Args:
        db_path: Path to SQLite database file
        row_factory: If True, use sqlite3.Row for dict-like access

    Yields:
        sqlite3.Connection object

    Example:
        with get_db_connection('trading.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM signals")
            results = cursor.fetchall()
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        if row_factory:
            conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        yield conn
    finally:
        if conn:
            conn.close()


def execute_with_retry(db_path, query, params=None, max_retries=5, retry_delay=0.5):
    """
    Execute a write query with retry logic for locked databases.

    Args:
        db_path: Path to SQLite database file
        query: SQL query to execute
        params: Query parameters (tuple or dict)
        max_retries: Maximum retry attempts
        retry_delay: Initial delay between retries (doubles each attempt)

    Returns:
        lastrowid for INSERT, rowcount for UPDATE/DELETE

    Raises:
        sqlite3.Error: If all retries fail
    """
    lock = get_db_lock(db_path)
    delay = retry_delay
    last_error = None

    for attempt in range(max_retries):
        try:
            with lock:
                with get_db_connection(db_path) as conn:
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    conn.commit()
                    return cursor.lastrowid if cursor.lastrowid else cursor.rowcount

        except sqlite3.OperationalError as e:
            last_error = e
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                if attempt < max_retries - 1:
                    logging.warning(f"[DB RETRY {attempt+1}/{max_retries}] Database locked, waiting {delay:.1f}s...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                continue
            raise
        except sqlite3.IntegrityError:
            # Duplicate entry - not an error, just return None
            return None

    logging.error(f"[DB ERROR] Failed after {max_retries} attempts: {last_error}")
    raise last_error


def fetch_all(db_path, query, params=None):
    """
    Fetch all results from a SELECT query.

    Args:
        db_path: Path to SQLite database file
        query: SQL SELECT query
        params: Query parameters (tuple or dict)

    Returns:
        List of sqlite3.Row objects
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchall()


def fetch_one(db_path, query, params=None):
    """
    Fetch a single result from a SELECT query.

    Args:
        db_path: Path to SQLite database file
        query: SQL SELECT query
        params: Query parameters (tuple or dict)

    Returns:
        sqlite3.Row object or None
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()


class ThreadSafeDB:
    """
    Thread-safe database wrapper class.

    Provides a cleaner interface for database operations with built-in
    thread safety and retry logic.

    Example:
        db = ThreadSafeDB('trading.db')
        db.init_signals_table()
        signal_id = db.insert_signal(channel_id, channel_name, ...)
        pending = db.get_pending_signals()
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = get_db_lock(db_path)
        self._init_db()

    def _init_db(self):
        """Initialize database with WAL mode"""
        with get_db_connection(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()

    def init_signals_table(self, include_instrument_type=True):
        """Create signals table if not exists"""
        if include_instrument_type:
            query = '''
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT,
                    channel_name TEXT,
                    message_id INTEGER,
                    raw_text TEXT,
                    parsed_data TEXT,
                    timestamp TEXT,
                    processed INTEGER DEFAULT 0,
                    instrument_type TEXT DEFAULT 'OPTIONS',
                    UNIQUE(channel_id, message_id)
                )
            '''
        else:
            query = '''
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT,
                    channel_name TEXT,
                    message_id INTEGER,
                    raw_text TEXT,
                    parsed_data TEXT,
                    timestamp TEXT,
                    processed INTEGER DEFAULT 0,
                    parser_type TEXT,
                    signal_type TEXT,
                    UNIQUE(channel_id, message_id)
                )
            '''
        execute_with_retry(self.db_path, query)
        logging.info(f"[DB] Signals table ready in {self.db_path}")

    def insert_signal(self, channel_id, channel_name, message_id, raw_text,
                      parsed_data, timestamp=None, instrument_type='OPTIONS',
                      parser_type=None, signal_type=None):
        """
        Insert a signal with thread-safe retry logic.

        Returns:
            signal_id on success, None on duplicate
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()

        # Determine which columns to use based on what's provided
        if instrument_type and not parser_type:
            query = '''
                INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed, instrument_type)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            '''
            params = (channel_id, channel_name, message_id, raw_text,
                     json.dumps(parsed_data) if isinstance(parsed_data, dict) else parsed_data,
                     timestamp, instrument_type)
        else:
            query = '''
                INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed, parser_type, signal_type)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
            '''
            params = (channel_id, channel_name, message_id, raw_text,
                     json.dumps(parsed_data) if isinstance(parsed_data, dict) else parsed_data,
                     timestamp, parser_type, signal_type)

        try:
            result = execute_with_retry(self.db_path, query, params)
            return result if result and result > 0 else None
        except sqlite3.IntegrityError:
            return None

    def get_pending_signals(self):
        """Get all unprocessed signals"""
        return fetch_all(self.db_path,
                        "SELECT * FROM signals WHERE processed = 0 ORDER BY id ASC")

    def mark_signal_processed(self, signal_id, status=1):
        """Mark signal as processed (1=success, -1=failed)"""
        execute_with_retry(self.db_path,
                          "UPDATE signals SET processed = ? WHERE id = ?",
                          (status, signal_id))

    def get_signal_by_tradingsymbol(self, tradingsymbol):
        """Find signal containing tradingsymbol in parsed_data"""
        return fetch_one(self.db_path,
                        """SELECT * FROM signals
                           WHERE parsed_data LIKE ? AND processed = 1
                           ORDER BY timestamp DESC LIMIT 1""",
                        (f'%{tradingsymbol}%',))


# Convenience functions for common databases
def get_trading_db():
    """Get thread-safe wrapper for main trading database"""
    return ThreadSafeDB('trading.db')

def get_jp_signals_db():
    """Get thread-safe wrapper for JP signals database"""
    return ThreadSafeDB('jp_signals_trained.db')
