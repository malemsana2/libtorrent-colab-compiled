import sqlite3
import threading
from pathlib import Path
from typing import List, Optional

class MetadataManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Use WAL mode for better concurrency performance
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS segments (
                        start_ms INTEGER PRIMARY KEY,
                        end_ms INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        sources TEXT NOT NULL,
                        storage_id INTEGER
                    )
                ''')
                conn.commit()

    def is_segment_processed(self, start_ms: int) -> bool:
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM segments WHERE start_ms = ? AND status = ?', (start_ms, 'COMPLETED'))
                return cursor.fetchone() is not None

    def mark_segment_completed(self, start_ms: int, end_ms: int, sources: str, storage_id: int):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO segments (start_ms, end_ms, status, sources, storage_id)
                    VALUES (?, ?, 'COMPLETED', ?, ?)
                ''', (start_ms, end_ms, sources, storage_id))
                conn.commit()

    def get_all_completed_segments(self) -> List[dict]:
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT start_ms, end_ms, sources, storage_id FROM segments WHERE status = "COMPLETED" ORDER BY start_ms ASC')
                rows = cursor.fetchall()
                
                return [{
                    "start_ms": row['start_ms'],
                    "end_ms": row['end_ms'],
                    "sources": __import__('json').loads(row['sources']),
                    "storage_id": row['storage_id']
                } for row in rows]

    def reset(self):
        with self.lock:
            if self.db_path.exists():
                self.db_path.unlink()
            self._init_db()
