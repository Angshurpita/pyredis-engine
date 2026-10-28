"""
Snapshotting (RDB-style) persistence manager.
Serializes database state to a compact binary file format.
"""
import os
import pickle
import time
from typing import Any, List

MAGIC_HEADER = b"PYREDIS_RDB_V1\n"


class SnapshotManager:
    """Manages saving and restoring point-in-time database snapshots."""

    def __init__(self, filepath: str = "dump.rdb"):
        self.filepath = filepath

    def save(self, databases: List[Any]) -> None:
        """Serialize current state to disk atomically."""
        temp_path = f"{self.filepath}.temp.{os.getpid()}"
        now = time.time()

        # Extract serializable state
        dump_data = []
        for db in databases:
            db_state = {
                "id": db.db_id,
                "dict": {},
                "expires": {},
            }
            for k, obj in db._dict.items():
                if not db._is_expired(k, now):
                    db_state["dict"][k] = obj
                    if k in db._expires:
                        db_state["expires"][k] = db._expires[k]
            dump_data.append(db_state)

        with open(temp_path, "wb") as f:
            f.write(MAGIC_HEADER)
            pickle.dump(dump_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, self.filepath)

    def load(self, databases: List[Any]) -> bool:
        """Load state from dump.rdb if present. Returns True if restored."""
        if not os.path.exists(self.filepath):
            return False

        with open(self.filepath, "rb") as f:
            header = f.readline()
            if header != MAGIC_HEADER:
                return False
            dump_data = pickle.load(f)

        for db_state in dump_data:
            db_id = db_state["id"]
            if db_id < len(databases):
                target_db = databases[db_id]
                target_db._dict.clear()
                target_db._expires.clear()
                target_db._dict.update(db_state["dict"])
                target_db._expires.update(db_state["expires"])

        return True
